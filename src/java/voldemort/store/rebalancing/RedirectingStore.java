/*
 * Copyright 2008-2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.rebalancing;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxSetter;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * The RedirectingStore extends {@link DelegatingStore}
 * <p>
 * If current server_state is {@link VoldemortState#REBALANCING_MASTER_SERVER} <br>
 * then before serving any client request do a remote get() call, put it locally
 * ignoring any {@link ObsoleteVersionException} and then serve the client
 * requests. This piece of code is run on the stealer nodes.
 */
public class RedirectingStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private final static Logger logger = Logger.getLogger(RedirectingStore.class);
    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private final SocketStoreFactory storeFactory;
    private FailureDetector failureDetector;
    private AtomicBoolean isRedirectingStoreEnabled;

    public RedirectingStore(Store<ByteArray, byte[], byte[]> innerStore,
                            MetadataStore metadata,
                            StoreRepository storeRepository,
                            FailureDetector detector,
                            SocketStoreFactory storeFactory) {
        super(innerStore);
        this.metadata = metadata;
        this.storeRepository = storeRepository;
        this.storeFactory = storeFactory;
        this.failureDetector = detector;
        this.isRedirectingStoreEnabled = new AtomicBoolean(true);
    }

    @JmxSetter(name = "setRedirectingStoreEnabled", description = "Enable the redirecting store for this store")
    public void setIsRedirectingStoreEnabled(boolean isRedirectingStoreEnabled) {
        logger.info("Setting redirecting store flag for " + getName() + " to "
                    + isRedirectingStoreEnabled);
        this.isRedirectingStoreEnabled.set(isRedirectingStoreEnabled);
    }

    @JmxGetter(name = "isRedirectingStoreEnabled", description = "Get the redirecting store state for this store")
    public boolean getIsRedirectingStoreEnabled() {
        return this.isRedirectingStoreEnabled.get();
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        RebalancePartitionsInfo stealInfo = redirectingKey(key);

        /**
         * If I am rebalancing for this key, try to do remote get() , put it
         * locally first to get the correct version ignoring any
         * {@link ObsoleteVersionException}
         */
        if(stealInfo != null)
            proxyGetAndLocalPut(key, stealInfo.getDonorId(), transforms);

        getInnerStore().put(key, value, transforms);
    }

    private RebalancePartitionsInfo redirectingKey(ByteArray key) {
        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(metadata.getServerState())
           && isRedirectingStoreEnabled.get()) {
            return metadata.getRebalancerState().find(getName(),
                                                      metadata.getRoutingStrategy(getName())
                                                              .getPartitionList(key.get()),
                                                      metadata.getCluster()
                                                              .getNodeById(metadata.getNodeId())
                                                              .getPartitionIds());
        }
        return null;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        RebalancePartitionsInfo stealInfo = redirectingKey(key);

        /**
         * If I am rebalancing for this key, try to do remote get(), put it
         * locally first to get the correct version ignoring any
         * {@link ObsoleteVersionException}
         */
        if(stealInfo != null) {
            proxyGetAndLocalPut(key, stealInfo.getDonorId(), transforms);
        }

        return getInnerStore().get(key, transforms);
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        RebalancePartitionsInfo stealInfo = redirectingKey(key);

        /**
         * If I am rebalancing for this key, try to do remote get(), put it
         * locally first to get the correct version ignoring any
         * {@link ObsoleteVersionException}.
         */
        if(stealInfo != null) {
            proxyGetAndLocalPut(key, stealInfo.getDonorId(), null);
        }

        return getInnerStore().getVersions(key);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        Map<ByteArray, RebalancePartitionsInfo> rebalancePartitionsInfoPerKey = Maps.newHashMapWithExpectedSize(Iterables.size(keys));
        for(ByteArray key: keys) {
            RebalancePartitionsInfo info = redirectingKey(key);
            if(info != null) {
                rebalancePartitionsInfoPerKey.put(key, info);
            }
        }

        if(!rebalancePartitionsInfoPerKey.isEmpty()) {
            proxyGetAllAndLocalPut(rebalancePartitionsInfoPerKey, transforms);
        }

        return getInnerStore().getAll(keys, transforms);
    }

    /**
     * TODO : Handle delete correctly.
     * <p>
     * The options are:
     * <ol>
     * <li>
     * Delete locally and on remote node as well. The issue is cursor is open in
     * READ_UNCOMMITED mode while rebalancing and can push the value back.</li>
     * <li>
     * Keep the operation in separate slop store and apply all deletes after
     * rebalancing.</li>
     * <li>
     * Do not worry about deletes for now, Voldemort in general has an issue
     * that if node goes down during a delete, we will still keep the old
     * version.</li>
     * </ol>
     */
    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return getInnerStore().delete(key, version);
    }

    /**
     * Performs a back-door proxy get to
     * {@link voldemort.client.rebalance.RebalancePartitionsInfo#getDonorId()
     * getDonorId}
     * 
     * @param key Key
     * @param donorNodeId donor node id
     * @throws ProxyUnreachableException if donor node can't be reached
     */
    private List<Versioned<byte[]>> proxyGet(ByteArray key, int donorNodeId, byte[] transform) {
        Node donorNode = metadata.getCluster().getNodeById(donorNodeId);
        checkNodeAvailable(donorNode);
        long startNs = System.nanoTime();
        try {
            Store<ByteArray, byte[], byte[]> redirectingStore = getRedirectingSocketStore(getName(),
                                                                                          donorNodeId);
            List<Versioned<byte[]>> values = redirectingStore.get(key, transform);
            recordSuccess(donorNode, startNs);
            return values;
        } catch(UnreachableStoreException e) {
            recordException(donorNode, startNs, e);
            throw new ProxyUnreachableException("Failed to reach proxy node " + donorNode, e);
        }
    }

    private void checkNodeAvailable(Node donorNode) {
        if(!failureDetector.isAvailable(donorNode))
            throw new ProxyUnreachableException("Failed to reach proxy node " + donorNode
                                                + " is marked down by failure detector.");
    }

    /**
     * Performs a back-door proxy get to
     * {@link voldemort.client.rebalance.RebalancePartitionsInfo#getDonorId()
     * getDonorId}
     * 
     * @param rebalancePartitionsInfoPerKey Map of keys to corresponding
     *        partition info
     * @param transforms Map of keys to their corresponding transforms
     * @throws ProxyUnreachableException if donor node can't be reached
     */
    private Map<ByteArray, List<Versioned<byte[]>>> proxyGetAll(Map<ByteArray, RebalancePartitionsInfo> rebalancePartitionsInfoPerKey,
                                                                Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        Multimap<Integer, ByteArray> donorNodeToKeys = HashMultimap.create();
        int numKeys = 0;

        // Transform the map of key to plan to a map of donor node id to keys
        for(Map.Entry<ByteArray, RebalancePartitionsInfo> entry: rebalancePartitionsInfoPerKey.entrySet()) {
            numKeys++;
            donorNodeToKeys.put(entry.getValue().getDonorId(), entry.getKey());
        }

        Map<ByteArray, List<Versioned<byte[]>>> gatherMap = Maps.newHashMapWithExpectedSize(numKeys);

        for(int donorNodeId: donorNodeToKeys.keySet()) {
            Node donorNode = metadata.getCluster().getNodeById(donorNodeId);
            checkNodeAvailable(donorNode);
            long startNs = System.nanoTime();

            try {
                Map<ByteArray, List<Versioned<byte[]>>> resultsForNode = getRedirectingSocketStore(getName(),
                                                                                                   donorNodeId).getAll(donorNodeToKeys.get(donorNodeId),
                                                                                                                       transforms);
                recordSuccess(donorNode, startNs);

                for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: resultsForNode.entrySet()) {
                    gatherMap.put(entry.getKey(), entry.getValue());
                }
            } catch(UnreachableStoreException e) {
                recordException(donorNode, startNs, e);
                throw new ProxyUnreachableException("Failed to reach proxy node " + donorNode, e);
            }
        }

        return gatherMap;
    }

    /**
     * In <code>REBALANCING_MASTER_SERVER</code> state put should be committed
     * on stealer node. To follow Voldemort version guarantees, stealer node
     * should query donor node and put that value (proxyValue) before committing
     * the value from client.
     * <p>
     * Stealer node should ignore {@link ObsoleteVersionException} while
     * commiting proxyValue to local storage.
     * 
     * @param key Key
     * @param donorId donorId
     * @return Returns the proxy value
     * @throws VoldemortException if {@link #proxyGet(ByteArray, int)} fails
     */
    private List<Versioned<byte[]>> proxyGetAndLocalPut(ByteArray key,
                                                        int donorId,
                                                        byte[] transforms)
            throws VoldemortException {
        List<Versioned<byte[]>> proxyValues = proxyGet(key, donorId, transforms);
        for(Versioned<byte[]> proxyValue: proxyValues) {
            try {
                getInnerStore().put(key, proxyValue, null);
            } catch(ObsoleteVersionException e) {
                // ignore these
            }
        }
        return proxyValues;
    }

    /**
     * Similar to {@link #proxyGetAndLocalPut(ByteArray, int)} but meant for
     * {@link #getAll(Iterable)}
     * 
     * @param rebalancePartitionsInfoPerKey Map of keys which are being routed
     *        to their corresponding plan
     * @param transforms Map of key to their corresponding transforms
     * @return Returns a map of key to its corresponding list of values
     * @throws VoldemortException if {@link #proxyGetAll(List, List)} fails
     */
    private Map<ByteArray, List<Versioned<byte[]>>> proxyGetAllAndLocalPut(Map<ByteArray, RebalancePartitionsInfo> rebalancePartitionsInfoPerKey,
                                                                           Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        Map<ByteArray, List<Versioned<byte[]>>> proxyKeyValues = proxyGetAll(rebalancePartitionsInfoPerKey,
                                                                             transforms);
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> keyValuePair: proxyKeyValues.entrySet()) {
            for(Versioned<byte[]> proxyValue: keyValuePair.getValue()) {
                try {
                    getInnerStore().put(keyValuePair.getKey(), proxyValue, null);
                } catch(ObsoleteVersionException e) {
                    // ignore these
                }
            }
        }
        return proxyKeyValues;
    }

    /**
     * Get the {@link voldemort.store.socket.SocketStore} to redirect to for the
     * donor, creating one if needed.
     * 
     * @param storeName Name of the store
     * @param donorNodeId Donor node id
     * @return <code>SocketStore</code> object for <code>storeName</code> and
     *         <code>donorNodeId</code>
     */
    private Store<ByteArray, byte[], byte[]> getRedirectingSocketStore(String storeName,
                                                                       int donorNodeId) {
        if(!storeRepository.hasRedirectingSocketStore(storeName, donorNodeId)) {
            synchronized(storeRepository) {
                if(!storeRepository.hasRedirectingSocketStore(storeName, donorNodeId)) {
                    Node donorNode = getNodeIfPresent(donorNodeId);
                    logger.info("Creating new redirecting store for donor node "
                                + donorNode.getId() + " and store " + storeName);
                    storeRepository.addRedirectingSocketStore(donorNode.getId(),
                                                              storeFactory.create(storeName,
                                                                                  donorNode.getHost(),
                                                                                  donorNode.getSocketPort(),
                                                                                  RequestFormatType.PROTOCOL_BUFFERS,
                                                                                  RequestRoutingType.IGNORE_CHECKS));
                }
            }
        }

        return storeRepository.getRedirectingSocketStore(storeName, donorNodeId);
    }

    private Node getNodeIfPresent(int donorId) {
        try {
            return metadata.getCluster().getNodeById(donorId);
        } catch(Exception e) {
            throw new VoldemortException("Failed to get donorNode " + donorId
                                         + " from current cluster " + metadata.getCluster()
                                         + " at node " + metadata.getNodeId(), e);
        }
    }

    private void recordException(Node node, long startNs, UnreachableStoreException e) {
        failureDetector.recordException(node, (System.nanoTime() - startNs) / Time.NS_PER_MS, e);
    }

    private void recordSuccess(Node node, long startNs) {
        failureDetector.recordSuccess(node, (System.nanoTime() - startNs) / Time.NS_PER_MS);
    }
}
