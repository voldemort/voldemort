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
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
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
import voldemort.utils.ByteUtils;
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
    private boolean isProxyPutEnabled;

    public RedirectingStore(Store<ByteArray, byte[], byte[]> innerStore,
                            MetadataStore metadata,
                            StoreRepository storeRepository,
                            FailureDetector detector,
                            SocketStoreFactory storeFactory,
                            boolean isProxyPutEnabled) {
        super(innerStore);
        this.metadata = metadata;
        this.storeRepository = storeRepository;
        this.storeFactory = storeFactory;
        this.failureDetector = detector;
        this.isRedirectingStoreEnabled = new AtomicBoolean(true);
        this.isProxyPutEnabled = isProxyPutEnabled;
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
        // TODO These is some unneccessary performance hit here. keys already
        // moved over will always result in OVE and time spent waiting on this
        // (esp for cross zone moves) would be a total waste. Need to rework to
        // this logic to incur this when necessary
        if(stealInfo != null) {
            if(logger.isTraceEnabled()) {
                logger.trace("Proxying GET on stealer:" + metadata.getNodeId() + " for  key "
                             + ByteUtils.toHexString(key.get()) + " to donor:"
                             + stealInfo.getDonorId());
            }
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
        // TODO same fixes apply here as in get(..) above
        if(stealInfo != null) {
            if(logger.isTraceEnabled()) {
                logger.trace("Proxying GETVERSIONS on stealer:" + metadata.getNodeId()
                             + " for  key " + ByteUtils.toHexString(key.get()) + " to donor:"
                             + stealInfo.getDonorId());
            }
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
        // TODO Same optimizations. Go to the proxy only for keys that this node
        // does not have..
        if(!rebalancePartitionsInfoPerKey.isEmpty()) {
            if(logger.isTraceEnabled()) {
                String keyStr = "";
                for(ByteArray key: keys)
                    keyStr += key + " ";
                logger.trace("Proxying GETALL on stealer:" + metadata.getNodeId() + " for  keys "
                             + keyStr);
            }
            proxyGetAllAndLocalPut(rebalancePartitionsInfoPerKey, transforms);
        }

        return getInnerStore().getAll(keys, transforms);
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
        // TODO same optimizations apply here.. If the key already exists skip
        // this
        if(stealInfo != null) {
            if(logger.isTraceEnabled()) {
                logger.trace("Proxying GET (before PUT) on stealer:" + metadata.getNodeId()
                             + " for  key " + ByteUtils.toHexString(key.get()) + " to donor:"
                             + stealInfo.getDonorId());
            }
            proxyGetAndLocalPut(key, stealInfo.getDonorId(), transforms);
        }

        // put the data locally, if this step fails, there will be no proxy puts
        getInnerStore().put(key, value, transforms);

        // TODO make this best effort async replication. Failures will be
        // logged and the server log will be post processed in case the
        // rebalancing fails and we move back to old topology
        if(isProxyPutEnabled && stealInfo != null) {
            proxyPut(key, value, transforms, stealInfo.getDonorId());
        }
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

    /**
     * Replay the put to the donor proxy node so we will have the data available
     * at the proxy host, in case the rebalancing fails.
     * 
     * NOTE: This logic depends on the assumption that all the replicas for this
     * partition in the old topology are now either donors during rebalancing or
     * still active replicas. Otherwise, some old replica might not have any
     * incoming proxy puts. As a result, if the rebalancing fails, the updates
     * during the rebalancing window would not have made it to all the old
     * replicas.
     * 
     * @param key
     * @param value
     * @param transforms
     * @param donorNodeId
     * @throws ProxyUnreachableException if donor node can't be reached
     */
    private void proxyPut(ByteArray key, Versioned<byte[]> value, byte[] transforms, int donorNodeId) {
        Node donorNode = metadata.getCluster().getNodeById(donorNodeId);

        // Check if the donor is still a replica for the key. If we send proxy
        // puts there, then we could have a situation where the online
        // replicated write could lose out to the proxy put and hence fail the
        // client operation with an OVE
        // TODO not sure constructing this object everytime is a good idea. But
        // the current design lack the ability to be able to clearly detect when
        // the server goes back to NORMAL mode
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(metadata.getStoreDef(getName()),
                                                                                             metadata.getCluster());
        if(routingStrategy.routeRequest(key.get()).contains(donorNode)) {
            if(logger.isTraceEnabled()) {
                logger.trace("Donor " + donorNode.getId()
                             + " still a replica in the updated cluster for the key "
                             + ByteUtils.toHexString(key.get()) + ". Skipping proxy put");
            }
            return;
        }

        checkNodeAvailable(donorNode);

        long startNs = System.nanoTime();
        try {
            Store<ByteArray, byte[], byte[]> redirectingStore = getRedirectingSocketStore(getName(),
                                                                                          donorNodeId);
            redirectingStore.put(key, value, transforms);
            recordSuccess(donorNode, startNs);
        } catch(UnreachableStoreException e) {
            recordException(donorNode, startNs, e);
            logger.error("Failed to reach proxy node " + donorNode, e);
        } catch(ObsoleteVersionException ove) {
            // Proxy puts can get an OVE if somehow there are two stealers for
            // the same donor and the other stealer's proxy put already got to
            // the donor.. This will not result from online put winning, since
            // we don't issue proxy puts if the donor is still a replica
            if(logger.isTraceEnabled()) {
                logger.trace("OVE in proxy put for donor: " + donorNodeId + " from stealer:"
                                     + metadata.getNodeId() + " key "
                                     + ByteUtils.toHexString(key.get()),
                             ove);
            }
        } catch(Exception e) {
            // Just log the key.. Not sure having values in the log is a good
            // idea.
            logger.error("Unexpected exception in proxy put for key:"
                                 + ByteUtils.toHexString(key.get()) + " against donor node "
                                 + donorNodeId,
                         e);
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
                // TODO this is in TRACE because OVE is expected here, for keys
                // that are already moved over or proxy got. This will become
                // ERROR later post redesign
                if(logger.isTraceEnabled())
                    logger.trace("OVE in proxy get local put for key "
                                         + ByteUtils.toHexString(key.get()) + " Stealer:"
                                         + metadata.getNodeId() + " Donor:" + donorId,
                                 e);
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
