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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The RedirectingStore extends {@link DelegatingStore}
 * <p>
 * if current server_state is {@link VoldemortState#REBALANCING_MASTER_SERVER} <br>
 * then before serving any client request do a remote get() call, put it locally
 * ignoring any {@link ObsoleteVersionException} and then serve the client
 * requests.
 */
public class RedirectingStore extends DelegatingStore<ByteArray, byte[]> {

    private final static Logger logger = Logger.getLogger(RedirectingStore.class);
    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private final SocketPool socketPool;
    private FailureDetector failureDetector;

    public RedirectingStore(Store<ByteArray, byte[]> innerStore,
                            MetadataStore metadata,
                            StoreRepository storeRepository,
                            FailureDetector detector,
                            SocketPool socketPool) {
        super(innerStore);
        this.metadata = metadata;
        this.storeRepository = storeRepository;
        this.socketPool = socketPool;
        this.failureDetector = detector;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        metadata.readLock.lock();
        try {
            if(redirectingKey(key)) {
                // if I am rebalancing for this key, try to do remote get() , put it
                // locally first to get the correct version ignoring any
                // ObsoleteVersionExceptions.
                proxyGetAndLocalPut(key);
            }
        } finally {
            metadata.readLock.unlock();
        }

        getInnerStore().put(key, value);
    }

    private boolean redirectingKey(ByteArray key) {
        if (VoldemortState.REBALANCING_MASTER_SERVER.equals(metadata.getServerState())) {
            List<Integer> partitionIds = metadata.getRoutingStrategy(getName()).getPartitionList(key.get());
            if (getRebalancePartitionsInfo(partitionIds) != null)
                return true;
        }

        return false;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        metadata.readLock.lock();
        try {
            if(redirectingKey(key)) {
                // if I am rebalancing for this key, try to do remote get() , put it
                // locally first to get the correct version ignoring any
                // ObsoleteVersionExceptions.
                proxyGetAndLocalPut(key);
            }
        } finally {
            metadata.readLock.unlock();
        }

        return getInnerStore().get(key);
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        metadata.readLock.lock();
        try {
            if(redirectingKey(key)) {
                // if I am rebalancing for this key, try to do remote get() , put it
                // locally first to get the correct version ignoring any
                // ObsoleteVersionExceptions.
                proxyGetAndLocalPut(key);
            }
        } finally {
            metadata.readLock.unlock();
        }

        return getInnerStore().getVersions(key);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        metadata.readLock.lock();
        try {
            List<ByteArray> redirectingKeys = new ArrayList<ByteArray>();
            for(ByteArray key: keys) {
                if(redirectingKey(key))
                    redirectingKeys.add(key);
            }

            if(!redirectingKeys.isEmpty())
                proxyGetAllAndLocalPut(redirectingKeys);
        } finally {
            metadata.readLock.unlock();
        }

        return getInnerStore().getAll(keys);
    }

    /**
     * TODO : handle delete correctly <br>
     * option1: delete locally and on remote node as well, the issue is cursor
     * is open in READ_UNCOMMITED mode while rebalancing and can push the value
     * back.<br>
     * option2: keep it in separate slop store and apply deletes at the end of
     * rebalancing.<br>
     * option3: donot worry about deletes for now, voldemort in general have
     * this issue if node went down while delete will still keep the old
     * version.
     */
    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return getInnerStore().delete(key, version);
    }

    /**
     * Finds the first {@link voldemort.client.rebalance.RebalancePartitionsInfo} containing
     * any of supplied partitions ids for the store being redirected. Determines which
     * rebalance operation (if any) happening to the present store impacts a partition in this list.
     *
     * @param partitionIds List of partitions
     * @return <code>null</code> if none found
     */
    private RebalancePartitionsInfo getRebalancePartitionsInfo(List<Integer> partitionIds) {
        RebalancerState rebalancerState = metadata.getRebalancerState();
        return rebalancerState.find(getName(), partitionIds);
    }

    /**
     * Performs a back-door proxy get to 
     * {@link voldemort.client.rebalance.RebalancePartitionsInfo#getDonorId() getDonorId}
     *
     * @param key Key
     * @throws ProxyUnreachableException if donor node can't be reached
     */
    private List<Versioned<byte[]>> proxyGet(ByteArray key) throws VoldemortException {
        List<Integer> partitionIds = metadata.getRoutingStrategy(getName()).getPartitionList(key.get());
        RebalancePartitionsInfo rebalancePartitionsInfo = getRebalancePartitionsInfo(partitionIds);

        if (rebalancePartitionsInfo == null) {
            throw new IllegalStateException("No steal operation in progress for key " + key +
                                            " partitions(" + Joiner.on(", ").join(partitionIds) + ")");
        }

        Node donorNode = metadata.getCluster().getNodeById(rebalancePartitionsInfo.getDonorId());
        checkNodeAvailable(donorNode);
        long startNs = System.nanoTime();
        try {
            List<Versioned<byte[]>> values = getRedirectingSocketStore(getName(),
                                                                       rebalancePartitionsInfo.getDonorId()).get(key);
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
     * {@link voldemort.client.rebalance.RebalancePartitionsInfo#getDonorId() getDonorId}
     *
     * @param keys Iterable list of keys
     * @throws ProxyUnreachableException if donor node can't be reached
     */
    private Map<ByteArray, List<Versioned<byte[]>>> proxyGetAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        Multimap<Integer, ByteArray> scatterMap = HashMultimap.create();
        int numKeys=0;
        for (ByteArray key: keys) {
            numKeys++;
            RebalancerState rebalancerState = metadata.getRebalancerState();
            for (RebalancePartitionsInfo stealInfo: rebalancerState.find(getName())) {
                byte[] keyBytes = key.get();
                
                for (int p: metadata.getRoutingStrategy(getName()).getPartitionList(keyBytes)) {
                    if (stealInfo.getPartitionList().contains(p))
                        scatterMap.put(stealInfo.getDonorId(), key);
                }
            }
        }

        Map<ByteArray, List<Versioned<byte[]>>> gatherMap = new HashMap<ByteArray, List<Versioned<byte[]>>>(numKeys);

        for (int donorNodeId: scatterMap.keySet()) {
            Node donorNode = metadata.getCluster().getNodeById(donorNodeId);
            checkNodeAvailable(donorNode);
            long startNs = System.nanoTime();

            try {
                Map<ByteArray, List<Versioned<byte[]>>> resultsForNode = getRedirectingSocketStore(getName(),
                                                                                                   donorNodeId).getAll(scatterMap.get(donorNodeId));
                recordSuccess(donorNode, startNs);

                for (Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: resultsForNode.entrySet()) {
                    gatherMap.put(entry.getKey(), entry.getValue());
                }
            } catch (UnreachableStoreException e) {
                recordException(donorNode, startNs, e);
                throw new ProxyUnreachableException("Failed to reach proxy node " + donorNode, e);
            }
        }

        return gatherMap;
    }

    /**
     * In <code>REBALANCING_MASTER_SERVER</code> state
     * put should be committed on stealer node. To follow Voldemort version guarantees, stealer
     * node should query donor node and put that value (proxyValue) before
     * committing the value from client.
     * <p>
     * Stealer node should ignore {@link ObsoleteVersionException} while commiting proxyValue
     * to local storage.
     *
     * @param key Key
     * @throws VoldemortException if {@link #proxyGet(voldemort.utils.ByteArray)} fails
     */
    private void proxyGetAndLocalPut(ByteArray key) throws VoldemortException {
        List<Versioned<byte[]>> proxyValues = proxyGet(key);
        for(Versioned<byte[]> proxyValue: proxyValues) {
            try {

                getInnerStore().put(key, proxyValue);

            } catch(ObsoleteVersionException e) {
                // ignore these
            }
        }
    }

    /**
     * Similar to {@link #proxyGetAllAndLocalPut(Iterable)} but meant for {@link #getAll(Iterable)}
     *
     * @param keys Iterable collection of keys of keys
     * @throws VoldemortException if {@link #proxyGetAll(Iterable)} fails
     */
    private void proxyGetAllAndLocalPut(Iterable<ByteArray> keys) throws VoldemortException {
        Map<ByteArray, List<Versioned<byte[]>>> proxyKeyValues = proxyGetAll(keys);
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> keyValuePair: proxyKeyValues.entrySet()) {
            for(Versioned<byte[]> proxyValue: keyValuePair.getValue()) {
                try {
                    getInnerStore().put(keyValuePair.getKey(), proxyValue);
                } catch(ObsoleteVersionException e) {
                    // ignore these
                }
            }
        }

    }

    /**
     * Get the {@link voldemort.store.socket.SocketStore} to redirect to for the donor, creating one if needed.
     * 
     * @param storeName Name of the store
     * @param donorNodeId Donor node id
     * @return <code>SocketStore</code> object for <code>storeName</code> and <code>donorNodeId</code>
     */
    private Store<ByteArray, byte[]> getRedirectingSocketStore(String storeName, int donorNodeId) {
        if(!storeRepository.hasRedirectingSocketStore(storeName, donorNodeId)) {
            synchronized(storeRepository) {
                if(!storeRepository.hasRedirectingSocketStore(storeName, donorNodeId)) {
                    Node donorNode = getNodeIfPresent(donorNodeId);
                    logger.info("Creating redirectingSocketStore for donorNode " + donorNode
                                + " store " + storeName);
                    storeRepository.addRedirectingSocketStore(donorNode.getId(),
                                                              new SocketStore(storeName,
                                                                              new SocketDestination(donorNode.getHost(),
                                                                                                    donorNode.getSocketPort(),
                                                                                                    RequestFormatType.PROTOCOL_BUFFERS),
                                                                              socketPool,
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
