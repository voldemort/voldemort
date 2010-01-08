/*
 * Copyright 2008-2009 LinkedIn, Inc
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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
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
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * The RedirectingStore extends {@link DelegatingStore}
 * <p>
 * if current server_state is {@link VoldemortState#REBALANCING_MASTER_SERVER} <br>
 * then before serving any client request do a remote get() call, put it locally
 * ignoring any {@link ObsoleteVersionException} and then serve the client
 * requests.
 * 
 * @author bbansal
 * 
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
        if(redirectingKey(key)) {
            // if I am rebalancing for this key, try to do remote get() , put it
            // locally first to get the correct version ignoring any
            // ObsoleteVersionExceptions.
            proxyGetAndLocalPut(key);
        }

        getInnerStore().put(key, value);
    }

    private boolean redirectingKey(ByteArray key) {
        return MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER.equals(metadata.getServerState())
               && metadata.getRebalancingStealInfo().getUnbalancedStoreList().contains(getName())
               && checkKeyBelongsToStolenPartitions(key);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        if(redirectingKey(key)) {
            // if I am rebalancing for this key, try to do remote get() , put it
            // locally first to get the correct version ignoring any
            // ObsoleteVersionExceptions.
            proxyGetAndLocalPut(key);
        }

        return getInnerStore().get(key);
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        if(redirectingKey(key)) {
            // if I am rebalancing for this key, try to do remote get() , put it
            // locally first to get the correct version ignoring any
            // ObsoleteVersionExceptions.
            proxyGetAndLocalPut(key);
        }

        return getInnerStore().getVersions(key);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {

        proxyGetAllAndLocalPut(Iterables.filter(keys, new Predicate<ByteArray>() {

            public boolean apply(ByteArray key) {
                return redirectingKey(key);
            }
        }));

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

    protected boolean checkKeyBelongsToStolenPartitions(ByteArray key) {
        for(int partitionId: metadata.getRoutingStrategy(getName()).getPartitionList(key.get())) {
            if(metadata.getRebalancingStealInfo().getPartitionList().contains(partitionId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * performs back-door proxy get to {@link MetadataStore#getDonorNode()}
     * 
     * @param key
     * @return
     * @throws VoldemortException
     */
    private List<Versioned<byte[]>> proxyGet(ByteArray key) throws VoldemortException {
        Node donorNode = metadata.getCluster().getNodeById(metadata.getRebalancingStealInfo()
                                                                   .getDonorId());
        checkNodeAvailable(donorNode);
        try {
            List<Versioned<byte[]>> values = getRedirectingSocketStore(getName(),
                                                                       metadata.getRebalancingStealInfo()
                                                                               .getDonorId()).get(key);
            failureDetector.recordSuccess(donorNode);
            return values;
        } catch(UnreachableStoreException e) {
            failureDetector.recordException(donorNode, e);
            throw new ProxyUnreachableException("Failed to reach proxy node "
                                                           + donorNode, e);
        }
    }

    private void checkNodeAvailable(Node donorNode) {
        if(!failureDetector.isAvailable(donorNode))
            throw new ProxyUnreachableException("Failed to reach proxy node "
                                                           + donorNode
                                                           + " is marked down by failure detector.");
    }

    /**
     * performs back-door proxy get to {@link MetadataStore#getDonorNode()}
     * 
     * @param key
     * @return
     * @throws VoldemortException
     */
    private Map<ByteArray, List<Versioned<byte[]>>> proxyGetAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        Node donorNode = metadata.getCluster().getNodeById(metadata.getRebalancingStealInfo()
                                                                   .getDonorId());
        checkNodeAvailable(donorNode);
        try {
            Map<ByteArray, List<Versioned<byte[]>>> map = getRedirectingSocketStore(getName(),
                                                                                    metadata.getRebalancingStealInfo()
                                                                                            .getDonorId()).getAll(keys);
            failureDetector.recordSuccess(donorNode);
            return map;
        } catch(UnreachableStoreException e) {
            failureDetector.recordException(donorNode, e);
            throw new ProxyUnreachableException("Failed to reach proxy node "
                                                           + donorNode, e);
        }
    }

    /**
     * In RebalancingStealer state put should be commited on stealer node. <br>
     * to follow voldemort version guarantees stealer <br>
     * node should query donor node and put that value (proxyValue) before
     * committing the value from client.
     * <p>
     * stealer node should ignore {@link ObsoleteVersionException} while
     * commiting proxyValue
     * 
     * 
     * @param key
     * @param value
     * @throws VoldemortException
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
     * Create if needed redirectingSocketStore for the donorNode
     * 
     * @param storeName
     * @param donorNode
     * @return
     */
    private Store<ByteArray, byte[]> getRedirectingSocketStore(String storeName, int donorNodeId) {
        if(!storeRepository.hasRedirectingSocketStore(storeName, donorNodeId)) {
            Node donorNode = getNodeIfPresent(donorNodeId);
            logger.info("Creating redirectingSocketStore for donorNode " + donorNode + " store "
                        + storeName);
            storeRepository.addRedirectingSocketStore(donorNode.getId(),
                                                      new SocketStore(storeName,
                                                                      new SocketDestination(donorNode.getHost(),
                                                                                            donorNode.getSocketPort(),
                                                                                            RequestFormatType.PROTOCOL_BUFFERS),
                                                                      socketPool,
                                                                      RequestRoutingType.IGNORE_CHECKS));
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
}
