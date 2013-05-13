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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxSetter;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
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
 * If current server_state is {@link VoldemortState#REBALANCING_MASTER_SERVER}
 * then handle incoming requests in the following way, if the key belongs to a
 * partition that this server is currently stealing. Such a server has what we
 * call a 'proxy node', which is the server which owned that partition as the
 * exact same type of replica in the same zone, as per the old cluster topology.
 * 
 * 1. getVersions/get<br>
 * If the server contains the key locally, then serve it directly. Else, fetch
 * from proxy node, update local storage and then serve it off that.
 * 
 * 2. getAll<br>
 * Similarly, for keys that exist locally, serve it off directly. Else,
 * fetch-update the missing keys from proxyNode and then serve them off local
 * storage.
 * 
 * 3. put <br>
 * First write it to local storage, then submit a async put() to the proxy node,
 * so we can safely abort the rebalancing if we have to.
 * 
 * 4. delete <br>
 * :) :) :)
 * 
 */
public class RedirectingStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private final static Logger logger = Logger.getLogger(RedirectingStore.class);
    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private final SocketStoreFactory storeFactory;
    private FailureDetector failureDetector;
    private AtomicBoolean isRedirectingStoreEnabled;
    private boolean isProxyPutEnabled;
    private final ExecutorService proxyPutWorkerPool;

    // statistics on proxy put tasks
    private final ProxyPutStats proxyPutStats;

    public RedirectingStore(Store<ByteArray, byte[], byte[]> innerStore,
                            MetadataStore metadata,
                            StoreRepository storeRepository,
                            FailureDetector detector,
                            SocketStoreFactory storeFactory,
                            boolean isProxyPutEnabled,
                            ExecutorService proxyPutWorkerPool,
                            ProxyPutStats proxyPutStats) {
        super(innerStore);
        this.metadata = metadata;
        this.storeRepository = storeRepository;
        this.storeFactory = storeFactory;
        this.failureDetector = detector;
        this.isRedirectingStoreEnabled = new AtomicBoolean(true);
        this.isProxyPutEnabled = isProxyPutEnabled;
        this.proxyPutWorkerPool = proxyPutWorkerPool;
        this.proxyPutStats = proxyPutStats;
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

    /**
     * If needed, satisfies the get request by redirecting calls to the remote
     * proxy node. Also updates local storage accordingly.
     * 
     * @param key
     * @param transforms
     * @return
     * @throws VoldemortException
     */
    private List<Versioned<byte[]>> redirectingGet(ByteArray key, byte[] transforms)
            throws VoldemortException {
        /**
         * If I am rebalancing for this key, try to do remote get(), put it
         * locally first to get the correct version ignoring any
         * {@link ObsoleteVersionException}
         */
        Integer redirectNode = getProxyNode(key.get());
        if(redirectNode != null) {
            // First, attempt a local get
            List<Versioned<byte[]>> vals = getInnerStore().get(key, transforms);
            // If found, return
            if(!vals.isEmpty()) {
                /*
                 * There is a subtle race here if the underlying storage does
                 * not implement multiVersionPut(), since the we could read some
                 * versions of the key without all of it being transferred over
                 * by the background fetch. This is not a problem if we do bulk
                 * atomic writes of multiple versions of the same key into
                 * storage a.k.a multiVersionPut
                 */
                return vals;
            }

            if(logger.isTraceEnabled()) {
                logger.trace("Proxying GET on stealer:" + metadata.getNodeId() + " for  key "
                             + ByteUtils.toHexString(key.get()) + " to node:" + redirectNode);
            }
            proxyGetAndLocalPut(key, redirectNode, transforms);
        }
        return getInnerStore().get(key, transforms);
    }

    /**
     * If needed, satisfies the getVersions request by redirecting calls to the
     * remote proxy node. Also updates local storage accordingly.
     * 
     * @param key
     * @param transforms
     * @return
     * @throws VoldemortException
     */
    private List<Version> redirectingGetVersions(ByteArray key) {
        /**
         * If I am rebalancing for this key, try to do remote get(), put it
         * locally first to get the correct version ignoring any
         * {@link ObsoleteVersionException}.
         */
        Integer redirectNode = getProxyNode(key.get());
        if(redirectNode != null) {
            // First, attempt a local getVersions()
            List<Version> versions = getInnerStore().getVersions(key);
            // If found some versions, return
            if(!versions.isEmpty()) {
                // Same caveat here as in redirectingGet(). Need multiVersionPut
                // support in storage to avoid seeing partial versions
                return versions;
            }

            if(logger.isTraceEnabled()) {
                logger.trace("Proxying GETVERSIONS on stealer:" + metadata.getNodeId()
                             + " for  key " + ByteUtils.toHexString(key.get()) + " to node:"
                             + redirectNode);
            }
            proxyGetAndLocalPut(key, redirectNode, null);
        }
        return getInnerStore().getVersions(key);
    }

    /**
     * If needed, satisfies the getAll request by redirecting calls to the
     * remote proxy node. Also updates local storage accordingly.
     * 
     * 
     * @param keys
     * @param transforms
     * @return
     * @throws VoldemortException
     */
    private Map<ByteArray, List<Versioned<byte[]>>> redirectingGetAll(Iterable<ByteArray> keys,
                                                                      Map<ByteArray, byte[]> transforms)
            throws VoldemortException {

        // first determine how many keys are already present locally.
        Map<ByteArray, List<Versioned<byte[]>>> localVals = getInnerStore().getAll(keys, transforms);
        Map<ByteArray, Integer> keyToProxyNodeMap = Maps.newHashMapWithExpectedSize(Iterables.size(keys));
        for(ByteArray key: keys) {
            // Relies on inner getAll() to not return an entry for the key in
            // the result hashmap, in case the key does not exist on storage
            if(localVals.containsKey(key)) {
                // if you have it locally, move to next key
                continue;
            }
            Integer redirectNode = getProxyNode(key.get());
            /*
             * Else check if we are rebalancing for the key.. Intuitively, if we
             * don't have the key, then we must be rebalancing for that key,
             * right? Otherwise the key should have been here? Wrong, what if
             * this is a non-existent key. We can't really confirm key does not
             * exist, without going to the proxy node..
             */
            if(redirectNode != null) {
                /*
                 * If we are indeed rebalancing for the key, then a proxy fetch
                 * will make things certain.
                 */
                keyToProxyNodeMap.put(key, redirectNode);
            }
        }

        // If all keys were present locally, return. If not, do proxy fetch
        if(!keyToProxyNodeMap.isEmpty()) {
            if(logger.isTraceEnabled()) {
                String keyStr = "";
                for(ByteArray key: keys)
                    keyStr += key + " ";
                logger.trace("Proxying GETALL on stealer:" + metadata.getNodeId() + " for  keys "
                             + keyStr);
            }
            // Issue proxy fetches for non-rebalancing keys that did not exist
            // locally
            proxyGetAllAndLocalPut(keyToProxyNodeMap, transforms);
            // Now, issue a getAll for those keys alone
            Map<ByteArray, List<Versioned<byte[]>>> proxyFetchedVals = getInnerStore().getAll(keyToProxyNodeMap.keySet(),
                                                                                              transforms);
            // Merge the results
            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: proxyFetchedVals.entrySet()) {
                localVals.put(entry.getKey(), entry.getValue());
            }
        }
        return localVals;
    }

    /**
     * This is slightly different from other redirecting*** methods in that,
     * this updates the remote proxy node, with this put request, so we can
     * switch back to the old cluster topology if needed
     * 
     * @param key
     * @param value
     * @param transforms
     * @throws VoldemortException
     */
    private void redirectingPut(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        Cluster currentCluster = metadata.getCluster();
        // TODO:refactor O(n) linear lookup of storedef here. Ideally should be
        // a hash lookup.
        StoreDefinition storeDef = metadata.getStoreDef(getName());
        /*
         * defensively, error out if this is a read-only store and someone is
         * doing puts against it. We don't to do extra work and fill the log
         * with errors in that case.
         */
        if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
            throw new UnsupportedOperationException("put() not supported on read-only store");
        }
        StoreRoutingPlan currentRoutingPlan = new StoreRoutingPlan(currentCluster, storeDef);
        Integer redirectNode = getProxyNode(currentRoutingPlan, storeDef, key.get());
        /**
         * If I am rebalancing for this key, try to do remote get() if this node
         * does not have the key , put it locally first to get the correct
         * version ignoring any {@link ObsoleteVersionException}
         */
        if(redirectNode != null) {
            /*
             * first check if the key exists locally. If so, it means, it has
             * been moved over (either by a proxy fetch or background fetch) and
             * we are good simply issuing the put on top of that.
             */
            List<Versioned<byte[]>> vals = getInnerStore().get(key, transforms);
            if(vals.isEmpty()) {
                // if not, then go proxy fetch it
                if(logger.isTraceEnabled()) {
                    logger.trace("Proxying GET (before PUT) on stealer:" + metadata.getNodeId()
                                 + " for  key " + ByteUtils.toHexString(key.get()) + " to node:"
                                 + redirectNode);
                }
                proxyGetAndLocalPut(key, redirectNode, transforms);
            }
        }

        // Here we are sure that the current node has caught up with the proxy
        // for this key. Moving on to the put logic.
        // put the data locally, if this step fails, there will be no proxy puts
        getInnerStore().put(key, value, transforms);

        // submit an async task to issue proxy puts to the redirectNode
        // NOTE : if the redirect node is also a current replica for the key (we
        // could have a situation where the online replicated write could lose
        // out to the proxy put and hence fail the client operation with an
        // OVE). So do not send proxy puts in those cases.
        if(isProxyPutEnabled && redirectNode != null
           && !currentRoutingPlan.getReplicationNodeList(key.get()).contains(redirectNode)) {
            AsyncProxyPutTask asyncProxyPutTask = new AsyncProxyPutTask(this,
                                                                        key,
                                                                        value,
                                                                        transforms,
                                                                        redirectNode);
            proxyPutStats.reportProxyPutSubmission();
            proxyPutWorkerPool.submit(asyncProxyPutTask);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        if(isServerRebalancing()) {
            return redirectingGet(key, transforms);
        } else {
            return getInnerStore().get(key, transforms);
        }
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        if(isServerRebalancing()) {
            return redirectingGetVersions(key);
        } else {
            return getInnerStore().getVersions(key);
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        if(isServerRebalancing()) {
            return redirectingGetAll(keys, transforms);
        } else {
            return getInnerStore().getAll(keys, transforms);
        }
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        if(isServerRebalancing()) {
            redirectingPut(key, value, transforms);
        } else {
            getInnerStore().put(key, value, transforms);
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

    public boolean isServerRebalancing() {
        return VoldemortState.REBALANCING_MASTER_SERVER.equals(metadata.getServerState());
    }

    /**
     * Checks if the server has to do any proxying of gets/puts to another
     * server, as a part of an ongoing rebalance operation.
     * 
     * Basic idea : Any given node which is a stealer of a partition, as the ith
     * replica of a given zone, will proxy to the old ith replica of the
     * partition in the given zone, as per the source cluster metadata.
     * Exception : if this amounts to proxying to itself.
     * 
     * Note on Zone Expansion : For zone expansion, there will be no proxying
     * within the new zone. This is a practical assumption since if we fail, we
     * fallback to a cluster topology without the new zone. As a result, reads
     * from the new zone are not guaranteed to return some values during the
     * course of zone expansion. This is a also reasonable since any
     * organization undertaking such effort would need to have the data in place
     * in the new zone, before the client apps are moved over.
     * 
     * TODO:refactor Add helper methods to StoreRoutingPlan to simplify this
     * code
     * 
     * @param currentRoutingPlan routing plan object based on cluster's current
     *        topology
     * @param storeDef definition of the store being redirected
     * @param key to decide where to proxy to
     * @return Null if no proxying is required else node id of the server to
     *         proxy to
     */
    private Integer getProxyNode(StoreRoutingPlan currentRoutingPlan,
                                 StoreDefinition storeDef,
                                 byte[] key) {
        // get out if redirecting is disabled.
        if(!isRedirectingStoreEnabled.get()) {
            return null;
        }

        // TODO a better design would be to get these state changes from
        // metadata listener callbacks, so we need not allocate these objects
        // all the time
        Cluster sourceCluster = metadata.getRebalancingSourceCluster();
        if(sourceCluster == null) {
            /*
             * This is more for defensive coding purposes. The update of the
             * source cluster key happens before the server is put in
             * REBALANCING mode and is reset to null after the server goes back
             * to NORMAL mode.
             */
            if(logger.isTraceEnabled()) {
                logger.trace("Old Cluster is null.. bail");
            }
            return null;
        }

        Integer nodeId = metadata.getNodeId();
        Integer zoneId = currentRoutingPlan.getCluster().getNodeById(nodeId).getZoneId();

        StoreRoutingPlan oldRoutingPlan = new StoreRoutingPlan(sourceCluster, storeDef);
        // Check the current node's relationship to the key.
        int zoneReplicaType = currentRoutingPlan.getZoneReplicaType(zoneId, nodeId, key);
        // Determine which node held the key with the same relationship in the
        // old cluster. That is your man!
        Integer redirectNodeId;
        try {
            redirectNodeId = oldRoutingPlan.getZoneReplicaNode(zoneId, zoneReplicaType, key);
        } catch(VoldemortException ve) {
            /*
             * If the zone does not exist, as in the case of Zone Expansion,
             * there will be no proxy bridges built. The only other time an
             * exception can be thrown here is when the replicaType is invalid.
             * But that would mean we are changing say a 2/1/1 store to 3/2/2,
             * which Voldemort currently does not support anyway
             */
            return null;
        }
        // Unless he is the same as this node (where this is meaningless effort)
        if(redirectNodeId == nodeId) {
            return null;
        }
        return redirectNodeId;
    }

    /**
     * Wrapper around
     * {@link RedirectingStore#getProxyNode(StoreRoutingPlan, StoreDefinition, byte[])}
     * 
     * @param key
     * @return
     */
    private Integer getProxyNode(byte[] key) {
        Cluster currentCluster = metadata.getCluster();
        StoreDefinition storeDef = metadata.getStoreDef(getName());
        // TODO Ideally, this object construction should be done only when
        // metadata changes using a listener mechanism
        StoreRoutingPlan currentRoutingPlan = new StoreRoutingPlan(currentCluster, storeDef);
        return getProxyNode(currentRoutingPlan, storeDef, key);
    }

    /**
     * Performs a back-door proxy get to proxy node
     * 
     * @param key Key
     * @param proxyNodeId proxy node id
     * @throws ProxyUnreachableException if proxy node can't be reached
     */
    private List<Versioned<byte[]>> proxyGet(ByteArray key, int proxyNodeId, byte[] transform) {
        Node proxyNode = metadata.getCluster().getNodeById(proxyNodeId);
        checkNodeAvailable(proxyNode);
        long startNs = System.nanoTime();
        try {
            Store<ByteArray, byte[], byte[]> redirectingStore = getRedirectingSocketStore(getName(),
                                                                                          proxyNodeId);
            List<Versioned<byte[]>> values = redirectingStore.get(key, transform);
            recordSuccess(proxyNode, startNs);
            return values;
        } catch(UnreachableStoreException e) {
            recordException(proxyNode, startNs, e);
            throw new ProxyUnreachableException("Failed to reach proxy node " + proxyNode, e);
        }
    }

    protected void checkNodeAvailable(Node proxyNode) {
        if(!failureDetector.isAvailable(proxyNode))
            throw new ProxyUnreachableException("Failed to reach proxy node " + proxyNode
                                                + " is marked down by failure detector.");
    }

    /**
     * Performs a back-door proxy getAll
     * 
     * @param keyToProxyNodeMap Map of keys to corresponding proxy nodes housing
     *        the keys in source cluster
     * @param transforms Map of keys to their corresponding transforms
     * @throws ProxyUnreachableException if proxy node can't be reached
     */
    private Map<ByteArray, List<Versioned<byte[]>>> proxyGetAll(Map<ByteArray, Integer> keyToProxyNodeMap,
                                                                Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        Multimap<Integer, ByteArray> proxyNodeToKeys = HashMultimap.create();
        int numKeys = 0;

        // Transform the map of key to plan to a map of proxy node id to keys
        for(Map.Entry<ByteArray, Integer> entry: keyToProxyNodeMap.entrySet()) {
            numKeys++;
            proxyNodeToKeys.put(entry.getValue(), entry.getKey());
        }

        Map<ByteArray, List<Versioned<byte[]>>> gatherMap = Maps.newHashMapWithExpectedSize(numKeys);

        for(int proxyNodeId: proxyNodeToKeys.keySet()) {
            Node proxyNode = metadata.getCluster().getNodeById(proxyNodeId);
            checkNodeAvailable(proxyNode);
            long startNs = System.nanoTime();

            try {
                Map<ByteArray, List<Versioned<byte[]>>> resultsForNode = getRedirectingSocketStore(getName(),
                                                                                                   proxyNodeId).getAll(proxyNodeToKeys.get(proxyNodeId),
                                                                                                                       transforms);
                recordSuccess(proxyNode, startNs);

                for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: resultsForNode.entrySet()) {
                    gatherMap.put(entry.getKey(), entry.getValue());
                }
            } catch(UnreachableStoreException e) {
                recordException(proxyNode, startNs, e);
                throw new ProxyUnreachableException("Failed to reach proxy node " + proxyNode, e);
            }
        }

        return gatherMap;
    }

    /**
     * In <code>REBALANCING_MASTER_SERVER</code> state put should be committed
     * on stealer node. To follow Voldemort version guarantees, stealer node
     * should query proxy node and put that value (proxyValue) before committing
     * the value from client.
     * <p>
     * Stealer node should ignore {@link ObsoleteVersionException} while
     * commiting proxyValue to local storage.
     * 
     * @param key Key
     * @param proxyId proxy node id
     * @return Returns the proxy value
     * @throws VoldemortException if {@link #proxyGet(ByteArray, int)} fails
     */
    private List<Versioned<byte[]>> proxyGetAndLocalPut(ByteArray key,
                                                        int proxyId,
                                                        byte[] transforms)
            throws VoldemortException {
        List<Versioned<byte[]>> proxyValues = proxyGet(key, proxyId, transforms);
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
                                         + metadata.getNodeId() + " ProxyNode:" + proxyId,
                                 e);
            }
        }
        return proxyValues;
    }

    /**
     * Similar to {@link #proxyGetAndLocalPut(ByteArray, int)} but meant for
     * {@link #getAll(Iterable)}
     * 
     * @param keyToProxyNodeMap Map of keys which are being routed to their
     *        corresponding proxy nodes
     * @param transforms Map of key to their corresponding transforms
     * @return Returns a map of key to its corresponding list of values
     * @throws VoldemortException if {@link #proxyGetAll(List, List)} fails
     */
    private Map<ByteArray, List<Versioned<byte[]>>> proxyGetAllAndLocalPut(Map<ByteArray, Integer> keyToProxyNodeMap,
                                                                           Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        Map<ByteArray, List<Versioned<byte[]>>> proxyKeyValues = proxyGetAll(keyToProxyNodeMap,
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
     * proxy node, creating one if needed.
     * 
     * @param storeName Name of the store
     * @param proxyNodeId proxy node id
     * @return <code>SocketStore</code> object for <code>storeName</code> and
     *         <code>proxyNodeId</code>
     */
    protected Store<ByteArray, byte[], byte[]> getRedirectingSocketStore(String storeName,
                                                                         int proxyNodeId) {
        if(!storeRepository.hasRedirectingSocketStore(storeName, proxyNodeId)) {
            synchronized(storeRepository) {
                if(!storeRepository.hasRedirectingSocketStore(storeName, proxyNodeId)) {
                    Node proxyNode = getNodeIfPresent(proxyNodeId);
                    logger.info("Creating new redirecting store for proxy node "
                                + proxyNode.getId() + " and store " + storeName);
                    storeRepository.addRedirectingSocketStore(proxyNode.getId(),
                                                              storeFactory.create(storeName,
                                                                                  proxyNode.getHost(),
                                                                                  proxyNode.getSocketPort(),
                                                                                  RequestFormatType.PROTOCOL_BUFFERS,
                                                                                  RequestRoutingType.IGNORE_CHECKS));
                }
            }
        }

        return storeRepository.getRedirectingSocketStore(storeName, proxyNodeId);
    }

    private Node getNodeIfPresent(int proxyNodeId) {
        try {
            return metadata.getCluster().getNodeById(proxyNodeId);
        } catch(Exception e) {
            throw new VoldemortException("Failed to get proxyNode " + proxyNodeId
                                         + " from current cluster " + metadata.getCluster()
                                         + " at node " + metadata.getNodeId(), e);
        }
    }

    protected void recordException(Node node, long startNs, UnreachableStoreException e) {
        failureDetector.recordException(node, (System.nanoTime() - startNs) / Time.NS_PER_MS, e);
    }

    protected void recordSuccess(Node node, long startNs) {
        failureDetector.recordSuccess(node, (System.nanoTime() - startNs) / Time.NS_PER_MS);
    }

    protected MetadataStore getMetadataStore() {
        return metadata;
    }

    protected void reportProxyPutFailure() {
        proxyPutStats.reportProxyPutFailure();
    }

    protected void reportProxyPutSuccess() {
        proxyPutStats.reportProxyPutCompletion();
    }

    public ProxyPutStats getProxyPutStats() {
        return this.proxyPutStats;
    }
}
