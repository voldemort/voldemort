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

package voldemort.client.protocol.admin;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.cluster.Cluster;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * The base interface for all administrative functions.
 * 
 * @author bbansal
 * 
 */
public abstract class AdminClient {

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    private Cluster cluster;

    public AdminClient(Cluster cluster) {
        this.setCluster(cluster);
    }

    public AdminClient(String bootstrapUrl) {
        // try to bootstrap metadata from bootstrapUrl
        ClientConfig config = new ClientConfig().setBootstrapUrls(bootstrapUrl);
        SocketStoreClientFactory factory = new SocketStoreClientFactory(config);

        // get Cluster from bootStrapUrl
        String clusterXml = factory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY,
                                                                 factory.validateUrls(config.getBootstrapUrls()));
        setCluster(clusterMapper.readCluster(new StringReader(clusterXml)));

        // release all threads/sockets hold by the factory.
        factory.close();
    }

    /**
     * streaming API to get all entries belonging to any of the partition in the
     * input List.
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList: List of partitions to be fetched from remote
     *        server.
     * @param filter: A VoldemortFilter class to do server side filtering or
     *        null
     * @return
     * @throws VoldemortException
     */
    public abstract Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchPartitionEntries(int nodeId,
                                                                                       String storeName,
                                                                                       List<Integer> partitionList,
                                                                                       VoldemortFilter filter);

    /**
     * streaming API to get a list of all the keys that belong to any of the
     * partitions in the input list
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter
     * @return
     */
    public abstract Iterator<ByteArray> fetchPartitionKeys(int nodeId,
                                                           String storeName,
                                                           List<Integer> partitionList,
                                                           VoldemortFilter filter);

    /**
     * update Entries at (remote) node with all entries in iterator for passed
     * storeName
     * 
     * @param nodeId
     * @param storeName
     * @param entryIterator
     * @param filter: A VoldemortFilter class to do server side filtering or
     *        null.
     * @throws VoldemortException
     * @throws IOException
     */
    public abstract void updateEntries(int nodeId,
                                       String storeName,
                                       Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                       VoldemortFilter filter);

    /**
     * Delete all Entries at (remote) node for partitions in partitionList
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter: A VoldemortFilter class to do server side filtering or
     *        null.
     * @throws VoldemortException
     * @throws IOException
     */
    public abstract int deletePartitions(int nodeId,
                                         String storeName,
                                         List<Integer> partitionList,
                                         VoldemortFilter filter);

    /**
     * Pipe fetch from donorNode and update stealerNode in streaming mode.
     */
    public abstract int fetchAndUpdateStreams(int donorNodeId,
                                              int stealerNodeId,
                                              String storeName,
                                              List<Integer> stealList,
                                              VoldemortFilter filter);

    /**
     * Get the status of asynchornous request
     * 
     * @param nodeId Node to contact
     * @param requestId Previously returned request Id
     * @return A Pair of String (request status) and Boolean (is request
     *         complete?)
     */
    public abstract AsyncOperationStatus getAsyncRequestStatus(int nodeId, int requestId);

    /**
     * update remote metadata on a particular node
     * 
     * @param remoteNodeId
     * @param key
     * @param value
     */
    protected abstract void doUpdateRemoteMetadata(int remoteNodeId,
                                                   ByteArray key,
                                                   Versioned<byte[]> value);

    /**
     * get remote metadata on a particular node
     * 
     * @param remoteNodeId
     * @param key
     * @return
     */
    protected abstract Versioned<byte[]> doGetRemoteMetadata(int remoteNodeId, ByteArray key);

    /**
     * Wait for a task to finish completion, using exponential backoff to poll
     * the task completion status
     * 
     * @param nodeId Id of the node to poll
     * @param requestId Id of the request to check
     * @param maxWait Maximum time we'll keep checking a request until we give
     *        up
     * @param timeUnit Unit in which
     * @param maxWait is expressed
     * @return True if task finished in
     * @param maxWait , false otherwise
     */
    public boolean waitForCompletion(int nodeId, int requestId, long maxWait, TimeUnit timeUnit) {
        long delay = 250;
        long maxDelay = 1000 * 60; /*
                                    * don't do exponential back off past a
                                    * certain limit
                                    */
        long waitUntil = System.currentTimeMillis() + timeUnit.toMillis(maxWait);

        while(System.currentTimeMillis() < waitUntil) {
            AsyncOperationStatus status = getAsyncRequestStatus(nodeId, requestId);
            if(status.isComplete())
                return true;
            if(delay < maxDelay) /*
                                  * keep doubling the wait period until we rach
                                  * maxDelay
                                  */
                delay <<= 2;
            try {
                Thread.sleep(delay);
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return false;
    }

    public abstract void close();

    /* Helper functions */

    /**
     * update metadata at remote node.
     * 
     * @param remoteNodeId
     * @param key
     * @param value
     */
    public void updateRemoteMetadata(int remoteNodeId, String key, Versioned<String> value) {
        ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
        Versioned<byte[]> valueBytes = new Versioned<byte[]>(ByteUtils.getBytes(value.getValue(),
                                                                                "UTF-8"),
                                                             value.getVersion());

        doUpdateRemoteMetadata(remoteNodeId, keyBytes, valueBytes);
    }

    /**
     * get metadata from remote node.
     * 
     * @param remoteNodeId
     * @param key
     * @return
     */
    public Versioned<String> getRemoteMetadata(int remoteNodeId, String key) {
        ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
        Versioned<byte[]> value = doGetRemoteMetadata(remoteNodeId, keyBytes);
        return new Versioned<String>(ByteUtils.getString(value.getValue(), "UTF-8"),
                                     value.getVersion());
    }

    /**
     * update cluster information on a remote node.
     * 
     * @param nodeId
     * @param cluster
     * @throws VoldemortException
     */
    public void updateRemoteCluster(int nodeId, Cluster cluster) throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getRemoteCluster(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.CLUSTER_KEY,
                             new Versioned<String>(clusterMapper.writeCluster(cluster),
                                                   oldClock.incremented(nodeId, 1)));
    }

    /**
     * get cluster information from a remote node.
     * 
     * @param nodeId
     * @return
     * @throws VoldemortException
     */
    public Versioned<Cluster> getRemoteCluster(int nodeId) throws VoldemortException {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY);
        Cluster cluster = clusterMapper.readCluster(new StringReader(value.getValue()));
        return new Versioned<Cluster>(cluster, value.getVersion());

    }

    /**
     * update store definitions on remote node.
     * 
     * @param nodeId
     * @param storesList
     * @throws VoldemortException
     */
    public void updateRemoteStoreDefList(int nodeId, List<StoreDefinition> storesList)
            throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getRemoteStoreDefList(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.STORES_KEY,
                             new Versioned<String>(storeMapper.writeStoreList(storesList),
                                                   oldClock.incremented(nodeId, 1)));
    }

    /**
     * get store definitions from a remote node.
     * 
     * @param nodeId
     * @return
     * @throws VoldemortException
     */
    public Versioned<List<StoreDefinition>> getRemoteStoreDefList(int nodeId)
            throws VoldemortException {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.STORES_KEY);
        List<StoreDefinition> storeList = storeMapper.readStoreList(new StringReader(value.getValue()));
        return new Versioned<List<StoreDefinition>>(storeList, value.getVersion());
    }

    /**
     * update serverState on a remote node.
     * 
     * @param nodeId
     * @param state
     */
    public void updateRemoteServerState(int nodeId, MetadataStore.VoldemortState state) {
        VectorClock oldClock = (VectorClock) getRemoteServerState(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.SERVER_STATE_KEY,
                             new Versioned<String>(state.toString(),
                                                   oldClock.incremented(nodeId, 1)));
    }

    /**
     * get serverState from a remoteNode.
     * 
     * @param nodeId
     * @return
     */
    public Versioned<VoldemortState> getRemoteServerState(int nodeId) {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.SERVER_STATE_KEY);
        return new Versioned<VoldemortState>(VoldemortState.valueOf(value.getValue()),
                                             value.getVersion());
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Cluster getCluster() {
        return cluster;
    }
}
