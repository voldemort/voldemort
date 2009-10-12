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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.ServerState;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Abstract class for AdminClientRequestFormat. defines helper functions and
 * abstract functions need to be extended by different clients.
 * 
 * @author bbansal
 */
public abstract class AdminClientRequestFormat {

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    private final MetadataStore metadata;

    public AdminClientRequestFormat(MetadataStore metadata) {
        this.metadata = metadata;
    }

    /* Abstract functions need to be overwritten */

    /**
     * Updates Metadata at (remote) Node
     * 
     * @param nodeId
     * @param storesList
     * @throws VoldemortException
     */
    public abstract void doUpdateRemoteMetadata(int remoteNodeId,
                                                ByteArray key,
                                                Versioned<byte[]> value);

    /**
     * Get Metadata from (remote) Node
     * 
     * @param nodeId
     * @param storesList
     * @throws VoldemortException
     */
    public abstract Versioned<byte[]> doGetRemoteMetadata(int remoteNodeId, ByteArray key);

    /**
     * provides a mechanism to do forcedGet on (remote) store, Overrides all
     * security checks and return the value. queries the raw storageEngine at
     * server end to return the value
     * 
     * @param proxyDestNodeId
     * @param storeName
     * @param key
     * @return List<Versioned <byte[]>>
     */
    public abstract List<Versioned<byte[]>> doRedirectGet(int proxyDestNodeId,
                                                          String storeName,
                                                          ByteArray key);

    /**
     * streaming API to get all entries belonging to any of the partition in the
     * input List.
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filterRequest: <imp>Do not fetch entries filtered out (returned
     *        false) from the {@link VoldemortFilter} implementation</imp>
     * @return
     * @throws VoldemortException
     */
    public abstract Iterator<Pair<ByteArray, Versioned<byte[]>>> doFetchPartitionEntries(int nodeId,
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
     * @param filterRequest: <imp>Do not Update entries filtered out (returned
     *        false) from the {@link VoldemortFilter} implementation</imp>
     * @throws VoldemortException
     * @throws IOException
     */
    public abstract void doUpdatePartitionEntries(int nodeId,
                                                  String storeName,
                                                  Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                                  VoldemortFilter filter);

    /**
     * Delete all Entries at (remote) node for partitions in partitionList
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filterRequest: <imp>Do not Delete entries filtered out (returned
     *        false) from the {@link VoldemortFilter} implementation</imp>
     * @throws VoldemortException
     * @throws IOException
     */
    public abstract int doDeletePartitionEntries(int nodeId,
                                                 String storeName,
                                                 List<Integer> partitionList,
                                                 VoldemortFilter filter);

    /* helper functions */

    public Node getConnectedNode() {
        return metadata.getCluster().getNodeById(metadata.getNodeId());
    }

    public MetadataStore getMetadata() {
        return metadata;
    }

    /* get/update cluster metadata */
    public void updateClusterMetadata(int nodeId, Cluster cluster) throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getClusterMetadata(nodeId).getVersion();

        doUpdateRemoteMetadata(nodeId,
                               new ByteArray(ByteUtils.getBytes(MetadataStore.CLUSTER_KEY, "UTF-8")),
                               new Versioned<byte[]>(ByteUtils.getBytes(clusterMapper.writeCluster(cluster),
                                                                        "UTF-8"),
                                                     oldClock.incremented(nodeId, 1)));
    }

    public Versioned<Cluster> getClusterMetadata(int nodeId) throws VoldemortException {
        Versioned<byte[]> value = doGetRemoteMetadata(nodeId,
                                                      new ByteArray(ByteUtils.getBytes(MetadataStore.CLUSTER_KEY,
                                                                                       "UTF-8")));
        Cluster cluster = clusterMapper.readCluster(new StringReader(ByteUtils.getString(value.getValue(),
                                                                                         "UTF-8")));
        return new Versioned<Cluster>(cluster, value.getVersion());

    }

    /* get/update Store metadata */
    public void updateStoresMetadata(int nodeId, List<StoreDefinition> storesList)
            throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getStoresMetadata(nodeId).getVersion();

        doUpdateRemoteMetadata(nodeId,
                               new ByteArray(ByteUtils.getBytes(MetadataStore.STORES_KEY, "UTF-8")),
                               new Versioned<byte[]>(ByteUtils.getBytes(storeMapper.writeStoreList(storesList),
                                                                        "UTF-8"),
                                                     oldClock.incremented(nodeId, 1)));
    }

    public Versioned<List<StoreDefinition>> getStoresMetadata(int nodeId) throws VoldemortException {
        Versioned<byte[]> value = doGetRemoteMetadata(nodeId,
                                                      new ByteArray(ByteUtils.getBytes(MetadataStore.STORES_KEY,
                                                                                       "UTF-8")));
        List<StoreDefinition> storeList = storeMapper.readStoreList(new StringReader(ByteUtils.getString(value.getValue(),
                                                                                                         "UTF-8")));
        return new Versioned<List<StoreDefinition>>(storeList, value.getVersion());
    }

    /* get/update Server state metadata */
    public void updateServerState(int nodeId, MetadataStore.ServerState state) {
        VectorClock oldClock = (VectorClock) getServerState(nodeId).getVersion();

        doUpdateRemoteMetadata(nodeId,
                               new ByteArray(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                "UTF-8")),
                               new Versioned<byte[]>(ByteUtils.getBytes(state.toString(), "UTF-8"),
                                                     oldClock.incremented(nodeId, 1)));
    }

    public Versioned<ServerState> getServerState(int nodeId) {
        Versioned<byte[]> value = doGetRemoteMetadata(nodeId,
                                                      new ByteArray(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                                       "UTF-8")));
        return new Versioned<ServerState>(ServerState.valueOf(ByteUtils.getString(value.getValue(),
                                                                                  "UTF-8")),
                                          value.getVersion());
    }

    /* get/update Proxy Destination Node while rebalancing */
    public void updateRebalancingProxyDest(int rebalancingNodeId, int proxyDestNodeId) {
        VectorClock oldClock = (VectorClock) getRebalancingProxyDest(rebalancingNodeId).getVersion();

        doUpdateRemoteMetadata(rebalancingNodeId,
                               new ByteArray(ByteUtils.getBytes(MetadataStore.REBALANCING_PROXY_DEST_KEY,
                                                                "UTF-8")),
                               new Versioned<byte[]>(ByteUtils.getBytes("" + proxyDestNodeId,
                                                                        "UTF-8"),
                                                     oldClock.incremented(rebalancingNodeId, 1)));
    }

    public Versioned<Integer> getRebalancingProxyDest(int rebalancingNodeId) {
        Versioned<byte[]> value = doGetRemoteMetadata(rebalancingNodeId,
                                                      new ByteArray(ByteUtils.getBytes(MetadataStore.REBALANCING_PROXY_DEST_KEY,
                                                                                       "UTF-8")));
        return new Versioned<Integer>(Integer.parseInt(ByteUtils.getString(value.getValue(),
                                                                           "UTF-8")),
                                      value.getVersion());
    }

    /* get/update Rebalancing partition list */
    public void updateRebalancingPartitionList(int rebalancingNodeId, List<Integer> partitionList) {
        StringBuilder partitionListString = new StringBuilder();
        for(int i = 0; i < partitionList.size(); i++) {
            partitionListString.append("" + partitionList.get(i));
            if(i < partitionList.size() - 1) {
                partitionListString.append(",");
            }
        }

        VectorClock oldClock = (VectorClock) getRebalancingPartitionList(rebalancingNodeId).getVersion();

        doUpdateRemoteMetadata(rebalancingNodeId,
                               new ByteArray(ByteUtils.getBytes(MetadataStore.REBALANCING_PARTITIONS_LIST_KEY,
                                                                "UTF-8")),
                               new Versioned<byte[]>(ByteUtils.getBytes(partitionListString.toString(),
                                                                        "UTF-8"),
                                                     oldClock.incremented(rebalancingNodeId, 1)));
    }

    public Versioned<List<Integer>> getRebalancingPartitionList(int rebalancingNodeId) {
        Versioned<byte[]> value = doGetRemoteMetadata(rebalancingNodeId,
                                                      new ByteArray(ByteUtils.getBytes(MetadataStore.REBALANCING_PARTITIONS_LIST_KEY,
                                                                                       "UTF-8")));
        String[] partitionList = ByteUtils.getString(value.getValue(), "UTF-8").split(",");
        List<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i < partitionList.length; i++) {
            list.add(Integer.parseInt(partitionList[i]));
        }

        return new Versioned<List<Integer>>(list, value.getVersion());
    }

    /* redirectGet() while proxy mode. */
    public List<Versioned<byte[]>> redirectGet(int redirectedNodeId, String storeName, ByteArray key) {
        return doRedirectGet(redirectedNodeId, storeName, key);
    }

    /* Streaming APIs */

    /**
     * Pipe fetch from donorNode and update stealerNode in streaming mode.
     */
    public void fetchAndUpdateStreams(int donorNodeId,
                                      int stealerNodeId,
                                      String storeName,
                                      List<Integer> stealList,
                                      VoldemortFilter filter) {
        doUpdatePartitionEntries(stealerNodeId, storeName, doFetchPartitionEntries(donorNodeId,
                                                                                   storeName,
                                                                                   stealList,
                                                                                   filter), null);
    }
}
