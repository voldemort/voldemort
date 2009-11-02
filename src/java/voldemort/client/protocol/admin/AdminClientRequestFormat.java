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
public abstract class AdminClientRequestFormat {

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    private final MetadataStore metadata;

    public AdminClientRequestFormat(MetadataStore metadata) {
        this.metadata = metadata;
    }

    /* Abstract functions need to be overwritten by implementations */
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
    public abstract void updatePartitionEntries(int nodeId,
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
    public abstract int deletePartitionEntries(int nodeId,
                                               String storeName,
                                               List<Integer> partitionList,
                                               VoldemortFilter filter);

    /**
     * update remote metadata on a particular node
     * 
     * @param remoteNodeId
     * @param key
     * @param value
     */
    protected abstract void doUpdateRemoteMetadata(int remoteNodeId, ByteArray key, Versioned<byte[]> value);
    

    /**
     * get remote metadata on a particular node
     * 
     * @param remoteNodeId
     * @param key
     * @return
     */
    protected abstract Versioned<byte[]> doGetRemoteMetadata(int remoteNodeId, ByteArray key);

    /* Helper functions */

    public void updateRemoteMetadata(int remoteNodeId, String key, Versioned<String> value){
      ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
      Versioned<byte[]> valueBytes = new Versioned<byte[]> (ByteUtils.getBytes(value.getValue(), "UTF-8"), value.getVersion());
      
      doUpdateRemoteMetadata(remoteNodeId, keyBytes, valueBytes);
    }
    
    public Versioned<String> getRemoteMetadata(int remoteNodeId, String key){
        ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
        Versioned<byte[]> value = doGetRemoteMetadata(remoteNodeId, keyBytes);
        return new Versioned<String>(ByteUtils.getString(value.getValue(), "UTF-8"), value.getVersion());
    }
    
    public Node getConnectedNode() {
        return metadata.getCluster().getNodeById(metadata.getNodeId());
    }

    public MetadataStore getMetadata() {
        return metadata;
    }

    public void updateClusterMetadata(int nodeId, Cluster cluster) throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getClusterMetadata(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.CLUSTER_KEY,
                             new Versioned<String>(clusterMapper.writeCluster(cluster),
                                                   oldClock.incremented(nodeId, 1)));
    }

    public Versioned<Cluster> getClusterMetadata(int nodeId) throws VoldemortException {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY);
        Cluster cluster = clusterMapper.readCluster(new StringReader(value.getValue()));
        return new Versioned<Cluster>(cluster, value.getVersion());

    }

    /* get/update Store metadata */
    public void updateStoresMetadata(int nodeId, List<StoreDefinition> storesList)
            throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getStoresMetadata(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.STORES_KEY,
                             new Versioned<String>(storeMapper.writeStoreList(storesList),
                                                   oldClock.incremented(nodeId, 1)));
    }

    public Versioned<List<StoreDefinition>> getStoresMetadata(int nodeId) throws VoldemortException {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.STORES_KEY);
        List<StoreDefinition> storeList = storeMapper.readStoreList(new StringReader(value.getValue()));
        return new Versioned<List<StoreDefinition>>(storeList, value.getVersion());
    }

    /* get/update Server state metadata */
    public void updateServerState(int nodeId, MetadataStore.VoldemortState state) {
        VectorClock oldClock = (VectorClock) getServerState(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.SERVER_STATE_KEY,
                             new Versioned<String>(state.toString(),
                                                   oldClock.incremented(nodeId, 1)));
    }

    public Versioned<VoldemortState> getServerState(int nodeId) {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.SERVER_STATE_KEY);
        return new Versioned<VoldemortState>(VoldemortState.valueOf(value.getValue()),
                                             value.getVersion());
    }

    /* get/update Proxy Destination Node while rebalancing */
    public void updateRebalancingProxyDest(int rebalancingNodeId, int proxyDestNodeId) {
        VectorClock oldClock = (VectorClock) getRebalancingProxyDest(rebalancingNodeId).getVersion();

        updateRemoteMetadata(rebalancingNodeId,
                             MetadataStore.REBALANCING_SLAVES_LIST_KEY,
                             new Versioned<String>("" + proxyDestNodeId,
                                                   oldClock.incremented(rebalancingNodeId, 1)));
    }

    public Versioned<Integer> getRebalancingProxyDest(int rebalancingNodeId) {
        Versioned<String> value = getRemoteMetadata(rebalancingNodeId,
                                                    MetadataStore.REBALANCING_SLAVES_LIST_KEY);
        return new Versioned<Integer>(Integer.parseInt(value.getValue()), value.getVersion());
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

        updateRemoteMetadata(rebalancingNodeId,
                             MetadataStore.REBALANCING_PARTITIONS_LIST_KEY,
                             new Versioned<String>(partitionListString.toString(),
                                                   oldClock.incremented(rebalancingNodeId, 1)));
    }

    public Versioned<List<Integer>> getRebalancingPartitionList(int rebalancingNodeId) {
        Versioned<String> value = getRemoteMetadata(rebalancingNodeId,
                                                      MetadataStore.REBALANCING_PARTITIONS_LIST_KEY);
        String[] partitionList = value.getValue().split(",");
        List<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i < partitionList.length; i++) {
            list.add(Integer.parseInt(partitionList[i]));
        }

        return new Versioned<List<Integer>>(list, value.getVersion());
    }

    /**
     * Pipe fetch from donorNode and update stealerNode in streaming mode.
     */
    public void fetchAndUpdateStreams(int donorNodeId,
                                      int stealerNodeId,
                                      String storeName,
                                      List<Integer> stealList,
                                      VoldemortFilter filter) {
        updatePartitionEntries(stealerNodeId, storeName, fetchPartitionEntries(donorNodeId,
                                                                               storeName,
                                                                               stealList,
                                                                               filter), null);
    }
}
