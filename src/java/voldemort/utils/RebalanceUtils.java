package voldemort.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.protocol.admin.AsyncOperationRunner;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.versioning.VectorClock;

public class RebalanceUtils {

    private static Logger logger = Logger.getLogger(RebalanceUtils.class);

    /**
     * Compares the currentCluster configuration with the desired
     * targetConfiguration and returns a map of Target node-id to map of source
     * node-ids and partitions desired to be stolen/fetched.
     * 
     * @param currentCluster
     * @param targetCluster
     * @return Map : {NodeId(target) --> Map {NodeId(source), List(partitions)}}
     */
    public static Map<Integer, List<RebalanceStealInfo>> getStealPartitionsMap(String storeName,
                                                                               Cluster currentCluster,
                                                                               Cluster targetCluster) {
        Map<Integer, List<RebalanceStealInfo>> map = new HashMap<Integer, List<RebalanceStealInfo>>();

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        for(Node node: targetCluster.getNodes()) {
            List<RebalanceStealInfo> stealPartitionMap = getStealPartitionsMap(storeName,
                                                                               currentCluster,
                                                                               targetCluster,
                                                                               node.getId());
            if(!stealPartitionMap.isEmpty())
                map.put(node.getId(), stealPartitionMap);
        }
        return map;
    }

    public static String getStealPartitionsMapAsString(Map<Integer, List<RebalanceStealInfo>> stealMap) {
        StringBuilder builder = new StringBuilder("Rebalance Partitions transfer Map:\n");
        for(Entry<Integer, List<RebalanceStealInfo>> entry: stealMap.entrySet()) {
            builder.append("Node-" + entry.getKey() + " <== {");
            for(RebalanceStealInfo stealInfo: entry.getValue()) {
                builder.append("(Node-");
                builder.append(stealInfo.getDonorId() + " ");
                builder.append("partitions:" + stealInfo.getPartitionList());
                builder.append(")");
            }
            builder.append("}\n");
        }

        return builder.toString();
    }

    private static List<RebalanceStealInfo> getStealPartitionsMap(String storeName,
                                                                  Cluster currentCluster,
                                                                  Cluster targetCluster,
                                                                  int id) {
        Map<Integer, List<Integer>> stealPartitionsMap = new HashMap<Integer, List<Integer>>();
        Map<Integer, Integer> currentPartitionsToNodeMap = getCurrentPartitionMapping(currentCluster);
        List<Integer> targetList = targetCluster.getNodeById(id).getPartitionIds();
        List<Integer> currentList;

        if(containsNode(currentCluster, id))
            currentList = currentCluster.getNodeById(id).getPartitionIds();
        else
            currentList = new ArrayList<Integer>();

        for(int p: targetList) {
            if(!currentList.contains(p)) {
                // new extra partition
                int currentMasterNode = currentPartitionsToNodeMap.get(p);
                // create array if needed
                if(!stealPartitionsMap.containsKey(currentMasterNode)) {
                    stealPartitionsMap.put(currentMasterNode, new ArrayList<Integer>());
                }

                // add partition to list.
                stealPartitionsMap.get(currentMasterNode).add(p);
            }
        }

        List<RebalanceStealInfo> stealInfoList = new ArrayList<RebalanceStealInfo>();
        for(Entry<Integer, List<Integer>> stealEntry: stealPartitionsMap.entrySet()) {
            stealInfoList.add(new RebalanceStealInfo(storeName,
                                                     stealEntry.getKey(),
                                                     stealEntry.getValue(),
                                                     0));
        }
        return stealInfoList;
    }

    public static boolean containsNode(Cluster cluster, int nodeId) {
        try {
            cluster.getNodeById(nodeId);
            return true;
        } catch(VoldemortException e) {
            return false;
        }
    }

    private static Map<Integer, Integer> getCurrentPartitionMapping(Cluster currentCluster) {
        Map<Integer, Integer> partitionToNode = new HashMap<Integer, Integer>();

        for(Node n: currentCluster.getNodes()) {
            for(Integer partition: n.getPartitionIds()) {
                partitionToNode.put(partition, n.getId());
            }
        }

        return partitionToNode;
    }

    /**
     * We should only allow one cluster rebalancing at one time. We need to
     * implement kind of a global lock for that.
     * 
     * TODO: Currently the user is responsible for not starting more than on
     * cluster rebalancing.
     * 
     * @return
     */
    public static boolean getClusterRebalancingToken() {
        return true;
    }

    /**
     * Update the cluster with desired changes as marked in rebalanceNodeInfo
     * rebalanceNodeInfo.getFirst() is the stealerNode (destinationNode) <br>
     * rebalanceNodeInfo.getSecond() is the rebalance steal info contatining
     * donorId, partitionList<br>
     * Creates a new cluster Object with above partition list changes.<br>
     * Propagates the new cluster on all nodes
     * 
     * @param adminClient
     * @param rebalanceNodeInfo
     * @return
     */
    public static Cluster updateAndPropagateCluster(AdminClient adminClient,
                                                    Node stealerNode,
                                                    RebalanceStealInfo rebalanceNodeInfo) {
        synchronized(adminClient) {

            Cluster currentCluster = adminClient.getCluster();

            // add new partitions to stealerNode
            List<Integer> stealerPartitionList = new ArrayList<Integer>(stealerNode.getPartitionIds());
            stealerPartitionList.addAll(rebalanceNodeInfo.getPartitionList());
            stealerNode = updateNode(stealerNode, stealerPartitionList);

            // remove partitions from donorNode
            List<Integer> donorPartitionList = new ArrayList<Integer>(adminClient.getCluster()
                                                                                 .getNodeById(rebalanceNodeInfo.getDonorId())
                                                                                 .getPartitionIds());
            stealerPartitionList.removeAll(rebalanceNodeInfo.getPartitionList());
            Node donorNode = updateNode(adminClient.getCluster()
                                                   .getNodeById(rebalanceNodeInfo.getDonorId()),
                                        donorPartitionList);

            currentCluster = updateCluster(currentCluster, Arrays.asList(stealerNode, donorNode));

            // get VectorClock from donorNode
            VectorClock clock = (VectorClock) adminClient.getRemoteCluster(donorNode.getId())
                                                         .getVersion();
            // increment version mastered at stealerNode.
            clock = clock.incremented(stealerNode.getId(), System.currentTimeMillis());

            propagateCluster(adminClient, currentCluster, clock, Arrays.asList(stealerNode.getId(),
                                                                               donorNode.getId()));
            return currentCluster;
        }
    }

    public static Cluster updateCluster(Cluster currentCluster, List<Node> updatedNodeList) {
        List<Node> currentNodeList = new ArrayList<Node>(currentCluster.getNodes());
        for(Node updatedNode: updatedNodeList) {
            for(Node currentNode: currentNodeList)
                if(currentNode.getId() == updatedNode.getId())
                    currentNodeList.remove(currentNode);
        }

        currentNodeList.addAll(updatedNodeList);
        return new Cluster(currentCluster.getName(), currentNodeList);
    }

    public static Node updateNode(Node node, List<Integer> partitionsList) {
        return new Node(node.getId(),
                        node.getHost(),
                        node.getHttpPort(),
                        node.getSocketPort(),
                        node.getAdminPort(),
                        partitionsList,
                        node.getStatus());
    }

    /**
     * propagate the cluster configuration to all nodes.<br>
     * throws an exception if failed to propagate on any of the required nodes.
     * 
     * @param adminClient
     * @param masterNodeId
     * @param currentCluster
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster currentCluster,
                                        VectorClock clock,
                                        List<Integer> requiredNodeIds) {
        for(Node node: currentCluster.getNodes()) {
            try {
                adminClient.updateRemoteCluster(node.getId(), currentCluster, clock);
            } catch(VoldemortException e) {
                if(requiredNodeIds.contains(node.getId())) {
                    throw new VoldemortException("Failed to copy new cluster.xml(" + currentCluster
                                                 + ") on required node:" + node, e);
                } else {
                    logger.warn("Failed to copy new cluster.xml(" + currentCluster
                                + ") on non-required node:" + node, e);
                }
            }
        }
    }

    /**
     * Remove and return one stealInfo from the list of rebalancingStealInfos
     * for the passed stealerNodeId in the stealPartitionsMap.<br>
     * The stealerPartitionsMap is updated with the new list<br>
     * The stealerNodeId empty is deleted from the map entirely if the resultant
     * list becomes empty
     * 
     * @param stealerNodeId : nodeId
     * @param stealPartitionsMap
     * @return
     */
    public static RebalanceStealInfo getOneStealInfoAndUpdateStealMap(int stealerNodeId,
                                                                      Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
        synchronized(stealPartitionsMap) {
            List<RebalanceStealInfo> stealInfoList = stealPartitionsMap.get(stealerNodeId);
            int stealInfoIndex = stealInfoList.size() - 1;
            RebalanceStealInfo stealInfo = stealInfoList.get(stealInfoIndex);
            stealInfoList.remove(stealInfoIndex);

            if(stealInfoList.isEmpty()) {
                stealPartitionsMap.remove(stealerNodeId);
            }

            return stealInfo;
        }
    }

    /**
     * Returns one key randomly from the stealPartitionsMap. This is used to
     * start parallel rebalancing requests on different nodes.
     * 
     * @param stealPartitionsMap
     * @return
     */
    public static int getRandomStealerNodeId(Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
        int size = stealPartitionsMap.keySet().size();
        int randomIndex = (int) (Math.random() * size);
        return stealPartitionsMap.keySet().toArray(new Integer[0])[randomIndex];
    }

    /**
     * Rebalance logic at single node level.<br>
     * <imp> should be called by the rebalancing node itself</imp><br>
     * Attempt to rebalance from node {@link RebalanceStealInfo#getDonorId()}
     * for partitionList {@link RebalanceStealInfo#getPartitionList()}
     * <p>
     * Force Sets serverState to rebalancing, Sets stealInfo in MetadataStore,
     * fetch keys from remote node and upsert them locally.<br>
     * On success clean all states it changed
     * 
     * @param metadataStore
     * @param stealInfo
     * @return taskId for asynchronous task.
     */
    public static int rebalanceLocalNode(final MetadataStore metadataStore,
                                         final RebalanceStealInfo stealInfo,
                                         final AsyncOperationRunner asyncRunner,
                                         final AdminClient adminClient) {
        int requestId = asyncRunner.getUniqueRequestId();
        asyncRunner.submitOperation(requestId,
                                    new AsyncOperation(requestId, "rebalanceNode:"
                                                                  + stealInfo.toString()) {

                                        private int fetchAndUpdateAsyncId = -1;

                                        @Override
                                        public void operate() throws Exception {
                                            synchronized(metadataStore) {
                                                checkCurrentState(metadataStore, stealInfo);
                                                setRebalancingState(metadataStore, stealInfo);
                                            }
                                            fetchAndUpdateAsyncId = startAsyncPartitionFetch(metadataStore,
                                                                                             stealInfo);
                                            adminClient.waitForCompletion(metadataStore.getNodeId(),
                                                                          fetchAndUpdateAsyncId,
                                                                          24 * 60 * 60,
                                                                          TimeUnit.SECONDS);

                                            metadataStore.cleanAllRebalancingState();
                                        }

                                        @Override
                                        @JmxGetter(name = "asyncTaskStatus")
                                        public AsyncOperationStatus getStatus() {
                                            return adminClient.getAsyncRequestStatus(metadataStore.getNodeId(),
                                                                                     fetchAndUpdateAsyncId);
                                        }

                                        private int startAsyncPartitionFetch(MetadataStore metadataStore,
                                                                             RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            return adminClient.fetchAndUpdateStreams(metadataStore.getNodeId(),
                                                                                     stealInfo.getDonorId(),
                                                                                     stealInfo.getStoreName(),
                                                                                     stealInfo.getPartitionList(),
                                                                                     null);
                                        }

                                        private void setRebalancingState(MetadataStore metadataStore,
                                                                         RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            metadataStore.put(MetadataStore.SERVER_STATE_KEY,
                                                              VoldemortState.REBALANCING_MASTER_SERVER);
                                            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO,
                                                              stealInfo);
                                        }

                                        private void checkCurrentState(MetadataStore metadataStore,
                                                                       RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            if(metadataStore.getServerState()
                                                            .equals(VoldemortState.REBALANCING_MASTER_SERVER)
                                               && metadataStore.getRebalancingStealInfo()
                                                               .getDonorId() != stealInfo.getDonorId())
                                                throw new VoldemortException("Server "
                                                                             + metadataStore.getNodeId()
                                                                             + " is already rebalancing from:"
                                                                             + metadataStore.getRebalancingStealInfo()
                                                                             + " rejecting rebalance request:"
                                                                             + stealInfo);
                                        }

                                    });

        return requestId;
    }

    public static void revertStealPartitionsMap(Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap,
                                                int stealerNodeId,
                                                RebalanceStealInfo rebalanceStealInfo) {
        synchronized(rebalanceStealInfo) {
            List<RebalanceStealInfo> stealList = (stealPartitionsMap.containsKey(stealerNodeId)) ? stealPartitionsMap.get(stealerNodeId)
                                                                                                : new ArrayList<RebalanceStealInfo>();
            stealList.add(rebalanceStealInfo);
            stealPartitionsMap.put(stealerNodeId, stealList);
        }
    }

    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig, Cluster cluster) {
        ClientConfig config = new ClientConfig();
        config.setMaxConnectionsPerNode(1);
        config.setMaxThreads(1);
        config.setConnectionTimeout(voldemortConfig.getAdminConnectionTimeout(),
                                    TimeUnit.MILLISECONDS);
        config.setSocketTimeout(voldemortConfig.getAdminSocketTimeout(), TimeUnit.MILLISECONDS);
        config.setSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new ProtoBuffAdminClientRequestFormat(cluster, config);
    }
}