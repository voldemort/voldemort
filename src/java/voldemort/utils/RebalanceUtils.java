package voldemort.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
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

            // create target vectorClock increment version on stealerNode.
            VectorClock clock = (VectorClock) adminClient.getRemoteCluster(stealerNode.getId())
                                                         .getVersion();
            propagateCluster(adminClient,
                             currentCluster,
                             clock.incremented(stealerNode.getId(), System.currentTimeMillis()));
            return currentCluster;
        }
    }

    private static Cluster updateCluster(Cluster currentCluster, List<Node> updatedNodeList) {
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
     * 
     * @param adminClient
     * @param masterNodeId
     * @param currentCluster
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster currentCluster,
                                        VectorClock clock) {
        AtomicInteger failures = new AtomicInteger(0);
        for(Node node: currentCluster.getNodes()) {
            try {
                adminClient.updateRemoteCluster(node.getId(), currentCluster, clock);
            } catch(VoldemortException e) {
                logger.warn("Failed to copy new cluster.xml(" + currentCluster
                            + ") to remote node:" + node, e);
                failures.incrementAndGet();
            }
        }

        if(failures.get() == currentCluster.getNumberOfNodes())
            throw new VoldemortException("Failed to propagate new cluster.xml(" + currentCluster
                                         + ") to any node");
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
    public static Pair<Integer, RebalanceStealInfo> getOneStealInfoAndUpdateStealMap(int stealerNodeId,
                                                                                     Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
        synchronized(stealPartitionsMap) {
            List<RebalanceStealInfo> stealInfoList = stealPartitionsMap.get(stealerNodeId);
            int stealInfoIndex = stealInfoList.size() - 1;
            RebalanceStealInfo stealInfo = stealInfoList.get(stealInfoIndex);
            stealInfoList.remove(stealInfoIndex);

            if(stealInfoList.isEmpty()) {
                stealPartitionsMap.remove(stealerNodeId);
            }

            return new Pair<Integer, RebalanceStealInfo>(stealerNodeId, stealInfo);
        }
    }

    public static int getRandomStealerNodeId(Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
        int size = stealPartitionsMap.keySet().size();
        int randomIndex = (int) (Math.random() * size);
        return stealPartitionsMap.keySet().toArray(new Integer[0])[randomIndex];
    }
}