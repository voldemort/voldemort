package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.RebalanceUtils;

/**
 * Compares the currentCluster configuration with the desired
 * targetConfiguration and returns a map of Target node-id to map of source
 * node-ids and partitions desired to be stolen/fetched.<br>
 * <b> returned Queue is threadsafe </b>
 * 
 * @param currentCluster
 * @param targetCluster
 */
public class RebalanceClusterPlan {

    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    public RebalanceClusterPlan(Cluster currentCluster,
                                Cluster targetCluster,
                                List<String> storeList,
                                boolean deleteDonorPartition) {
        rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        for(Node node: targetCluster.getNodes()) {
            List<RebalancePartitionsInfo> rebalanceNodeList = getRebalanceNodeTask(currentCluster,
                                                                                   targetCluster,
                                                                                   storeList,
                                                                                   node.getId(),
                                                                                   deleteDonorPartition);
            if(rebalanceNodeList.size() > 0) {
                rebalanceTaskQueue.offer(new RebalanceNodePlan(node.getId(), rebalanceNodeList));
            }
        }
    }

    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        return rebalanceTaskQueue;
    }

    private List<RebalancePartitionsInfo> getRebalanceNodeTask(Cluster currentCluster,
                                                               Cluster targetCluster,
                                                               List<String> storeList,
                                                               int stealNodeId,
                                                               boolean deleteDonorPartition) {
        Map<Integer, List<Integer>> stealPartitionsMap = new HashMap<Integer, List<Integer>>();
        Map<Integer, Integer> currentPartitionsToNodeMap = getCurrentPartitionMapping(currentCluster);
        List<Integer> targetList = targetCluster.getNodeById(stealNodeId).getPartitionIds();
        List<Integer> currentList;

        if(RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();
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

        List<RebalancePartitionsInfo> stealInfoList = new ArrayList<RebalancePartitionsInfo>();
        for(Entry<Integer, List<Integer>> stealEntry: stealPartitionsMap.entrySet()) {
            stealInfoList.add(new RebalancePartitionsInfo(stealNodeId,
                                                          stealEntry.getKey(),
                                                          stealEntry.getValue(),
                                                          storeList,
                                                          deleteDonorPartition,
                                                          0));
        }

        return stealInfoList;
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

    @Override
    public String toString() {

        if(rebalanceTaskQueue.isEmpty()) {
            return "Cluster is already balanced, No rebalancing needed";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Cluster Rebalancing Plan:\n");
        for(RebalanceNodePlan nodePlan: rebalanceTaskQueue) {
            builder.append("StealerNode:" + nodePlan.getStealerNode() + "\n");
            for(RebalancePartitionsInfo stealInfo: nodePlan.getRebalanceTaskList()) {
                builder.append("\t" + stealInfo + "\n");
            }
        }

        return builder.toString();
    }
}