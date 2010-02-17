package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.Multimap;

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
    private final List<StoreDefinition> storeDefList;

    public RebalanceClusterPlan(Cluster currentCluster,
                                Cluster targetCluster,
                                List<StoreDefinition> storeDefList,
                                boolean deleteDonorPartition) {
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        this.storeDefList = storeDefList;

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        for(Node node: targetCluster.getNodes()) {
            List<RebalancePartitionsInfo> rebalanceNodeList = getRebalanceNodeTask(currentCluster,
                                                                                   targetCluster,
                                                                                   RebalanceUtils.getStoreNames(storeDefList),
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
        Map<Integer, Integer> currentPartitionsToNodeMap = RebalanceUtils.getCurrentPartitionMapping(currentCluster);
        List<Integer> stealList = getStealList(currentCluster, targetCluster, stealNodeId);

        Map<Integer, List<Integer>> masterPartitionsMap = getStealMasterPartitions(stealList,
                                                                                   currentPartitionsToNodeMap);

        // copies partitions needed to satisfy new replication mapping.
        // these partitions should be copied but not deleted from original node.
        Map<Integer, List<Integer>> replicationPartitionsMap = getReplicationChanges(currentCluster,
                                                                                     targetCluster,
                                                                                     stealNodeId,
                                                                                     currentPartitionsToNodeMap);

        List<RebalancePartitionsInfo> stealInfoList = new ArrayList<RebalancePartitionsInfo>();
        for(Node donorNode: currentCluster.getNodes()) {
            Set<Integer> stealPartitions = new HashSet<Integer>();
            Set<Integer> deletePartitions = new HashSet<Integer>();
            Set<Integer> stealMasterPartitions = new HashSet<Integer>();// create Set for steal master partitions 
            
            if(masterPartitionsMap.containsKey(donorNode.getId())) {
                stealPartitions.addAll(masterPartitionsMap.get(donorNode.getId()));
                stealMasterPartitions.addAll(masterPartitionsMap.get(donorNode.getId()));// add one steal master partition
                if(deleteDonorPartition)
                    deletePartitions.addAll(masterPartitionsMap.get(donorNode.getId()));
            }

            if(replicationPartitionsMap.containsKey(donorNode.getId())) {
                stealPartitions.addAll(replicationPartitionsMap.get(donorNode.getId()));
            }

            if(stealPartitions.size() > 0) {
                stealInfoList.add(new RebalancePartitionsInfo(stealNodeId,
                                                              donorNode.getId(),
                                                              new ArrayList<Integer>(stealPartitions),
                                                              new ArrayList<Integer>(deletePartitions),
                                                              new ArrayList<Integer>(stealMasterPartitions),// create RebalancePartitionsInfo with stealMasterPartitions 
                                                              storeList,
                                                              0));
            }
        }

        return stealInfoList;
    }

    private List<Integer> getStealList(Cluster currentCluster,
                                       Cluster targetCluster,
                                       int stealNodeId) {
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return targetList;
    }

    private Map<Integer, List<Integer>> getReplicationChanges(Cluster currentCluster,
                                                              Cluster targetCluster,
                                                              int stealNodeId,
                                                              Map<Integer, Integer> currentPartitionsToNodeMap) {
        Map<Integer, List<Integer>> replicationMapping = new HashMap<Integer, List<Integer>>();
        List<Integer> targetList = targetCluster.getNodeById(stealNodeId).getPartitionIds();

        // get changing replication mapping
        RebalanceClusterTool clusterTool = new RebalanceClusterTool(currentCluster,
                                                                    RebalanceUtils.getMaxReplicationStore(this.storeDefList));
        Multimap<Integer, Pair<Integer, Integer>> replicationChanges = clusterTool.getRemappedReplicas(targetCluster);

        for(final Entry<Integer, Pair<Integer, Integer>> entry: replicationChanges.entries()) {
            int newReplicationPartition = entry.getValue().getSecond();
            if(targetList.contains(newReplicationPartition)) {
                // stealerNode need to replicate some new partition now.
                int donorNode = currentPartitionsToNodeMap.get(entry.getKey());
                // TODO LOW: not copying partitions on same node for now
                if(donorNode != stealNodeId)
                    createAndAdd(replicationMapping, donorNode, entry.getKey());
            }
        }

        return replicationMapping;
    }

    private Map<Integer, List<Integer>> getStealMasterPartitions(List<Integer> stealList,
                                                                 Map<Integer, Integer> currentPartitionsToNodeMap) {
        HashMap<Integer, List<Integer>> stealPartitionsMap = new HashMap<Integer, List<Integer>>();
        for(int p: stealList) {
            int donorNode = currentPartitionsToNodeMap.get(p);
            createAndAdd(stealPartitionsMap, donorNode, p);
        }

        return stealPartitionsMap;
    }

    private void createAndAdd(Map<Integer, List<Integer>> map, int key, int value) {
        // create array if needed
        if(!map.containsKey(key)) {
            map.put(key, new ArrayList<Integer>());
        }

        // add partition to list.
        map.get(key).add(value);
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