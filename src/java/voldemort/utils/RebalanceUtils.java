package voldemort.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

public class RebalanceUtils {

    /**
     * Compares the currentCluster configuration with the desired
     * targetConfiguration and returns a map of Target node-id to map of source
     * node-ids and partitions desired to be stolen/fetched.
     * 
     * @param currentCluster
     * @param targetCluster
     * @return Map : {NodeId(target) --> Map {NodeId(source), List(partitions)}}
     */
    public static Map<Integer, Map<Integer, List<Integer>>> getStealPartitionsMap(Cluster currentCluster,
                                                                                  Cluster targetCluster) {
        Map<Integer, Map<Integer, List<Integer>>> map = new HashMap<Integer, Map<Integer, List<Integer>>>();

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        for(Node node: targetCluster.getNodes()) {
            Map<Integer, List<Integer>> stealPartitionMap = getStealPartitionsMap(currentCluster,
                                                                                  targetCluster,
                                                                                  node.getId());
            if(!stealPartitionMap.isEmpty())
                map.put(node.getId(), stealPartitionMap);
        }
        return map;
    }

    public static String getStealPartitionsMapAsString(Map<Integer, Map<Integer, List<Integer>>> stealMap) {
        StringBuilder builder = new StringBuilder("Rebalance Partitions transfer Map:\n");
        for(Entry<Integer, Map<Integer, List<Integer>>> entry: stealMap.entrySet()) {
            builder.append("Node-" + entry.getKey() + " <== {");
            for(Entry<Integer, List<Integer>> stealEntries: entry.getValue().entrySet()) {
                builder.append("(");
                builder.append("Node-" + stealEntries.getKey() + ",");
                builder.append(stealEntries.getValue());
                builder.append(")");
            }
            builder.append("}\n");
        }

        return builder.toString();
    }

    private static Map<Integer, List<Integer>> getStealPartitionsMap(Cluster currentCluster,
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

        return stealPartitionsMap;
    }

    private static boolean containsNode(Cluster cluster, int nodeId) {
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

}