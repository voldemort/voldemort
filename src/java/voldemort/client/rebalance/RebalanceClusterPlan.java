package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import voldemort.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Compares the current cluster configuration with the target cluster
 * configuration. <br>
 * The end result is a map of target node-ids to another map of source node-ids
 * and partitions desired to be stolen or fetched
 * 
 */
public class RebalanceClusterPlan {

    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;
    private final List<StoreDefinition> storeDefs;

    /**
     * For the "current" cluster, creates a map of node-ids to corresponding
     * list of <replica,partition> tuples (primary & replicas)
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions;

    /**
     * For the "target" cluster, creates a map of node-ids to corresponding list
     * of <replica,partition> tuples (primary & replicas)
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> targetNodeIdToAllPartitions;

    private final Cluster currentCluster;
    private final Cluster targetCluster;

    /**
     * Compares the currentCluster configuration with the desired
     * targetConfiguration and builds a map of Target node-id to map of source
     * node-ids and partitions desired to be stolen/fetched.
     * 
     * @param currentCluster The current cluster definition
     * @param targetCluster The target cluster definition
     * @param storeDefList The list of store definitions to rebalance
     * @param enabledDeletePartition Delete the RW partition on the donor side
     *        after rebalance
     */
    public RebalanceClusterPlan(final Cluster currentCluster,
                                final Cluster targetCluster,
                                final List<StoreDefinition> storeDefs,
                                final boolean enabledDeletePartition) {
        this.currentCluster = currentCluster;
        this.targetCluster = targetCluster;

        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        this.storeDefs = Collections.unmodifiableList(storeDefs);

        // Number of partitions to remain same
        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should be equal [ Current cluster ("
                                         + currentCluster.getNumberOfPartitions()
                                         + ") not equal to Target cluster ("
                                         + targetCluster.getNumberOfPartitions() + ") ]");

        // Similarly number of nodes to remain same
        if(currentCluster.getNumberOfNodes() != targetCluster.getNumberOfNodes())
            throw new VoldemortException("Total number of nodes should be equal [ Current cluster ("
                                         + currentCluster.getNumberOfNodes()
                                         + ") not equal to Target cluster ("
                                         + targetCluster.getNumberOfNodes() + ") ]");

        // Create node id to all partitions mapping
        this.nodeIdToAllPartitions = Collections.unmodifiableMap(RebalanceUtils.getNodeIdToAllPartitions(currentCluster,
                                                                                                         storeDefs,
                                                                                                         true));
        this.targetNodeIdToAllPartitions = Collections.unmodifiableMap(RebalanceUtils.getNodeIdToAllPartitions(targetCluster,
                                                                                                               storeDefs,
                                                                                                               true));

        for(Node node: targetCluster.getNodes()) {
            List<RebalancePartitionsInfo> rebalanceNodeList = getRebalancePartitionsInfo(currentCluster,
                                                                                         targetCluster,
                                                                                         RebalanceUtils.getStoreNames(storeDefs),
                                                                                         node.getId(),
                                                                                         enabledDeletePartition);

            if(rebalanceNodeList.size() > 0) {
                rebalanceTaskQueue.offer(new RebalanceNodePlan(node.getId(), rebalanceNodeList));
            }
        }
    }

    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        return rebalanceTaskQueue;
    }

    /**
     * Generate the list of partition movement based on 2 principles:
     * 
     * <ol>
     * <li>The number of partitions don't change; they are only redistributed
     * across nodes
     * <li>A primary or replica partition that is going to be deleted is never
     * used to copy from data from another stealer
     * </ol>
     * 
     * @param currentCluster Current cluster configuration
     * @param targetCluster Target cluster configuration
     * @param storeNames List of store names
     * @param stealerId Id of the stealer node
     * @param enableDeletePartition To delete or not to delete?
     * 
     */
    private List<RebalancePartitionsInfo> getRebalancePartitionsInfo(final Cluster currentCluster,
                                                                     final Cluster targetCluster,
                                                                     final List<String> storeNames,
                                                                     final int stealerId,
                                                                     final boolean enableDeletePartition) {

        final List<RebalancePartitionsInfo> result = new ArrayList<RebalancePartitionsInfo>();

        // Separate the partitions being copied from those being deleted
        final Set<Pair<Integer, Integer>> haveToStealTuples = RebalanceUtils.getStolenPartitionTuples(currentCluster,
                                                                                                      targetCluster,
                                                                                                      storeDefs,
                                                                                                      stealerId);

        // If empty, done with this stealer node
        if(haveToStealTuples.size() == 0) {
            return result;
        }

        // Now we find out which donor can donate partitions to this stealer
        for(Node donorNode: currentCluster.getNodes()) {

            // The same node can't donate
            if(donorNode.getId() == stealerId)
                continue;

            // Finished treating all partitions?
            if(haveFinishedPartitions(haveToStealTuples)) {
                break;
            }

            final Set<Pair<Integer, Integer>> trackStealPartitionsTuples = new HashSet<Pair<Integer, Integer>>();

            // Checks if this donor can donate any tuples
            donatePartitionTuple(donorNode, haveToStealTuples, trackStealPartitionsTuples);

            if(trackStealPartitionsTuples.size() > 0) {

                result.add(new RebalancePartitionsInfo(stealerId,
                                                       donorNode.getId(),
                                                       flatten(trackStealPartitionsTuples),
                                                       storeNames,
                                                       currentCluster,
                                                       enableDeletePartition,
                                                       0));
            }
        }

        return result;
    }

    /**
     * Given a donor node and a set of tuples that need to be stolen, checks if
     * the donor can contribute any
     * 
     * @param donorNode Donor node we are checking
     * @param haveToStealPartitions The partition tuples which we ideally want
     *        to steal
     * @param trackStealPartitions Set of partitions tuples already stolen
     */
    private void donatePartitionTuple(final Node donorNode,
                                      Set<Pair<Integer, Integer>> haveToStealPartitions,
                                      Set<Pair<Integer, Integer>> trackStealPartitions) {
        final Set<Pair<Integer, Integer>> donorPrimaryPartitionIds = nodeIdToAllPartitions.get(donorNode.getId());
        final Iterator<Pair<Integer, Integer>> iter = haveToStealPartitions.iterator();
        while(iter.hasNext()) {
            Pair<Integer, Integer> partitionTupleToSteal = iter.next();
            if(donorPrimaryPartitionIds.contains(partitionTupleToSteal)) {
                trackStealPartitions.add(partitionTupleToSteal);
                // This partition has been donated, let's remove it
                iter.remove();
            }
        }
    }

    private boolean haveFinishedPartitions(Set<Pair<Integer, Integer>> set) {
        return (set == null || set.size() == 0);
    }

    /**
     * Returns a string representation of the cluster
     * 
     * <pre>
     * Current Cluster:
     * 0 - [0, 1, 2, 3] + [7, 8, 9]
     * 1 - [4, 5, 6] + [0, 1, 2, 3]
     * 2 - [7, 8, 9] + [4, 5, 6]
     * </pre>
     * 
     * @param nodeIdToAllPartitions Mapping of node id to all tuples
     * @param cluster The cluster metadata
     * @return Returns a string representation of the cluster
     */
    private String printMap(final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions,
                            final Cluster cluster) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Integer, Set<Pair<Integer, Integer>>> entry: nodeIdToAllPartitions.entrySet()) {
            final Integer nodeId = entry.getKey();
            final Set<Pair<Integer, Integer>> allPartitions = entry.getValue();

            final HashMap<Integer, List<Integer>> replicaTypeToPartitions = flatten(allPartitions);

            sb.append(nodeId);
            if(replicaTypeToPartitions.size() > 0) {
                for(Entry<Integer, List<Integer>> partitions: replicaTypeToPartitions.entrySet()) {
                    sb.append(" - " + partitions.getValue());
                }
            } else {
                sb.append(" - empty");
            }
            sb.append(Utils.NEWLINE);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        if(rebalanceTaskQueue.isEmpty()) {
            return "Rebalance task queue is empty, No rebalancing needed";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Cluster Rebalancing Plan:").append(Utils.NEWLINE);
        builder.append(toString(getRebalancingTaskQueue()));
        return builder.toString();
    }

    public String toString(Queue<RebalanceNodePlan> queue) {
        if(queue == null || queue.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder(Utils.NEWLINE);
        for(RebalanceNodePlan nodePlan: queue) {
            builder.append("StealerNode:" + nodePlan.getStealerNode()).append(Utils.NEWLINE);
            for(RebalancePartitionsInfo rebalancePartitionsInfo: nodePlan.getRebalanceTaskList()) {
                builder.append("\t RebalancePartitionsInfo: " + rebalancePartitionsInfo)
                       .append(Utils.NEWLINE);
            }
        }

        return builder.toString();
    }

    /**
     * Given a list of tuples of <replica_type, partition>, flattens it and
     * generates a map of replica_type to partition mapping
     * 
     * @param partitionTuples List of <replica_type, partition> tuples
     * @return Map of replica_type to set of partitions
     */
    private HashMap<Integer, List<Integer>> flatten(Set<Pair<Integer, Integer>> partitionTuples) {
        HashMap<Integer, List<Integer>> flattenedTuples = Maps.newHashMap();
        for(Pair<Integer, Integer> pair: partitionTuples) {
            if(flattenedTuples.containsKey(pair.getFirst())) {
                flattenedTuples.get(pair.getFirst()).add(pair.getSecond());
            } else {
                List<Integer> newPartitions = Lists.newArrayList();
                newPartitions.add(pair.getSecond());
                flattenedTuples.put(pair.getFirst(), newPartitions);
            }
        }
        return flattenedTuples;
    }

    public String printPartitionDistribution() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current Cluster: ")
          .append(Utils.NEWLINE)
          .append(printMap(nodeIdToAllPartitions, currentCluster))
          .append(Utils.NEWLINE);
        sb.append("Target Cluster: ")
          .append(Utils.NEWLINE)
          .append(printMap(targetNodeIdToAllPartitions, targetCluster));
        return sb.toString();
    }

}