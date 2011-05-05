package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Sets;

/**
 * Compares the current cluster configuration with the target cluster
 * configuration. <br>
 * The end result is a map of target node-ids to another map of source node-ids
 * and partitions desired to be stolen or fetched
 * 
 */
public class RebalanceClusterPlan {

    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    /**
     * For the "current" cluster, creates a map of node-ids to corresponding
     * list of [replica,partition] tuples (primary & replicas)
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> currentNodeIdToAllPartitionTuples;

    /**
     * For the "target" cluster, creates a map of node-ids to corresponding list
     * of [replica,partition] tuples (primary & replicas)
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> targetNodeIdToAllPartitionTuples;

    /**
     * A map of all the stealer nodes to their corresponding stolen [replica,
     * partition] tuples
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> stealerNodeIdToStolenPartitionTuples;

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

        // Create node id to all tuples mapping
        this.currentNodeIdToAllPartitionTuples = Collections.unmodifiableMap(RebalanceUtils.getNodeIdToAllPartitions(currentCluster,
                                                                                                                     storeDefs,
                                                                                                                     true));
        this.targetNodeIdToAllPartitionTuples = Collections.unmodifiableMap(RebalanceUtils.getNodeIdToAllPartitions(targetCluster,
                                                                                                                    storeDefs,
                                                                                                                    true));

        // Create stealer node to stolen tuples mapping
        this.stealerNodeIdToStolenPartitionTuples = Collections.unmodifiableMap(RebalanceUtils.getStolenPartitionTuples(currentCluster,
                                                                                                                        targetCluster,
                                                                                                                        storeDefs));

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
     */
    private List<RebalancePartitionsInfo> getRebalancePartitionsInfo(final Cluster currentCluster,
                                                                     final Cluster targetCluster,
                                                                     final List<String> storeNames,
                                                                     final int stealerId,
                                                                     final boolean enableDeletePartition) {

        final List<RebalancePartitionsInfo> result = new ArrayList<RebalancePartitionsInfo>();

        // If null, done with this stealer node
        if(stealerNodeIdToStolenPartitionTuples.get(stealerId) == null) {
            return result;
        }

        final Set<Pair<Integer, Integer>> haveToStealTuples = Sets.newHashSet(stealerNodeIdToStolenPartitionTuples.get(stealerId));

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
            final Set<Pair<Integer, Integer>> trackDeletePartitionsTuples = new HashSet<Pair<Integer, Integer>>();

            // Checks if this donor node can donate any tuples
            donatePartitionTuple(donorNode, haveToStealTuples, trackStealPartitionsTuples);

            // Check if we can delete the partitions this donor just donated
            donateDeletePartitionTuple(donorNode,
                                       trackStealPartitionsTuples,
                                       trackDeletePartitionsTuples,
                                       enableDeletePartition);

            if(trackStealPartitionsTuples.size() > 0 || trackDeletePartitionsTuples.size() > 0) {

                result.add(new RebalancePartitionsInfo(stealerId,
                                                       donorNode.getId(),
                                                       RebalanceUtils.flattenPartitionTuples(trackStealPartitionsTuples),
                                                       RebalanceUtils.flattenPartitionTuples(trackDeletePartitionsTuples),
                                                       storeNames,
                                                       currentCluster,
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
     * @param haveToStealTuples The partition tuples which we ideally want to
     *        steal
     * @param trackStealPartitionsTuples Set of partitions tuples already stolen
     */
    private void donatePartitionTuple(final Node donorNode,
                                      Set<Pair<Integer, Integer>> haveToStealTuples,
                                      Set<Pair<Integer, Integer>> trackStealPartitionsTuples) {
        final Set<Pair<Integer, Integer>> donorPartitionTuples = currentNodeIdToAllPartitionTuples.get(donorNode.getId());
        final Iterator<Pair<Integer, Integer>> iter = haveToStealTuples.iterator();

        // Iterate over the partition tuples to steal and check if this node can
        // donate it
        while(iter.hasNext()) {
            Pair<Integer, Integer> partitionTupleToSteal = iter.next();
            if(donorPartitionTuples.contains(partitionTupleToSteal)) {
                trackStealPartitionsTuples.add(partitionTupleToSteal);

                // This partition has been donated, remove it
                iter.remove();
            }
        }
    }

    /**
     * We do not delete if this donor node is also the stealer node for another
     * plan and is stealing data for the same partition ( irrespective of the
     * replica type ). This is a problem only for BDB storage engine but has
     * been placed at this level since for RO stores we don't delete the data
     * anyways.
     * 
     * @param donor Donor node
     * @param trackStealPartitionsTuples Partitions being stolen
     * @param trackDeletePartitionsTuples Partitions going to be deleted
     * @param enableDeletePartition Are we allowed to delete?
     */
    private void donateDeletePartitionTuple(final Node donor,
                                            Set<Pair<Integer, Integer>> trackStealPartitionsTuples,
                                            Set<Pair<Integer, Integer>> trackDeletePartitionsTuples,
                                            boolean enableDeletePartition) {

        // Are we allowed to delete AND do we have anything to check for
        // deletion?
        if(enableDeletePartition && trackStealPartitionsTuples.size() > 0) {

            // Retrieve information about what this donor node will steal
            List<Integer> partitionsStolenByDonor = RebalanceUtils.getPartitionsFromTuples(stealerNodeIdToStolenPartitionTuples.get(donor.getId()));

            // Check if this stolen partitions overlap with those being donated
            for(Pair<Integer, Integer> tuple: trackStealPartitionsTuples) {
                if(!partitionsStolenByDonor.contains(tuple.getSecond())) {
                    trackDeletePartitionsTuples.add(tuple);
                }
            }
        }
    }

    private boolean haveFinishedPartitions(Set<Pair<Integer, Integer>> set) {
        return (set == null || set.size() == 0);
    }

    @Override
    public String toString() {
        if(rebalanceTaskQueue.isEmpty()) {
            return "No rebalancing required since rebalance task is empty";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Cluster Rebalancing Plan : ").append(Utils.NEWLINE);
        builder.append(toString(getRebalancingTaskQueue()));
        return builder.toString();
    }

    public String toString(Queue<RebalanceNodePlan> queue) {
        if(queue == null || queue.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder(Utils.NEWLINE);
        for(RebalanceNodePlan nodePlan: queue) {
            builder.append("StealerNode : " + nodePlan.getStealerNode()).append(Utils.NEWLINE);
            for(RebalancePartitionsInfo rebalancePartitionsInfo: nodePlan.getRebalanceTaskList()) {
                builder.append("\t RebalancePartitionsInfo : " + rebalancePartitionsInfo)
                       .append(Utils.NEWLINE);
            }
        }

        return builder.toString();
    }

    public String printPartitionDistribution() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current Cluster: ")
          .append(Utils.NEWLINE)
          .append(RebalanceUtils.printMap(currentNodeIdToAllPartitionTuples, currentCluster))
          .append(Utils.NEWLINE);
        sb.append("Target Cluster: ")
          .append(Utils.NEWLINE)
          .append(RebalanceUtils.printMap(targetNodeIdToAllPartitionTuples, targetCluster));
        return sb.toString();
    }

}