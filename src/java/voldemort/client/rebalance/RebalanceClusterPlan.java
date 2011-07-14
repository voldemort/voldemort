package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Compares the current cluster configuration with the target cluster
 * configuration and generates a plan to move the partitions. The plan can be
 * either generated from the perspective of the stealer or the donor. <br>
 * 
 * The end result is one of the following -
 * 
 * <li>A map of stealer node-ids to partitions desired to be stolen from various
 * donor nodes
 * 
 * <li>A map of donor node-ids to partitions which we need to donate to various
 * stealer nodes
 * 
 */
public class RebalanceClusterPlan {

    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    /**
     * For the purposes of printing we collect the partition tuples for all
     * nodes for all stores
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> currentAllStoresNodeIdToAllPartitionTuples;
    private final Map<Integer, Set<Pair<Integer, Integer>>> targetAllStoresNodeIdToAllPartitionTuples;

    /**
     * Compares the currentCluster configuration with the desired
     * targetConfiguration and builds a map of Target node-id to map of source
     * node-ids and partitions desired to be stolen/fetched.
     * 
     * @param currentCluster The current cluster definition
     * @param targetCluster The target cluster definition
     * @param storeDefs The list of store definitions to rebalance
     * @param enabledDeletePartition Delete the RW partition on the donor side
     *        after rebalance
     * @param isStealerBased Do we want to generate the final plan based on the
     *        stealer node or the donor node?
     */
    public RebalanceClusterPlan(final Cluster currentCluster,
                                final Cluster targetCluster,
                                final List<StoreDefinition> storeDefs,
                                final boolean enabledDeletePartition) {
        this(currentCluster, targetCluster, storeDefs, enabledDeletePartition, true);
    }

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
     * @param isStealerBased Do we want to generate the final plan based on the
     *        stealer node or the donor node?
     */
    public RebalanceClusterPlan(final Cluster currentCluster,
                                final Cluster targetCluster,
                                final List<StoreDefinition> storeDefs,
                                final boolean enabledDeletePartition,
                                final boolean isStealerBased) {
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        this.currentAllStoresNodeIdToAllPartitionTuples = Maps.newHashMap();
        this.targetAllStoresNodeIdToAllPartitionTuples = Maps.newHashMap();

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

        HashMultimap<Integer, RebalancePartitionsInfo> rebalancePartitionList = HashMultimap.create();
        for(Node node: targetCluster.getNodes()) {
            for(RebalancePartitionsInfo info: getRebalancePartitionsInfo(currentCluster,
                                                                         targetCluster,
                                                                         storeDefs,
                                                                         node.getId(),
                                                                         enabledDeletePartition)) {
                if(isStealerBased) {
                    rebalancePartitionList.put(info.getStealerId(), info);
                } else {
                    rebalancePartitionList.put(info.getDonorId(), info);
                }
            }
        }

        // Populate the rebalance task queue
        for(int nodeId: rebalancePartitionList.keySet()) {
            rebalanceTaskQueue.offer(new RebalanceNodePlan(nodeId,
                                                           Lists.newArrayList(rebalancePartitionList.get(nodeId)),
                                                           isStealerBased));
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
     * @param storeDefs List of store definitions
     * @param stealerNodeId Id of the stealer node
     * @param enableDeletePartition To delete or not to delete?
     */
    private List<RebalancePartitionsInfo> getRebalancePartitionsInfo(final Cluster currentCluster,
                                                                     final Cluster targetCluster,
                                                                     final List<StoreDefinition> storeDefs,
                                                                     final int stealerNodeId,
                                                                     final boolean enableDeletePartition) {
        final HashMap<Integer, HashMap<String, HashMap<Integer, List<Integer>>>> donorNodeToStoreToStealPartition = Maps.newHashMap();
        final HashMap<Integer, HashMap<String, HashMap<Integer, List<Integer>>>> donorNodeToStoreToDeletePartition = Maps.newHashMap();

        // Generate plans for individual stores
        for(StoreDefinition storeDef: storeDefs) {

            // For the "current" cluster, creates a map of node-ids to
            // corresponding list of [replica,partition] tuples (primary &
            // replicas)
            Map<Integer, Set<Pair<Integer, Integer>>> currentNodeIdToAllPartitionTuples = RebalanceUtils.getNodeIdToAllPartitions(currentCluster,
                                                                                                                                  storeDef,
                                                                                                                                  true);
            RebalanceUtils.combinePartitionTuples(currentAllStoresNodeIdToAllPartitionTuples,
                                                  currentNodeIdToAllPartitionTuples);

            // For the "target" cluster, creates a map of node-ids to
            // corresponding list of [replica,partition] tuples (primary &
            // replicas)
            Map<Integer, Set<Pair<Integer, Integer>>> targetNodeIdToAllPartitionTuples = RebalanceUtils.getNodeIdToAllPartitions(targetCluster,
                                                                                                                                 storeDef,
                                                                                                                                 true);

            RebalanceUtils.combinePartitionTuples(targetAllStoresNodeIdToAllPartitionTuples,
                                                  targetNodeIdToAllPartitionTuples);

            // A map of all the stealer nodes to their corresponding stolen
            Map<Integer, Set<Pair<Integer, Integer>>> stealerNodeIdToStolenPartitionTuples = RebalanceUtils.getStolenPartitionTuples(currentCluster,
                                                                                                                                     targetCluster,
                                                                                                                                     storeDef);

            // If null, done with this stealer node for this store
            if(stealerNodeIdToStolenPartitionTuples.get(stealerNodeId) == null) {
                continue;
            }

            final Set<Pair<Integer, Integer>> haveToStealTuples = Sets.newHashSet(stealerNodeIdToStolenPartitionTuples.get(stealerNodeId));

            // Now we find out which donor can donate partitions to this stealer
            for(Node donorNode: currentCluster.getNodes()) {

                // The same node can't donate
                if(donorNode.getId() == stealerNodeId)
                    continue;

                // Finished treating all partitions?
                if(haveFinishedPartitions(haveToStealTuples)) {
                    break;
                }

                final Set<Pair<Integer, Integer>> trackStealPartitionsTuples = new HashSet<Pair<Integer, Integer>>();
                final Set<Pair<Integer, Integer>> trackDeletePartitionsTuples = new HashSet<Pair<Integer, Integer>>();

                // Checks if this donor node can donate any tuples
                donatePartitionTuple(donorNode,
                                     haveToStealTuples,
                                     trackStealPartitionsTuples,
                                     currentNodeIdToAllPartitionTuples.get(donorNode.getId()));

                // Check if we can delete the partitions this donor just donated
                donateDeletePartitionTuple(donorNode,
                                           trackStealPartitionsTuples,
                                           trackDeletePartitionsTuples,
                                           stealerNodeIdToStolenPartitionTuples.get(donorNode.getId()),
                                           enableDeletePartition);

                // Add it to our completed list
                if(trackStealPartitionsTuples.size() > 0) {
                    addPartitionsToPlan(trackStealPartitionsTuples,
                                        donorNodeToStoreToStealPartition,
                                        donorNode.getId(),
                                        storeDef.getName());

                }

                // Add it to our completed list
                if(trackDeletePartitionsTuples.size() > 0) {
                    addPartitionsToPlan(trackDeletePartitionsTuples,
                                        donorNodeToStoreToDeletePartition,
                                        donorNode.getId(),
                                        storeDef.getName());
                }

            }
        }

        // Now combine the plans generated individually into the actual
        // rebalance partitions information
        final List<RebalancePartitionsInfo> result = new ArrayList<RebalancePartitionsInfo>();

        for(int donorNodeId: donorNodeToStoreToStealPartition.keySet()) {
            HashMap<String, HashMap<Integer, List<Integer>>> storeToStealPartition = donorNodeToStoreToStealPartition.get(donorNodeId);
            HashMap<String, HashMap<Integer, List<Integer>>> storeToDeletePartition = donorNodeToStoreToDeletePartition.containsKey(donorNodeId) ? donorNodeToStoreToDeletePartition.get(donorNodeId)
                                                                                                                                                : new HashMap<String, HashMap<Integer, List<Integer>>>();
            result.add(new RebalancePartitionsInfo(stealerNodeId,
                                                   donorNodeId,
                                                   storeToStealPartition,
                                                   storeToDeletePartition,
                                                   currentCluster,
                                                   0));
        }

        return result;
    }

    private void addPartitionsToPlan(Set<Pair<Integer, Integer>> trackPartitionsTuples,
                                     HashMap<Integer, HashMap<String, HashMap<Integer, List<Integer>>>> donorNodeToStoreToPartitionTuples,
                                     int donorNodeId,
                                     String name) {
        HashMap<String, HashMap<Integer, List<Integer>>> storeToStealPartitionTuples = null;
        if(donorNodeToStoreToPartitionTuples.containsKey(donorNodeId)) {
            storeToStealPartitionTuples = donorNodeToStoreToPartitionTuples.get(donorNodeId);
        } else {
            storeToStealPartitionTuples = Maps.newHashMap();
            donorNodeToStoreToPartitionTuples.put(donorNodeId, storeToStealPartitionTuples);
        }
        storeToStealPartitionTuples.put(name,
                                        RebalanceUtils.flattenPartitionTuples(trackPartitionsTuples));
    }

    /**
     * Given a donor node and a set of tuples that need to be stolen, checks if
     * the donor can contribute any
     * 
     * @param donorNode Donor node we are checking
     * @param haveToStealTuples The partition tuples which we ideally want to
     *        steal
     * @param trackStealPartitionsTuples Set of partitions tuples already stolen
     * @param donorPartitionTuples All partition tuples on donor node
     */
    private void donatePartitionTuple(final Node donorNode,
                                      Set<Pair<Integer, Integer>> haveToStealTuples,
                                      Set<Pair<Integer, Integer>> trackStealPartitionsTuples,
                                      Set<Pair<Integer, Integer>> donorPartitionTuples) {
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
     * @param donorPartitionTuples All partition tuples on donor node
     * @param enableDeletePartition Are we allowed to delete?
     */
    private void donateDeletePartitionTuple(final Node donor,
                                            Set<Pair<Integer, Integer>> trackStealPartitionsTuples,
                                            Set<Pair<Integer, Integer>> trackDeletePartitionsTuples,
                                            Set<Pair<Integer, Integer>> donorPartitionTuples,
                                            boolean enableDeletePartition) {

        // Are we allowed to delete AND do we have anything to check for
        // deletion?
        if(enableDeletePartition && trackStealPartitionsTuples.size() > 0) {

            // Retrieve information about what this donor node will steal
            List<Integer> partitionsStolenByDonor = RebalanceUtils.getPartitionsFromTuples(donorPartitionTuples);

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

        if(rebalanceTaskQueue == null || rebalanceTaskQueue.isEmpty()) {
            return "";
        }

        for(RebalanceNodePlan nodePlan: rebalanceTaskQueue) {
            builder.append((nodePlan.isNodeStealer() ? "Stealer " : "Donor ") + "Node "
                           + nodePlan.getNodeId());
            for(RebalancePartitionsInfo rebalancePartitionsInfo: nodePlan.getRebalanceTaskList()) {
                builder.append(rebalancePartitionsInfo).append(Utils.NEWLINE);
            }
        }

        return builder.toString();

    }

    public String printPartitionDistribution() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current Cluster: ")
          .append(Utils.NEWLINE)
          .append(RebalanceUtils.printMap(currentAllStoresNodeIdToAllPartitionTuples))
          .append(Utils.NEWLINE);
        sb.append("Target Cluster: ")
          .append(Utils.NEWLINE)
          .append(RebalanceUtils.printMap(targetAllStoresNodeIdToAllPartitionTuples));
        return sb.toString();
    }

}