package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

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
     * list of partitions (primary & replicas)
     */
    private final Map<Integer, Set<Integer>> nodeIdToAllPartitions;

    /**
     * For the "target" cluster, creates a map of node-ids to corresponding list
     * of partitions (primary & replicas)
     */
    private final Map<Integer, Set<Integer>> targetNodeIdToAllPartitions;

    /**
     * For the "target" cluster, creates a map of node-ids to partitions being
     * deleted
     */
    private final Map<Integer, Set<Integer>> targetNodeIdToAllDeletedPartitions;

    /**
     * For the "target" cluster, creates a map of node-ids to partitions being
     * donated
     */
    private final Map<Integer, Set<Integer>> targetNodeIdToAllStolenPartitions;

    /**
     * This map keeps track of already deleted partitions on a per node basis
     */
    private Map<Integer, Set<Integer>> alreadyDeletedNodeIdToPartions = new TreeMap<Integer, Set<Integer>>();

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

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should be equal [ Current cluster ("
                                         + currentCluster.getNumberOfPartitions()
                                         + ") not equal to Target cluster ("
                                         + targetCluster.getNumberOfPartitions() + ") ]");

        // Create node id to all partitions mapping
        this.nodeIdToAllPartitions = Collections.unmodifiableMap(RebalanceUtils.getNodeIdToAllPartitions(currentCluster,
                                                                                                         storeDefs,
                                                                                                         true));
        this.targetNodeIdToAllPartitions = Collections.unmodifiableMap(RebalanceUtils.getNodeIdToAllPartitions(targetCluster,
                                                                                                               storeDefs,
                                                                                                               true));

        this.targetNodeIdToAllDeletedPartitions = Collections.unmodifiableMap(getTargetNodeIdToAllDeletePartitions());
        this.targetNodeIdToAllStolenPartitions = Collections.unmodifiableMap(getTargetNodeIdToAllStolenPartitions());

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
     * Creates a mapping of node id to the list of partitions that are going to
     * be deleted as a result of rebalance. This is simply the difference of set
     * of all partitions on node after rebalancing and all partitions before
     * rebalancing
     * 
     * @return Map to all "target" nodes-to-lost partitions
     */
    private Map<Integer, Set<Integer>> getTargetNodeIdToAllDeletePartitions() {
        final Map<Integer, Set<Integer>> map = new TreeMap<Integer, Set<Integer>>();
        for(Integer targetNodeId: targetNodeIdToAllPartitions.keySet()) {
            Set<Integer> clusterAllPartitions = nodeIdToAllPartitions.get(targetNodeId);
            Set<Integer> targetAllPartitions = targetNodeIdToAllPartitions.get(targetNodeId);

            Set<Integer> deletedPartitions = RebalanceUtils.getDeletedInTarget(clusterAllPartitions,
                                                                               targetAllPartitions);
            map.put(targetNodeId, deletedPartitions);
        }
        return map;
    }

    /**
     * Generates a map of node ids to the corresponding partitions that will be
     * stolen by this node
     * 
     * @return Map to all "target" nodes-to-stolen partitions relationship.
     */
    private Map<Integer, Set<Integer>> getTargetNodeIdToAllStolenPartitions() {
        final Map<Integer, Set<Integer>> map = new TreeMap<Integer, Set<Integer>>();
        for(int stealNodeId: targetNodeIdToAllPartitions.keySet()) {
            Set<Integer> donatedPartitions = RebalanceUtils.getStolenPrimaries(currentCluster,
                                                                               targetCluster,
                                                                               stealNodeId);
            donatedPartitions.addAll(RebalanceUtils.getStolenReplicas(currentCluster,
                                                                      targetCluster,
                                                                      storeDefs,
                                                                      stealNodeId));
            map.put(stealNodeId, donatedPartitions);
        }
        return map;
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

        // Separate the primary partitions from the replica partitions
        final Set<Integer> haveToStealPrimaries = RebalanceUtils.getStolenPrimaries(currentCluster,
                                                                                    targetCluster,
                                                                                    stealerId);
        final Set<Integer> haveToStealReplicas = RebalanceUtils.getStolenReplicas(currentCluster,
                                                                                  targetCluster,
                                                                                  storeDefs,
                                                                                  stealerId);
        final Set<Integer> haveToDeletePartitions = getDeletedPartitions(stealerId);

        // If all of them are empty, done with this stealer node
        if(haveToStealPrimaries.size() == 0 && haveToStealReplicas.size() == 0
           && haveToDeletePartitions.size() == 0) {
            return result;
        }

        // Now we find out which donor can donate partitions to this stealer
        for(Node donorNode: currentCluster.getNodes()) {

            // The same node can't donate
            if(donorNode.getId() == stealerId)
                continue;

            // Finished treating all partitions, done
            if(haveFinishedPartitions(haveToStealPrimaries)
               && haveFinishedPartitions(haveToStealReplicas)
               && haveFinishedPartitions(haveToDeletePartitions)) {
                break;
            }

            final Set<Integer> trackStealMasterPartitions = new HashSet<Integer>();
            final Set<Integer> trackStealReplicaPartitions = new HashSet<Integer>();
            final Set<Integer> trackDeletePartitions = new HashSet<Integer>();

            // Checks if this donor can donate a primary partition
            donatePrimary(donorNode, haveToStealPrimaries, trackStealMasterPartitions);

            // Checks if this donor can donate a replica
            donateReplicas(donorNode, haveToStealReplicas, trackStealReplicaPartitions);

            // Delete partition if you have donated a primary or replica
            deleteDonatedPartitions(donorNode,
                                    trackDeletePartitions,
                                    enableDeletePartition,
                                    stealerId);

            if(trackStealMasterPartitions.size() > 0 || trackStealReplicaPartitions.size() > 0
               || trackDeletePartitions.size() > 0) {

                result.add(new RebalancePartitionsInfo(stealerId,
                                                       donorNode.getId(),
                                                       new ArrayList<Integer>(trackStealMasterPartitions),
                                                       new ArrayList<Integer>(trackStealReplicaPartitions),
                                                       new ArrayList<Integer>(trackDeletePartitions),
                                                       storeNames,
                                                       0));
            }
        }

        return result;
    }

    /**
     * Returns a list of partitions that remain to be deleted on this node.
     * 
     * @param nodeId Stealer node id
     * @return List of remaining partitions to be deleted on this node.
     */
    private Set<Integer> getDeletedPartitions(int nodeId) {
        Set<Integer> delPartitions = new TreeSet<Integer>();
        if(targetNodeIdToAllDeletedPartitions.get(nodeId) != null
           && targetNodeIdToAllDeletedPartitions.get(nodeId).size() > 0) {
            // Get all the partitions to delete for this donor
            delPartitions = new TreeSet<Integer>(targetNodeIdToAllDeletedPartitions.get(nodeId));

            // How many have you deleted so far?
            Set<Integer> alreadyDeletedPartitions = alreadyDeletedNodeIdToPartions.get(nodeId);

            // Why delete an already deleted partition?
            if(alreadyDeletedPartitions != null)
                delPartitions.removeAll(alreadyDeletedPartitions);

        }

        return delPartitions;
    }

    /**
     * Given a donor node and a set of primary partitions that need to be
     * stolen, checks if the donor can contribute any partitions
     * 
     * @param donorNode Donor node we are checking
     * @param haveToStealPrimaries The partitions which we ideally want to steal
     * @param trackStealMasterPartitions Set of master partitions already stolen
     */
    private void donatePrimary(final Node donorNode,
                               Set<Integer> haveToStealPrimaries,
                               Set<Integer> trackStealMasterPartitions) {
        final List<Integer> donorPrimaryPartitionIds = Collections.unmodifiableList(donorNode.getPartitionIds());
        final Iterator<Integer> iter = haveToStealPrimaries.iterator();
        while(iter.hasNext()) {
            Integer primaryPartitionToSteal = iter.next();
            if(donorPrimaryPartitionIds.contains(primaryPartitionToSteal)) {
                trackStealMasterPartitions.add(primaryPartitionToSteal);
                // This partition has been donated, let's remove it
                iter.remove();
            }
        }
    }

    /**
     * Given a donor node and a set of replica partitions that need to be
     * stolen, checks if the donor can contribute any partitions
     * 
     * @param donorNode Donor node
     * @param haveToStealReplicas The replica partitions we want to steal
     * @param trackStealPartitions Set of replica partitions already stolen
     */
    private void donateReplicas(final Node donorNode,
                                Set<Integer> haveToStealReplicas,
                                Set<Integer> trackStealPartitions) {
        final int donorId = donorNode.getId();
        final Set<Integer> donorAllPartitions = nodeIdToAllPartitions.get(donorId);
        final Iterator<Integer> iter = haveToStealReplicas.iterator();

        while(iter.hasNext()) {
            int replicaPartitionToSteal = iter.next();

            boolean deletedInFuture = false;

            // If going to be deleted, ignore
            for(Node targetNode: targetCluster.getNodes()) {
                if(targetNode.getId() == donorId)
                    continue;

                Set<Integer> allDeletedPartitions = targetNodeIdToAllDeletedPartitions.get(targetNode.getId());
                Set<Integer> alreadyDeletedPartitions = alreadyDeletedNodeIdToPartions.get(targetNode.getId());
                if(alreadyDeletedPartitions != null && !alreadyDeletedPartitions.isEmpty())
                    allDeletedPartitions.removeAll(alreadyDeletedPartitions);

                if(allDeletedPartitions.contains(replicaPartitionToSteal)) {
                    deletedInFuture = true;
                }
            }

            if(deletedInFuture)
                continue;

            // Make sure that the partition is not previously donated
            // to this donor
            Set<Integer> donorDonatedPartitions = targetNodeIdToAllStolenPartitions.get(donorId);
            if(donorDonatedPartitions != null
               && donorDonatedPartitions.contains(replicaPartitionToSteal))
                continue;

            Set<Integer> deletedPartitions = alreadyDeletedNodeIdToPartions.get(donorId);
            boolean isDeletedPartition = false;
            if(deletedPartitions != null && deletedPartitions.contains(replicaPartitionToSteal)) {
                isDeletedPartition = true;
            }

            if(donorAllPartitions.contains(replicaPartitionToSteal) && !isDeletedPartition) {
                trackStealPartitions.add(replicaPartitionToSteal);
                iter.remove();
            }
        }

    }

    /**
     * Given a donor node and a set of partitions which need to be deleted,
     * checks if any partitions will be deleted on this donor
     * 
     * @param donor Donor node
     * @param trackDeletePartitions Set of partitions already deleted
     * @param enabledDeletePartition Do we want to delete?
     * @param stealerId Stealer node id
     */
    private void deleteDonatedPartitions(final Node donor,
                                         Set<Integer> trackDeletePartitions,
                                         boolean enabledDeletePartition,
                                         int stealerId) {
        final int donorId = donor.getId();

        if(enabledDeletePartition) {
            if(targetNodeIdToAllDeletedPartitions.get(donorId) != null
               && targetNodeIdToAllDeletedPartitions.get(donorId).size() > 0) {
                // Gets all partitions to be deleted for this donor
                Set<Integer> delPartitions = new TreeSet<Integer>(targetNodeIdToAllDeletedPartitions.get(donorId));

                // Does this donor need to give this partition to somebody else
                // in the future ? If yes, then don't delete it now. It will be
                // deleted when it is given away to another stealer.
                final Cluster sortedTargetCluster = sortCluster(targetCluster);
                for(Node targetNode: sortedTargetCluster.getNodes()) {
                    if(targetNode.getId() == donorId || targetNode.getId() <= stealerId)
                        continue;

                    Set<Integer> stolenPartitions = targetNodeIdToAllStolenPartitions.get(targetNode.getId());
                    Iterator<Integer> iterator = delPartitions.iterator();
                    while(iterator.hasNext()) {
                        Integer delPartition = iterator.next();
                        if(stolenPartitions != null && stolenPartitions.contains(delPartition)) {
                            // avoid the deletion.
                            iterator.remove();
                        }
                    }
                }

                // How many have you deleted so far?
                Set<Integer> alreadyDeletedPartitions = alreadyDeletedNodeIdToPartions.get(donorId);

                // Remove already deleted partitions
                if(alreadyDeletedPartitions != null) {
                    delPartitions.removeAll(alreadyDeletedPartitions);
                }

                // Add the remaining partitions to be deleted
                trackDeletePartitions.addAll(delPartitions);

                // Also add them to the global list of deleted partitions
                for(int delPartition: delPartitions) {
                    if(alreadyDeletedNodeIdToPartions.containsKey(donorId)) {
                        alreadyDeletedNodeIdToPartions.get(donorId).add(delPartition);
                    } else {
                        Set<Integer> set = new TreeSet<Integer>();
                        set.add(delPartition);
                        alreadyDeletedNodeIdToPartions.put(donorId, set);
                    }
                }

            }
        }

    }

    private boolean haveFinishedPartitions(Set<Integer> partitionToBeTreatedSet) {
        return (partitionToBeTreatedSet == null || partitionToBeTreatedSet.size() == 0);
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
     * @param nodeIdToAllPartitions Mapping of node id to all the partitions
     * @param cluster The cluster metadata
     * @return Returns a string representation of the cluster
     */
    private String printMap(final Map<Integer, Set<Integer>> nodeIdToAllPartitions,
                            final Cluster cluster) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Integer, Set<Integer>> entry: nodeIdToAllPartitions.entrySet()) {
            final Integer nodeId = entry.getKey();
            final Set<Integer> primariesAndReplicas = entry.getValue();

            final List<Integer> primaries = cluster.getNodeById(nodeId).getPartitionIds();
            Set<Integer> onlyPrimaries = new TreeSet<Integer>();
            Set<Integer> onlyReplicas = new TreeSet<Integer>();

            for(Integer allPartition: primariesAndReplicas) {
                if(primaries.contains(allPartition)) {
                    onlyPrimaries.add(allPartition);
                } else {
                    onlyReplicas.add(allPartition);
                }
            }
            sb.append(nodeId + " - " + onlyPrimaries + " + " + onlyReplicas).append(Utils.NEWLINE);
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
                builder.append("\t\t Steal master partitions: "
                               + rebalancePartitionsInfo.getStealMasterPartitions())
                       .append(Utils.NEWLINE);
                builder.append("\t\t Stealer replica partitions: "
                               + rebalancePartitionsInfo.getStealReplicaPartitions())
                       .append(Utils.NEWLINE);
                builder.append("\t\t Delete partitions: "
                               + rebalancePartitionsInfo.getDeletePartitionsList())
                       .append(Utils.NEWLINE);
                builder.append("\t\t getUnbalancedStoreList(): "
                               + rebalancePartitionsInfo.getUnbalancedStoreList())
                       .append(Utils.NEWLINE);
            }
        }

        return builder.toString();
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

    private Cluster sortCluster(final Cluster cluster) {
        Collection<Node> nodes = cluster.getNodes();
        List<Node> deepCopy = new ArrayList<Node>(nodes);
        Collections.sort(deepCopy);
        return new Cluster(currentCluster.getName(),
                           deepCopy,
                           Lists.newArrayList(cluster.getZones()));
    }
}