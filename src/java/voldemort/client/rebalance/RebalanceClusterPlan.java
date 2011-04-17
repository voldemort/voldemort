package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;

/**
 * Compares the currentCluster configuration with the desired
 * targetConfiguration and returns a map of Target node-id to map of source
 * node-ids and partitions desired to be stolen/fetched.
 * 
 */
public class RebalanceClusterPlan {

    private final static String NL = System.getProperty("line.separator");
    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;
    private final List<StoreDefinition> storeDefList;

    /**
     * For the "current" cluster, this map contains a list of partitions
     * (primary & replicas) that each node has.
     */
    private final Map<Integer, Set<Integer>> clusterNodeIdToAllPartitions;

    /**
     * For the "target" cluster, this map contains a list of partitions (primary
     * & replicas) that each node will have as a result of re-mapping the
     * cluster.
     */
    private final Map<Integer, Set<Integer>> targetNodeIdToAllPartitions;

    /**
     * As a result of re-mapping the cluster, you will have nodes that will lose
     * partitions (primary or replicas). This map contains all partitions
     * (primary & replicas) that were moved away from the node i.e. deleted from
     * the node.
     */
    private final Map<Integer, Set<Integer>> targetNodeIdToAllDeletedPartitions;

    /**
     * As a result of re-mapping the cluster, you will have nodes that will
     * receive a partition or "donation" (primary or replicas). This map
     * contains all partitions (primary & replicas) that were donated to a node.
     */
    private final Map<Integer, Set<Integer>> targetNodeIdToAllDonatedPartitions;

    /**
     * This map keeps track of the deleted partitions that have being taking
     * care by the logic that builds the rebalance-plan.
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
     * @param currentROStoreVersionsDirs A mapping of nodeId to map of store
     *        names to version ids
     */
    public RebalanceClusterPlan(final Cluster currentCluster,
                                final Cluster targetCluster,
                                final List<StoreDefinition> storeDefList,
                                final boolean enabledDeletePartition,
                                final Map<Integer, Map<String, String>> currentROStoreVersionsDirs) {
        this.currentCluster = currentCluster;
        this.targetCluster = targetCluster;

        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        this.storeDefList = Collections.unmodifiableList(storeDefList);

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        // Create only once the node-to-all-partitions relationship.
        // All-partitions mean primaries and replicas that this node is
        // responsible for.
        this.clusterNodeIdToAllPartitions = Collections.unmodifiableMap(createNodeIdToAllPartitions(currentCluster));
        this.targetNodeIdToAllPartitions = Collections.unmodifiableMap(createNodeIdToAllPartitions(targetCluster));

        // As a result of re-mapping the cluster, target nodes can lose
        // partitions.
        this.targetNodeIdToAllDeletedPartitions = Collections.unmodifiableMap(createTargetNodeItToAllDeletePartitions());
        this.targetNodeIdToAllDonatedPartitions = Collections.unmodifiableMap(createTargetNodeItToAllDonatedPartitions());

        for(Node node: targetCluster.getNodes()) {
            List<RebalancePartitionsInfo> rebalanceNodeList = createRebalancePartitionsInfo(currentCluster,
                                                                                            targetCluster,
                                                                                            RebalanceUtils.getStoreNames(storeDefList),
                                                                                            node.getId(),
                                                                                            enabledDeletePartition);

            if(rebalanceNodeList.size() > 0) {
                if(currentROStoreVersionsDirs != null && currentROStoreVersionsDirs.size() > 0) {
                    for(RebalancePartitionsInfo partitionsInfo: rebalanceNodeList) {
                        partitionsInfo.setStealerNodeROStoreToDir(currentROStoreVersionsDirs.get(partitionsInfo.getStealerId()));
                        partitionsInfo.setDonorNodeROStoreToDir(currentROStoreVersionsDirs.get(partitionsInfo.getDonorId()));
                    }
                }
                rebalanceTaskQueue.offer(new RebalanceNodePlan(node.getId(), rebalanceNodeList));
            }
        }
    }

    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        return rebalanceTaskQueue;
    }

    /**
     * Creates a mapping of node id to the list of partitions that are lost as a
     * result of rebalance. This is simply the difference of set of all
     * partitions on node after rebalancing and all partitions before
     * rebalancing
     * 
     * @return Map to all "target" nodes-to-lost partitions
     */
    private Map<Integer, Set<Integer>> createTargetNodeItToAllDeletePartitions() {
        final Map<Integer, Set<Integer>> map = new TreeMap<Integer, Set<Integer>>();
        for(Integer targetNodeId: targetNodeIdToAllPartitions.keySet()) {
            Set<Integer> clusterAllPartitions = clusterNodeIdToAllPartitions.get(targetNodeId);
            Set<Integer> targetAllPartitions = targetNodeIdToAllPartitions.get(targetNodeId);

            Set<Integer> deletedPartitions = getDeletedInTarget(clusterAllPartitions,
                                                                targetAllPartitions);
            map.put(targetNodeId, deletedPartitions);
        }
        return map;
    }

    /**
     * Target nodes that received partitions (primary and/or replicas) as a
     * result of rebalance.
     * 
     * @return Map to all "target" nodes-to-donated partitions relationship.
     */
    private Map<Integer, Set<Integer>> createTargetNodeItToAllDonatedPartitions() {
        final Map<Integer, Set<Integer>> map = new TreeMap<Integer, Set<Integer>>();
        for(Integer stealNodeId: targetNodeIdToAllPartitions.keySet()) {
            Set<Integer> donatedPartitions = getStealPrimaries(currentCluster,
                                                               targetCluster,
                                                               stealNodeId);
            donatedPartitions.addAll(getStealReplicas(currentCluster, targetCluster, stealNodeId));
            map.put(stealNodeId, donatedPartitions);
        }
        return map;
    }

    /**
     * This approach is based on 2 principals:
     * 
     * <ol>
     * <li>The number of partitions don't change only they get redistributes
     * across nodes.
     * <li>A primary or replica partition that is going to be deleted is never
     * used to copy from by another stealer.
     * </ol>
     * 
     * Principal #2, based on design rebalance can be run in parallel, using
     * multiple threads. If one thread deletes a partition that another thread
     * is using to copy data from then a raised condition will be encounter
     * losing data. To prevent this we follow principal #2 in this method.
     * 
     */
    private List<RebalancePartitionsInfo> createRebalancePartitionsInfo(final Cluster currentCluster,
                                                                        final Cluster targetCluster,
                                                                        final List<String> storeNames,
                                                                        final int stealerId,
                                                                        final boolean enabledDeletePartition) {

        final List<RebalancePartitionsInfo> result = new ArrayList<RebalancePartitionsInfo>();

        // Separates primaries from replicas this stealer is stealing.
        final Set<Integer> haveToStealPrimaries = getStealPrimaries(currentCluster,
                                                                    targetCluster,
                                                                    stealerId);
        final Set<Integer> haveToStealReplicas = getStealReplicas(currentCluster,
                                                                  targetCluster,
                                                                  stealerId);
        final Set<Integer> haveToDeletingPartitions = getRemainToBeDeletedPartitions(stealerId);

        // If this stealer is not stealing any partitions or it has no partition
        // to be deleted then return.
        if(haveToStealPrimaries.size() == 0 && haveToStealReplicas.size() == 0
           && haveToDeletingPartitions.size() == 0) {
            return result;
        }

        // Now let's find out which donor can donate partitions to this stealer.
        for(Node donorNode: currentCluster.getNodes()) {
            if(donorNode.getId() == stealerId)
                continue;

            // If you have treated all partitions (primaries and replicas) that
            // this
            // stealer should steal, then you are all done.
            if(hasAllPartitionTreated(haveToStealPrimaries)
               && hasAllPartitionTreated(haveToStealReplicas)
               && hasAllPartitionTreated(haveToDeletingPartitions)) {
                break;
            }

            final Set<Integer> trackStealPartitions = new HashSet<Integer>();
            final Set<Integer> trackDeletePartitions = new HashSet<Integer>();
            final Set<Integer> trackStealMasterPartitions = new HashSet<Integer>();

            // Checks if this donor is donating primary partition only.
            donatePrimary(donorNode,
                          haveToStealPrimaries,
                          trackStealMasterPartitions,
                          trackStealPartitions);

            // Checks if this donor can donate a replicas.
            donateReplicas(donorNode, haveToStealReplicas, trackStealPartitions);

            // Delete partition if at least you have donated a primary or
            // replica
            deleteDonatedPartitions(donorNode,
                                    trackDeletePartitions,
                                    enabledDeletePartition,
                                    stealerId);

            if(trackStealPartitions.size() > 0 || trackDeletePartitions.size() > 0) {

                result.add(new RebalancePartitionsInfo(stealerId,
                                                       donorNode.getId(),
                                                       new ArrayList<Integer>(trackStealPartitions),
                                                       new ArrayList<Integer>(trackDeletePartitions),
                                                       new ArrayList<Integer>(trackStealMasterPartitions),
                                                       storeNames,
                                                       new HashMap<String, String>(),
                                                       new HashMap<String, String>(),
                                                       0));
            }
        }

        return result;
    }

    /**
     * Returns a list of partition that remain to be deleted in this node. That
     * is calculated based on Cluster and Target difference, minus all the other
     * partitions that this node already lost at the time of invoking this
     * method.
     * 
     * @param nodeId Node to be analyze.
     * @return List of remaining partitions to be deleted on this node.
     */
    private Set<Integer> getRemainToBeDeletedPartitions(int nodeId) {
        Set<Integer> delPartitions = new TreeSet<Integer>();
        if(targetNodeIdToAllDeletedPartitions.get(nodeId) != null
           && targetNodeIdToAllDeletedPartitions.get(nodeId).size() > 0) {
            // Gets all delete partition for this donor (Creates a deep copy
            // first).
            delPartitions = new TreeSet<Integer>(targetNodeIdToAllDeletedPartitions.get(nodeId));

            // How many have you deleted so far?
            Set<Integer> alreadyDeletedPartitions = alreadyDeletedNodeIdToPartions.get(nodeId);

            // Removes any primary partitions and the ones already deleted.
            // There is no side effect in deleting an already deleted partition,
            // but just for the sake of clarity, let's only delete once a
            // partition.
            if(alreadyDeletedPartitions != null)
                delPartitions.removeAll(alreadyDeletedPartitions);

        }

        return delPartitions;
    }

    /**
     * For a particular stealer node find all the replica partitions it will
     * steal. If the stealer node has a primary that was given away and later on
     * this partition became a replica then this replica is not going to be
     * consider one that need to be migrated due to the fact that it's already
     * in the stealer node so there is no need to copy it because it's alredy in
     * the stealer node.
     * 
     * @param cluster current running cluster.
     * @param target target cluster.
     * @param stealerId node-id acting as stealer
     * @return set of partition ids representing the replicas about to be
     *         stolen.
     */
    private Set<Integer> getStealReplicas(final Cluster cluster,
                                          final Cluster target,
                                          final int stealerId) {
        Map<Integer, Set<Integer>> clusterNodeIdToReplicas = getNodeIdToReplicas(cluster);
        Map<Integer, Set<Integer>> targetNodeIdToReplicas = getNodeIdToReplicas(target);

        Set<Integer> clusterStealerReplicas = clusterNodeIdToReplicas.get(stealerId);
        Set<Integer> targetStealerReplicas = targetNodeIdToReplicas.get(stealerId);

        Set<Integer> replicasAddedInTarget = getAddedInTarget(clusterStealerReplicas,
                                                              targetStealerReplicas);

        if(RebalanceUtils.containsNode(cluster, stealerId)) {
            replicasAddedInTarget.removeAll(cluster.getNodeById(stealerId).getPartitionIds());
        }
        return replicasAddedInTarget;
    }

    private void deleteDonatedPartitions(final Node donor,
                                         Set<Integer> trackDeletePartitions,
                                         boolean enabledDeletePartition,
                                         int stealerId) {
        final int donorId = donor.getId();

        if(enabledDeletePartition) {
            if(targetNodeIdToAllDeletedPartitions.get(donorId) != null
               && targetNodeIdToAllDeletedPartitions.get(donorId).size() > 0) {
                // Gets all delete partition for this donor (Creates a deep copy
                // first).
                Set<Integer> delPartitions = new TreeSet<Integer>(targetNodeIdToAllDeletedPartitions.get(donorId));

                // Does this donor need to give this partition to somebody else
                // later on?
                // If yes, then don't delete it now, it will be deleted when
                // it's given away to another Stealer.
                final Cluster sortedTargetCluster = sortCluster(targetCluster);
                for(Node somebody: sortedTargetCluster.getNodes()) {
                    if(somebody.getId() == donorId || somebody.getId() <= stealerId)
                        continue;

                    Set<Integer> somebodyWillReceivePartitions = targetNodeIdToAllDonatedPartitions.get(somebody.getId());
                    Iterator<Integer> iterator = delPartitions.iterator();
                    while(iterator.hasNext()) {
                        Integer delPartition = iterator.next();
                        if(somebodyWillReceivePartitions != null
                           && somebodyWillReceivePartitions.contains(delPartition)) {
                            // avoid the deletion.
                            iterator.remove();
                        }
                    }
                }

                // How many have you deleted so far?
                Set<Integer> alreadyDeletedPartitions = alreadyDeletedNodeIdToPartions.get(donorId);

                // Removes any primary partitions and the ones already deleted.
                // There is no side effect in deleting an already deleted
                // partition,
                // but just for the sake of clarity, let's only delete once a
                // partition.
                if(alreadyDeletedPartitions != null) {
                    delPartitions.removeAll(alreadyDeletedPartitions);
                }

                // add these del partition to the set used in the
                // RebalancePartitionsIndo.
                trackDeletePartitions.addAll(delPartitions);

                addDeletedPartition(alreadyDeletedNodeIdToPartions, donorId, delPartitions);
            }
        }

    }

    private void donateReplicas(final Node donorNode,
                                Set<Integer> haveToStealReplicas,
                                Set<Integer> trackStealPartitions) {
        final int donorId = donorNode.getId();
        final Set<Integer> donorAllParitions = clusterNodeIdToAllPartitions.get(donorId);
        final Iterator<Integer> iter = haveToStealReplicas.iterator();

        while(iter.hasNext()) {
            Integer stealingReplica = iter.next();

            // Does somebody needs to delete (in terms of cleaning or not longer
            // need)
            // this "stealingReplica" partition besides this Donor?
            // If YES then skip this Donor, it'll be donated by the one whom
            // will need
            // to delete it.
            //
            // Example:
            // Current Cluster:
            // 0 - [3, 6, 9, 12, 15] + [1, 2, 4, 5, 7, 8, 10, 11, 13, 14]
            // 1 - [1, 4, 7, 10, 13, 16] + [0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15,
            // 17]
            // 2 - [2, 5, 8, 11, 14, 17] + [0, 1, 3, 4, 6, 7, 9, 10, 12, 13, 15,
            // 16]
            // 3 - [0] + [16, 17]
            //
            // Target Cluster:
            // 0 - [0, 3, 6, 9, 12, 15] + [1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16,
            // 17]
            // 1 - [1, 4, 7, 10, 13, 16] + [0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15,
            // 17]
            // 2 - [2, 5, 8, 11, 14, 17] + [0, 1, 3, 4, 6, 7, 9, 10, 12, 13, 15,
            // 16]
            // 3 - [] + []
            //
            // Let's see this optimization by looking the following 2 plans:
            //
            // Plan #1:
            // RebalancingStealInfo(0 <--- 3 partitions:[0] steal master
            // partitions:[0] deleted:[17, 0, 16]
            // RebalancingStealInfo(0 <--- 1 partitions:[17, 16] steal master
            // partitions:[] deleted:[]
            // 
            // *** BAD Plan - The reason this is not optimal is due to the fact
            // that fist task will remap the cluster AND partitions 17 and 16
            // will be deleted leaving effectively 2 replicas of 16 and 17.
            // If Node1 dies (the Donor) before completing second task
            // (see second task) then you will have one replica of 16 and 17
            // (Quorum-Exception),
            //
            // Plan #2:
            // RebalancingStealInfo(0 <--- 3 partitions:[17, 0, 16] steal master
            // partitions:[0] deleted:[17, 0, 16]
            //
            // *** GOOD Plan - This is better because if Node3 dies you will
            // still have 2 replicas of 16 and 17.
            //
            boolean isSomebodyDeleting = false;
            for(Node somebody: targetCluster.getNodes()) {
                if(somebody.getId() == donorId)
                    continue;

                Set<Integer> somebodyDeletedPartitions = targetNodeIdToAllDeletedPartitions.get(somebody.getId());
                Set<Integer> alreadyDeletedPartitions = alreadyDeletedNodeIdToPartions.get(somebody.getId());
                if(alreadyDeletedPartitions != null && !alreadyDeletedPartitions.isEmpty())
                    somebodyDeletedPartitions.removeAll(alreadyDeletedPartitions);

                if(somebodyDeletedPartitions.contains(stealingReplica)) {
                    isSomebodyDeleting = true;
                }
            }

            if(isSomebodyDeleting)
                continue;

            // Make sure that the partition is not previously donated
            // to this Donor, a different thread could be donating
            // this partition and not yet completed (full-transfer the data)
            Set<Integer> donorDonatedPartitions = targetNodeIdToAllDonatedPartitions.get(donorId);
            if(donorDonatedPartitions != null && donorDonatedPartitions.contains(stealingReplica))
                continue;

            // Nobody has to delete this partition and
            // this partition was not donated previously to this Donor.
            // then you can donate the replica partition from this Donor now.
            Set<Integer> deletedPartitions = alreadyDeletedNodeIdToPartions.get(donorId);
            boolean isDeletedPartition = false;
            if(deletedPartitions != null && deletedPartitions.contains(stealingReplica)) {
                isDeletedPartition = true;
            }

            if(donorAllParitions.contains(stealingReplica) && !isDeletedPartition) {
                trackStealPartitions.add(stealingReplica);
                // only one node will donate this partition.
                iter.remove();
            }
        }

    }

    private void donatePrimary(final Node donorNode,
                               Set<Integer> haveToStealPrimaries,
                               Set<Integer> trackStealMasterPartitions,
                               Set<Integer> trackStealPartitions) {
        final List<Integer> donorPrimaryPartitionIds = Collections.unmodifiableList(donorNode.getPartitionIds());
        final Iterator<Integer> iter = haveToStealPrimaries.iterator();
        while(iter.hasNext()) {
            Integer stealingPrimary = iter.next();
            if(donorPrimaryPartitionIds.contains(stealingPrimary)) {
                trackStealMasterPartitions.add(stealingPrimary);
                trackStealPartitions.add(stealingPrimary);

                // This slealedPrimary partition has been donated, let's counter
                // it out.
                iter.remove();
            }
        }
    }

    private boolean hasAllPartitionTreated(Set<Integer> partitionToBeTreatedSet) {
        return (partitionToBeTreatedSet == null || partitionToBeTreatedSet.size() == 0);
    }

    private void addDeletedPartition(Map<Integer, Set<Integer>> map, int key, Set<Integer> values) {
        for(Integer value: values) {
            addDeletedPartition(map, key, value);
        }
    }

    private void addDeletedPartition(Map<Integer, Set<Integer>> map, int key, Integer value) {
        if(map.containsKey(key)) {
            map.get(key).add(value);
        } else {
            Set<Integer> set = new TreeSet<Integer>();
            set.add(value);
            map.put(key, set);
        }
    }

    /**
     * Returns a set of partitions that were added to the target for a given
     * node
     * 
     * getAddedInTarget(cluster, null) - nothing was added, returns null. <br>
     * getAddedInTarget(null, target) - everything in target was added, return
     * target. <br>
     * getAddedInTarget(null, null) - neither added nor deleted, return null. <br>
     * getAddedInTarget(cluster, target)) - returns new partition not found in
     * cluster.
     * 
     * @param current Set of partitions present in current cluster for a given
     *        node.
     * @param target Set of partitions present in target cluster for a given
     *        node.
     * @return A set of added partitions in target cluster or empty set
     */
    private Set<Integer> getAddedInTarget(Set<Integer> current, Set<Integer> target) {
        if(current == null || target == null) {
            return new TreeSet<Integer>();
        }
        return getDiff(target, current);
    }

    /**
     * Returns a set of partitions that were deleted to the target
     * 
     * getDeletedInTarget(cluster, null) - everything was deleted, returns
     * cluster. <br>
     * getDeletedInTarget(null, target) - everything in target was added, return
     * target. <br>
     * getDeletedInTarget(null, null) - neither added nor deleted, return null. <br>
     * getDeletedInTarget(cluster, target)) - returns deleted partition not
     * found in target.
     * 
     * @param current Set of partitions present in current cluster for a given
     *        node.
     * @param target Set of partitions present in target cluster for a given
     *        node.
     * @return A set of deleted partitions in Target cluster or empty set
     */
    private Set<Integer> getDeletedInTarget(final Set<Integer> current, final Set<Integer> target) {
        if(current == null || target == null) {
            return new TreeSet<Integer>();
        }
        return getDiff(current, target);
    }

    private Set<Integer> getDiff(final Set<Integer> source, final Set<Integer> dest) {
        Set<Integer> diff = new TreeSet<Integer>();
        for(Integer id: source) {
            if(!dest.contains(id)) {
                diff.add(id);
            }
        }
        return diff;
    }

    /**
     * This is a very useful method that returns a string representation of the
     * cluster The idea of this method is to expose clearly the Node and its
     * primary & replicas partitions in the allowing format:
     * 
     * <pre>
     * 
     * Current Cluster:
     * 0 - [0, 1, 2, 3] + [7, 8, 9]
     * 1 - [4, 5, 6] + [0, 1, 2, 3]
     * 2 - [7, 8, 9] + [4, 5, 6]
     * 
     * </pre>
     * 
     * @param nodeItToAllPartitions
     * @param cluster
     * @return string representation of the cluster, indicating node,primary and
     *         replica relationship.
     * 
     */
    private String printMap(final Map<Integer, Set<Integer>> nodeItToAllPartitions,
                            final Cluster cluster) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Integer, Set<Integer>> entry: nodeItToAllPartitions.entrySet()) {
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
            sb.append(nodeId + " - " + onlyPrimaries + " + " + onlyReplicas).append(NL);
        }
        return sb.toString();
    }

    /**
     * For a particular cluster creates a mapping of node id to their
     * corresponding list of primary and replica partitions
     * 
     * @param cluster The cluster metadata
     * @return Map of node id to set of "all" partitions
     */
    private Map<Integer, Set<Integer>> createNodeIdToAllPartitions(final Cluster cluster) {
        final Collection<Node> nodes = cluster.getNodes();
        final StoreDefinition maxReplicationStore = RebalanceUtils.getMaxReplicationStore(this.storeDefList);
        final RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(maxReplicationStore,
                                                                                                   cluster);

        final Map<Integer, Set<Integer>> nodeIdToAllPartitions = new HashMap<Integer, Set<Integer>>();
        final Map<Integer, Integer> partitionToNodeIdMap = getPartitionToNode(nodes);

        // Map initialization.
        for(Node node: nodes) {
            nodeIdToAllPartitions.put(node.getId(), new TreeSet<Integer>());
        }

        // Loops through all nodes
        for(Node node: nodes) {

            // Gets the partitions that this node was configured with.
            for(Integer primary: node.getPartitionIds()) {

                // Gets the list of replicating partitions.
                List<Integer> replicaPartitionList = routingStrategy.getReplicatingPartitionList(primary);

                // Get the node that this replicating partition belongs to.
                for(Integer replicaPartition: replicaPartitionList) {
                    Integer replicaNodeId = partitionToNodeIdMap.get(replicaPartition);

                    // The replicating node will have a copy of primary.
                    nodeIdToAllPartitions.get(replicaNodeId).add(primary);
                }
            }
        }
        return nodeIdToAllPartitions;
    }

    private Map<Integer, Set<Integer>> getNodeIdToReplicas(final Cluster cluster) {
        final Collection<Node> nodes = cluster.getNodes();
        final StoreDefinition maxReplicationStore = RebalanceUtils.getMaxReplicationStore(this.storeDefList);
        final RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(maxReplicationStore,
                                                                                                   cluster);

        final Map<Integer, Set<Integer>> nodeIdToReplicas = new HashMap<Integer, Set<Integer>>();
        final Map<Integer, Integer> partitionToNodeIdMap = getPartitionToNode(nodes);

        // Map initialization.
        for(Node node: nodes) {
            nodeIdToReplicas.put(node.getId(), new TreeSet<Integer>());
        }

        // Loops through all nodes
        for(Node node: nodes) {

            // Gets the partitions that this node was configured with.
            for(Integer primary: node.getPartitionIds()) {

                // Gets the list of replicating partitions.
                List<Integer> replicaPartitionList = routingStrategy.getReplicatingPartitionList(primary);

                // Because we only want the replicas and
                // "outingStrategy.getReplicatingPartitionList(primary)"
                // gives you also the primary we would have to remove the
                // primary from the list.
                replicaPartitionList.remove(primary);

                // Get the node that this replicating partition belongs to.
                for(Integer replicaPartition: replicaPartitionList) {
                    Integer replicaNodeId = partitionToNodeIdMap.get(replicaPartition);

                    // The replicating node will have a copy of primary.
                    nodeIdToReplicas.get(replicaNodeId).add(primary);
                }
            }
        }
        return nodeIdToReplicas;
    }

    /**
     * @return Reverse lookup from partitionId-to-NodeID. This is a Map of the
     *         information found in cluster.xml.
     */
    private Map<Integer, Integer> getPartitionToNode(final Collection<Node> nodes) {
        final Map<Integer, Integer> partitionToNodeIdMap = new LinkedHashMap<Integer, Integer>();

        // For each node in the cluster, get its partitions (shards in VShards
        // terminology)
        for(Node node: nodes) {
            for(Integer partitionId: node.getPartitionIds()) {

                // Make sure that this same partition was NOT configured in
                // another
                // Node. If so then throw an exception and let me know
                // the Nodes that shared this bad partition id.
                Integer previousRegisteredNodeId = partitionToNodeIdMap.get(partitionId);
                if(previousRegisteredNodeId != null) {
                    throw new IllegalArgumentException("Partition id " + partitionId
                                                       + " found in 2 nodes ID's: " + node.getId()
                                                       + " and " + previousRegisteredNodeId);
                }

                partitionToNodeIdMap.put(partitionId, node.getId());
            }
        }

        return partitionToNodeIdMap;
    }

    /**
     * For a particular stealer node find all the partitions it will steal
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param targetCluster The target cluster definition
     * @param stealNodeId The id of the stealer node
     * @return Returns a list of partitions which this stealer node will get
     */
    private Set<Integer> getStealPrimaries(Cluster currentCluster,
                                           Cluster targetCluster,
                                           int stealNodeId) {
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return new TreeSet<Integer>(targetList);
    }

    @Override
    public String toString() {
        if(rebalanceTaskQueue.isEmpty()) {
            return "Rebalance task queue is empty, No rebalancing needed";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Cluster Rebalancing Plan:").append(NL);
        builder.append(toString(getRebalancingTaskQueue()));
        return builder.toString();
    }

    public String toString(Queue<RebalanceNodePlan> queue) {
        if(queue == null || queue.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder(NL);
        for(RebalanceNodePlan nodePlan: queue) {
            builder.append("StealerNode:" + nodePlan.getStealerNode()).append(NL);
            for(RebalancePartitionsInfo rebalancePartitionsInfo: nodePlan.getRebalanceTaskList()) {
                builder.append("\t RebalancePartitionsInfo: " + rebalancePartitionsInfo).append(NL);
                builder.append("\t\t getStealMasterPartitions(): "
                               + rebalancePartitionsInfo.getStealMasterPartitions()).append(NL);
                builder.append("\t\t getPartitionList(): "
                               + rebalancePartitionsInfo.getPartitionList()).append(NL);
                builder.append("\t\t getDeletePartitionsList(): "
                               + rebalancePartitionsInfo.getDeletePartitionsList()).append(NL);
                builder.append("\t\t getUnbalancedStoreList(): "
                               + rebalancePartitionsInfo.getUnbalancedStoreList())
                       .append(NL)
                       .append(NL);
            }
        }

        return builder.toString();
    }

    public String printPartitionDistribution() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current Cluster: ").append(NL).append(printMap(clusterNodeIdToAllPartitions,
                                                                  currentCluster)).append(NL);
        sb.append("Target Cluster: ").append(NL).append(printMap(targetNodeIdToAllPartitions,
                                                                 targetCluster)).append(NL);
        return sb.toString();
    }

    private Cluster sortCluster(final Cluster cluster) {
        Collection<Node> nodes = cluster.getNodes();
        List<Node> deepCopy = new ArrayList<Node>(nodes);
        Collections.sort(deepCopy);
        return new Cluster(currentCluster.getName(), deepCopy);
    }
}