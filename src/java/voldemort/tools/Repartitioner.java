/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.ClusterUtils;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * RepartitionUtils provides functions that balance the distribution of
 * partitions across a cluster.
 * 
 */
public class Repartitioner {

    static Logger logger = Logger.getLogger(Repartitioner.class);

    /**
     * Recommended (default) number of times to attempt repartitioning.
     */
    public final static int DEFAULT_REPARTITION_ATTEMPTS = 5;
    /**
     * Default number of random partition ID swaps to attempt, if random swaps
     * are enabled.
     */
    public final static int DEFAULT_RANDOM_SWAP_ATTEMPTS = 100;
    /**
     * Default number of successful random swaps (i.e., the random swap improves
     * balance) after which reparitioning stops, if random swaps are enabled.
     */
    public final static int DEFAULT_RANDOM_SWAP_SUCCESSES = 100;
    /**
     * Default number of greedy partition ID swaps to perform, if greedy swaps
     * are enabled. Each greedy partition ID swaps considers (some number of
     * partitions per node) X (some number of partitions from rest of cluster)
     * and selects the best such swap.
     */
    public final static int DEFAULT_GREEDY_SWAP_ATTEMPTS = 5;
    /**
     * Default setting for which zone IDs to run greedy swap algorithm. null
     * implies greedily swapping across all zones.
     */
    public final static List<Integer> DEFAULT_GREEDY_ZONE_IDS = null;
    /**
     * Default (max) number of partition IDs per node to consider, if greedy
     * swaps are enabled.
     */
    public final static int DEFAULT_GREEDY_MAX_PARTITIONS_PER_NODE = 5;
    /**
     * Default (max) number of partition IDs from all the other nodes in the
     * cluster to consider, if greedy swaps are enabled.
     */
    public final static int DEFAULT_GREEDY_MAX_PARTITIONS_PER_ZONE = 25;
    /**
     * Default limit on length of contiguous partition ID run within a zone. 0
     * implies no limit on such runs.
     */
    public final static int DEFAULT_MAX_CONTIGUOUS_PARTITIONS = 0;

    /**
     * Runs a number of distinct algorithms over the specified clusters/store
     * defs to better balance partition IDs over nodes such that all nodes have
     * similar iops and capacity usage.
     * 
     * The algorithms (in order):
     * <ul>
     * <li>Get rid of contiguous runs of partition IDs within a zone. Such runs
     * make balancing load overall more difficult.
     * <li>Balance partition IDs among zones and/or among nodes within zones.
     * <li>Randomly swap partition IDs among nodes to improve overall balance.
     * (Any swap that improves balance is accepted.)
     * <li>Greedily swap partition IDs among nodes to improve overall balance.
     * (Some number of swaps are considered and the best of which is accepted.)
     * </ul>
     * 
     * This method is used for three key use cases:
     * <ul>
     * <li>Shuffling : Distribute partition IDs better for an existing cluster.
     * <li>Cluster expansion : Distribute partition IDs to take advantage of new
     * nodes (added to some of the zones).
     * <li>Zone expansion : Distribute partition IDs into a new zone.
     * </ul>
     * 
     * @param currentCluster current cluster
     * @param currentStoreDefs current store defs
     * @param interimCluster interim cluster; needed for cluster or zone
     *        expansion, otherwise pass in same as currentCluster.
     * @param finalStoreDefs final store defs; needed for zone expansion,
     *        otherwise pass in same as currentStores.
     * @param outputDir Directory in which to dump cluster xml and analysis
     *        files.
     * @param attempts Number of distinct repartitionings to attempt, the best
     *        of which is returned.
     * @param disableNodeBalancing Disables the core algorithm that balances
     *        primaries among nodes within each zone.
     * @param disableZoneBalancing For the core algorithm that balances
     *        primaries among nodes in each zone, disable balancing primaries
     *        among zones.
     * @param enableRandomSwaps Enables random swap optimization.
     * @param randomSwapAttempts
     * @param randomSwapSuccesses
     * @param randomSwapZoneIds 
     * @param enableGreedySwaps Enables greedy swap optimization.
     * @param greedyZoneIds
     * @param greedySwapAttempts
     * @param greedySwapMaxPartitionsPerNode
     * @param greedySwapMaxPartitionsPerZone
     * @param maxContiguousPartitionsPerZone
     * @return "final cluster" that has had all specified balancing algorithms
     *         run against it. The number of zones and number of nodes will
     *         match that of the specified "interim cluster".
     */
    public static Cluster repartition(final Cluster currentCluster,
                                      final List<StoreDefinition> currentStoreDefs,
                                      final Cluster interimCluster,
                                      final List<StoreDefinition> finalStoreDefs,
                                      final String outputDir,
                                      final int attempts,
                                      final boolean disableNodeBalancing,
                                      final boolean disableZoneBalancing,
                                      final boolean enableRandomSwaps,
                                      final int randomSwapAttempts,
                                      final int randomSwapSuccesses,
                                      final List<Integer> randomSwapZoneIds,
                                      final boolean enableGreedySwaps,
                                      final List<Integer> greedyZoneIds,
                                      final int greedySwapAttempts,
                                      final int greedySwapMaxPartitionsPerNode,
                                      final int greedySwapMaxPartitionsPerZone,
                                      final int maxContiguousPartitionsPerZone) {
        PartitionBalance partitionBalance = new PartitionBalance(currentCluster, currentStoreDefs);
        RebalanceUtils.dumpAnalysisToFile(outputDir,
                                          RebalanceUtils.currentClusterFileName,
                                          partitionBalance);

        Cluster minCluster = interimCluster;

        double minUtility = Double.MAX_VALUE;

        for(int attempt = 0; attempt < attempts; attempt++) {
            Cluster nextCandidateCluster = interimCluster;

            if(maxContiguousPartitionsPerZone > 0) {
                nextCandidateCluster = repeatedlyBalanceContiguousPartitionsPerZone(nextCandidateCluster,
                                                                                    maxContiguousPartitionsPerZone);
            }

            if(!disableNodeBalancing) {
                nextCandidateCluster = balancePrimaryPartitions(nextCandidateCluster,
                                                                !disableZoneBalancing);
            }

            if(enableRandomSwaps) {
                nextCandidateCluster = randomShufflePartitions(nextCandidateCluster,
                                                               randomSwapAttempts,
                                                               randomSwapSuccesses,
                                                               randomSwapZoneIds,
                                                               finalStoreDefs);
            }
            if(enableGreedySwaps) {
                nextCandidateCluster = greedyShufflePartitions(nextCandidateCluster,
                                                               greedySwapAttempts,
                                                               greedySwapMaxPartitionsPerNode,
                                                               greedySwapMaxPartitionsPerZone,
                                                               new ArrayList<Integer>(interimCluster.getZoneIds()),
                                                               finalStoreDefs);
            }
            RebalanceUtils.validateCurrentFinalCluster(currentCluster, nextCandidateCluster);

            System.out.println("-------------------------\n");
            partitionBalance = new PartitionBalance(nextCandidateCluster, finalStoreDefs);
            double currentUtility = partitionBalance.getUtility();
            System.out.println("Optimization number " + attempt + ": " + currentUtility
                               + " max/min ratio");
            System.out.println("-------------------------\n");
            System.out.println(RebalanceUtils.analyzeInvalidMetadataRate(interimCluster,
                                                                         finalStoreDefs,
                                                                         nextCandidateCluster,
                                                                         finalStoreDefs));

            if(currentUtility <= minUtility) {
                minUtility = currentUtility;
                minCluster = nextCandidateCluster;

                RebalanceUtils.dumpClusterToFile(outputDir, RebalanceUtils.finalClusterFileName
                                                            + attempt, minCluster);
                RebalanceUtils.dumpAnalysisToFile(outputDir, RebalanceUtils.finalClusterFileName
                                                             + attempt, partitionBalance);
            }
            System.out.println("-------------------------\n");
        }

        System.out.println("\n==========================");
        System.out.println("Final distribution");
        partitionBalance = new PartitionBalance(minCluster, finalStoreDefs);
        System.out.println(partitionBalance);

        RebalanceUtils.dumpClusterToFile(outputDir, RebalanceUtils.finalClusterFileName, minCluster);
        RebalanceUtils.dumpAnalysisToFile(outputDir,
                                          RebalanceUtils.finalClusterFileName,
                                          partitionBalance);
        return minCluster;
    }

    /**
     * Determines how many primary partitions each node within each zone should
     * have. The list of integers returned per zone is the same length as the
     * number of nodes in that zone.
     * 
     * @param nextCandidateCluster
     * @param targetPartitionsPerZone
     * @return A map of zoneId to list of target number of partitions per node
     *         within zone.
     */
    public static HashMap<Integer, List<Integer>> getBalancedNumberOfPrimaryPartitionsPerNode(final Cluster nextCandidateCluster,
                                                                                              Map<Integer, Integer> targetPartitionsPerZone) {
        HashMap<Integer, List<Integer>> numPartitionsPerNode = Maps.newHashMap();
        for(Integer zoneId: nextCandidateCluster.getZoneIds()) {
            List<Integer> partitionsOnNode = Utils.distributeEvenlyIntoList(nextCandidateCluster.getNumberOfNodesInZone(zoneId),
                                                                            targetPartitionsPerZone.get(zoneId));
            numPartitionsPerNode.put(zoneId, partitionsOnNode);
        }
        return numPartitionsPerNode;
    }

    /**
     * Assign target number of partitions per node to specific node IDs. Then,
     * separates Nodes into donorNodes and stealerNodes based on whether the
     * node needs to donate or steal primary partitions.
     * 
     * @param nextCandidateCluster
     * @param numPartitionsPerNodePerZone
     * @return a Pair. First element is donorNodes, second element is
     *         stealerNodes. Each element in the pair is a HashMap of Node to
     *         Integer where the integer value is the number of partitions to
     *         store.
     */
    public static Pair<HashMap<Node, Integer>, HashMap<Node, Integer>> getDonorsAndStealersForBalance(final Cluster nextCandidateCluster,
                                                                                                      Map<Integer, List<Integer>> numPartitionsPerNodePerZone) {
        HashMap<Node, Integer> donorNodes = Maps.newHashMap();
        HashMap<Node, Integer> stealerNodes = Maps.newHashMap();

        HashMap<Integer, Integer> numNodesAssignedInZone = Maps.newHashMap();
        for(Integer zoneId: nextCandidateCluster.getZoneIds()) {
            numNodesAssignedInZone.put(zoneId, 0);
        }
        for(Node node: nextCandidateCluster.getNodes()) {
            int zoneId = node.getZoneId();

            int offset = numNodesAssignedInZone.get(zoneId);
            numNodesAssignedInZone.put(zoneId, offset + 1);

            int numPartitions = numPartitionsPerNodePerZone.get(zoneId).get(offset);

            if(numPartitions < node.getNumberOfPartitions()) {
                donorNodes.put(node, numPartitions);
            } else if(numPartitions > node.getNumberOfPartitions()) {
                stealerNodes.put(node, numPartitions);
            }
        }

        // Print out donor/stealer information
        for(Node node: donorNodes.keySet()) {
            System.out.println("Donor Node: " + node.getId() + ", zoneId " + node.getZoneId()
                               + ", numPartitions " + node.getNumberOfPartitions()
                               + ", target number of partitions " + donorNodes.get(node));
        }
        for(Node node: stealerNodes.keySet()) {
            System.out.println("Stealer Node: " + node.getId() + ", zoneId " + node.getZoneId()
                               + ", numPartitions " + node.getNumberOfPartitions()
                               + ", target number of partitions " + stealerNodes.get(node));
        }

        return new Pair<HashMap<Node, Integer>, HashMap<Node, Integer>>(donorNodes, stealerNodes);
    }

    /**
     * This method balances primary partitions among nodes within a zone, and
     * optionally primary partitions among zones. The balancing is done at the
     * level of partitionIds. Such partition Id movement may, or may not, result
     * in data movement during a rebalancing. See RebalancePlan for the object
     * responsible for determining which partition-stores move where for a
     * specific repartitioning.
     * 
     * @param nextCandidateCluster
     * @param balanceZones indicates whether or not number of primary partitions
     *        per zone should be balanced.
     * @return
     */
    public static Cluster balancePrimaryPartitions(final Cluster nextCandidateCluster,
                                                   boolean balanceZones) {
        System.out.println("Balance number of partitions across all nodes and zones.");

        Map<Integer, Integer> targetPartitionsPerZone;
        if(balanceZones) {
            targetPartitionsPerZone = Utils.distributeEvenlyIntoMap(nextCandidateCluster.getZoneIds(),
                                                                    nextCandidateCluster.getNumberOfPartitions());

            System.out.println("numPartitionsPerZone");
            for(int zoneId: nextCandidateCluster.getZoneIds()) {
                System.out.println(zoneId + " : "
                                   + nextCandidateCluster.getNumberOfPartitionsInZone(zoneId)
                                   + " -> " + targetPartitionsPerZone.get(zoneId));
            }
            System.out.println("numNodesPerZone");
            for(int zoneId: nextCandidateCluster.getZoneIds()) {
                System.out.println(zoneId + " : "
                                   + nextCandidateCluster.getNumberOfNodesInZone(zoneId));
            }
        } else {
            // Keep number of partitions per zone the same.
            targetPartitionsPerZone = new HashMap<Integer, Integer>();
            for(int zoneId: nextCandidateCluster.getZoneIds()) {
                targetPartitionsPerZone.put(zoneId,
                                            nextCandidateCluster.getNumberOfPartitionsInZone(zoneId));
            }
        }

        HashMap<Integer, List<Integer>> numPartitionsPerNodeByZone = getBalancedNumberOfPrimaryPartitionsPerNode(nextCandidateCluster,
                                                                                                                 targetPartitionsPerZone);

        Pair<HashMap<Node, Integer>, HashMap<Node, Integer>> donorsAndStealers = getDonorsAndStealersForBalance(nextCandidateCluster,
                                                                                                                numPartitionsPerNodeByZone);
        HashMap<Node, Integer> donorNodes = donorsAndStealers.getFirst();
        List<Node> donorNodeKeys = new ArrayList<Node>(donorNodes.keySet());

        HashMap<Node, Integer> stealerNodes = donorsAndStealers.getSecond();
        List<Node> stealerNodeKeys = new ArrayList<Node>(stealerNodes.keySet());

        /*
         * There is no "intelligence" here about which partition IDs are moved
         * where. The RebalancePlan object owns determining how to move data
         * around to meet a specific repartitioning. That said, a little bit of
         * intelligence here may go a long way. For example, for zone expansion
         * data could be minimized by:
         * 
         * (1) Selecting a minimal # of partition IDs for the new zoneto
         * minimize how much the ring in existing zones is perturbed;
         * 
         * (2) Selecting partitions for the new zone from contiguous runs of
         * partition IDs in other zones that are not currently n-ary partitions
         * for other primary partitions;
         * 
         * (3) Some combination of (1) and (2)...
         */

        // Go over every stealerNode and steal partition Ids from donor nodes
        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);

        Collections.shuffle(stealerNodeKeys, new Random(System.currentTimeMillis()));
        for(Node stealerNode: stealerNodeKeys) {
            int partitionsToSteal = stealerNodes.get(stealerNode)
                                    - stealerNode.getNumberOfPartitions();

            System.out.println("Node (" + stealerNode.getId() + ") in zone ("
                               + stealerNode.getZoneId() + ") has partitionsToSteal of "
                               + partitionsToSteal);

            while(partitionsToSteal > 0) {
                Collections.shuffle(donorNodeKeys, new Random(System.currentTimeMillis()));

                // Repeatedly loop over donor nodes to distribute stealing
                for(Node donorNode: donorNodeKeys) {
                    Node currentDonorNode = returnCluster.getNodeById(donorNode.getId());

                    // Only steal from donor nodes with extra partitions
                    int partitionsToDonate = currentDonorNode.getNumberOfPartitions()
                                             - donorNodes.get(donorNode);
                    if(partitionsToDonate <= 0) {
                        continue;
                    }

                    List<Integer> donorPartitions = Lists.newArrayList(currentDonorNode.getPartitionIds());

                    Collections.shuffle(donorPartitions, new Random(System.currentTimeMillis()));
                    for(int donorPartition: donorPartitions) {
                        Cluster intermediateCluster = RebalanceUtils.createUpdatedCluster(returnCluster,
                                                                                          stealerNode.getId(),
                                                                                          Lists.newArrayList(donorPartition));

                        returnCluster = intermediateCluster;
                        partitionsToSteal--;
                        partitionsToDonate--;
                        System.out.println("Stealer node " + stealerNode.getId() + ", donor node "
                                           + currentDonorNode.getId() + ", partition stolen "
                                           + donorPartition);

                        if(partitionsToSteal == 0 || partitionsToDonate == 0)
                            break;
                    }
                    if(partitionsToSteal == 0)
                        break;
                }
            }
        }

        return returnCluster;
    }

    /**
     * Loops over cluster and repeatedly tries to break up contiguous runs of
     * partitions. After each phase of breaking up contiguous partitions, random
     * partitions are selected to move between zones to balance the number of
     * partitions in each zone. The second phase may re-introduce contiguous
     * partition runs in another zone. Therefore, this overall process is
     * repeated multiple times.
     * 
     * @param nextCandidateCluster
     * @param maxContiguousPartitionsPerZone See RebalanceCLI.
     * @return
     */
    public static Cluster repeatedlyBalanceContiguousPartitionsPerZone(final Cluster nextCandidateCluster,
                                                                       final int maxContiguousPartitionsPerZone) {
        System.out.println("Looping to evenly balance partitions across zones while limiting contiguous partitions");
        // This loop is hard to make definitive. I.e., there are corner cases
        // for small clusters and/or clusters with few partitions for which it
        // may be impossible to achieve tight limits on contiguous run lenghts.
        // Therefore, a constant number of loops are run. Note that once the
        // goal is reached, the loop becomes a no-op.
        int repeatContigBalance = 10;
        Cluster returnCluster = nextCandidateCluster;
        for(int i = 0; i < repeatContigBalance; i++) {
            returnCluster = balanceContiguousPartitionsPerZone(returnCluster,
                                                               maxContiguousPartitionsPerZone);

            returnCluster = balancePrimaryPartitions(returnCluster, false);
            System.out.println("Completed round of balancing contiguous partitions: round "
                               + (i + 1) + " of " + repeatContigBalance);
        }

        return returnCluster;
    }

    /**
     * Ensures that no more than maxContiguousPartitionsPerZone partitions are
     * contiguous within a single zone.
     * 
     * Moves the necessary partitions to break up contiguous runs from each zone
     * to some other random zone/node. There is some chance that such random
     * moves could result in contiguous partitions in other zones.
     * 
     * @param nextCandidateCluster cluster metadata
     * @param maxContiguousPartitionsPerZone See RebalanceCLI.
     * @return Return updated cluster metadata.
     */
    public static Cluster balanceContiguousPartitionsPerZone(final Cluster nextCandidateCluster,
                                                             final int maxContiguousPartitionsPerZone) {
        System.out.println("Balance number of contiguous partitions within a zone.");
        System.out.println("numPartitionsPerZone");
        for(int zoneId: nextCandidateCluster.getZoneIds()) {
            System.out.println(zoneId + " : "
                               + nextCandidateCluster.getNumberOfPartitionsInZone(zoneId));
        }
        System.out.println("numNodesPerZone");
        for(int zoneId: nextCandidateCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + nextCandidateCluster.getNumberOfNodesInZone(zoneId));
        }

        // Break up contiguous partitions within each zone
        HashMap<Integer, List<Integer>> partitionsToRemoveFromZone = Maps.newHashMap();
        System.out.println("Contiguous partitions");
        for(Integer zoneId: nextCandidateCluster.getZoneIds()) {
            System.out.println("\tZone: " + zoneId);
            Map<Integer, Integer> partitionToRunLength = ClusterUtils.getMapOfContiguousPartitions(nextCandidateCluster,
                                                                                                   zoneId);

            List<Integer> partitionsToRemoveFromThisZone = new ArrayList<Integer>();
            for(Map.Entry<Integer, Integer> entry: partitionToRunLength.entrySet()) {
                if(entry.getValue() > maxContiguousPartitionsPerZone) {
                    List<Integer> contiguousPartitions = new ArrayList<Integer>(entry.getValue());
                    for(int partitionId = entry.getKey(); partitionId < entry.getKey()
                                                                        + entry.getValue(); partitionId++) {
                        contiguousPartitions.add(partitionId
                                                 % nextCandidateCluster.getNumberOfPartitions());
                    }
                    System.out.println("Contiguous partitions: " + contiguousPartitions);
                    partitionsToRemoveFromThisZone.addAll(Utils.removeItemsToSplitListEvenly(contiguousPartitions,
                                                                                             maxContiguousPartitionsPerZone));
                }
            }

            partitionsToRemoveFromZone.put(zoneId, partitionsToRemoveFromThisZone);
            System.out.println("\t\tPartitions to remove: " + partitionsToRemoveFromThisZone);
        }

        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);

        Random r = new Random();
        for(int zoneId: returnCluster.getZoneIds()) {
            for(int partitionId: partitionsToRemoveFromZone.get(zoneId)) {
                // Pick a random other zone Id
                List<Integer> otherZoneIds = new ArrayList<Integer>();
                for(int otherZoneId: returnCluster.getZoneIds()) {
                    if(otherZoneId != zoneId) {
                        otherZoneIds.add(otherZoneId);
                    }
                }
                int whichOtherZoneId = otherZoneIds.get(r.nextInt(otherZoneIds.size()));

                // Pick a random node from other zone ID
                int whichNodeOffset = r.nextInt(returnCluster.getNumberOfNodesInZone(whichOtherZoneId));
                int whichNodeId = new ArrayList<Integer>(returnCluster.getNodeIdsInZone(whichOtherZoneId)).get(whichNodeOffset);

                // Steal partition from one zone to another!
                returnCluster = RebalanceUtils.createUpdatedCluster(returnCluster,
                                                                    whichNodeId,
                                                                    Lists.newArrayList(partitionId));
            }
        }

        return returnCluster;
    }

    /**
     * Swaps two specified partitions
     * 
     * @return modified cluster metadata.
     */
    public static Cluster swapPartitions(final Cluster nextCandidateCluster,
                                         final int nodeIdA,
                                         final int partitionIdA,
                                         final int nodeIdB,
                                         final int partitionIdB) {
        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);

        // Swap partitions between nodes!
        returnCluster = RebalanceUtils.createUpdatedCluster(returnCluster,
                                                            nodeIdA,
                                                            Lists.newArrayList(partitionIdB));
        returnCluster = RebalanceUtils.createUpdatedCluster(returnCluster,
                                                            nodeIdB,
                                                            Lists.newArrayList(partitionIdA));

        return returnCluster;
    }

    /**
     * Within a single zone, swaps one random partition on one random node with
     * another random partition on different random node.
     * 
     * @param nextCandidateCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @return
     */
    public static Cluster swapRandomPartitionsWithinZone(final Cluster nextCandidateCluster,
                                                         final int zoneId) {
        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);
        Random r = new Random();

        List<Integer> nodeIdsInZone = new ArrayList<Integer>(nextCandidateCluster.getNodeIdsInZone(zoneId));

        if(nodeIdsInZone.size() == 0) {
            return returnCluster;
        }

        // Select random stealer node
        int stealerNodeOffset = r.nextInt(nodeIdsInZone.size());
        Integer stealerNodeId = nodeIdsInZone.get(stealerNodeOffset);

        // Select random stealer partition
        List<Integer> stealerPartitions = returnCluster.getNodeById(stealerNodeId)
                                                       .getPartitionIds();
        if(stealerPartitions.size() == 0) {
            return nextCandidateCluster;
        }
        int stealerPartitionOffset = r.nextInt(stealerPartitions.size());
        int stealerPartitionId = stealerPartitions.get(stealerPartitionOffset);

        // Select random donor node
        List<Integer> donorNodeIds = new ArrayList<Integer>();
        donorNodeIds.addAll(nodeIdsInZone);
        donorNodeIds.remove(stealerNodeId);

        if(donorNodeIds.isEmpty()) { // No donor nodes!
            return returnCluster;
        }
        int donorIdOffset = r.nextInt(donorNodeIds.size());
        Integer donorNodeId = donorNodeIds.get(donorIdOffset);

        // Select random donor partition
        List<Integer> donorPartitions = returnCluster.getNodeById(donorNodeId).getPartitionIds();
        int donorPartitionOffset = r.nextInt(donorPartitions.size());
        int donorPartitionId = donorPartitions.get(donorPartitionOffset);

        return swapPartitions(returnCluster,
                              stealerNodeId,
                              stealerPartitionId,
                              donorNodeId,
                              donorPartitionId);
    }

    /**
     * Randomly shuffle partitions between nodes within every zone.
     * 
     * @param nextCandidateCluster cluster object.
     * @param randomSwapAttempts See RebalanceCLI.
     * @param randomSwapSuccesses See RebalanceCLI.
     * @param storeDefs List of store definitions
     * @return updated cluster
     */
    public static Cluster randomShufflePartitions(final Cluster nextCandidateCluster,
                                                  final int randomSwapAttempts,
                                                  final int randomSwapSuccesses,
                                                  final List<Integer> randomSwapZoneIds,
                                                  List<StoreDefinition> storeDefs) {

        List<Integer> zoneIds = null;
        if (randomSwapZoneIds.isEmpty()) {
            zoneIds = new ArrayList<Integer>(nextCandidateCluster.getZoneIds());
        } else {
            zoneIds = new ArrayList<Integer>(randomSwapZoneIds);
        }

        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);
        double currentUtility = new PartitionBalance(returnCluster, storeDefs).getUtility();

        int successes = 0;
        for(int i = 0; i < randomSwapAttempts; i++) {
            Collections.shuffle(zoneIds, new Random(System.currentTimeMillis()));
            for(Integer zoneId: zoneIds) {
                Cluster shuffleResults = swapRandomPartitionsWithinZone(returnCluster, zoneId);
                double nextUtility = new PartitionBalance(shuffleResults, storeDefs).getUtility();
                if(nextUtility < currentUtility) {
                    System.out.println("Swap improved max-min ratio: " + currentUtility + " -> "
                                       + nextUtility + " (improvement " + successes
                                       + " on swap attempt " + i + " in zone " + zoneId + ")");
                    successes++;
                    returnCluster = shuffleResults;
                    currentUtility = nextUtility;
                }
            }
            if(successes >= randomSwapSuccesses) {
                // Enough successes, move on.
                break;
            }
        }

        return returnCluster;
    }

    /**
     * For each node in specified zones, tries swapping some minimum number of
     * random partitions per node with some minimum number of random partitions
     * from other specified nodes. Chooses the best swap in each iteration.
     * Large values of the greedSwapMaxPartitions... arguments make this method
     * equivalent to comparing every possible swap. This may get very expensive.
     * 
     * @param nextCandidateCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @param greedySwapMaxPartitionsPerNode See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerZone See RebalanceCLI.
     * @param storeDefs
     * @return
     */
    public static Cluster swapGreedyRandomPartitions(final Cluster nextCandidateCluster,
                                                     final List<Integer> nodeIds,
                                                     final int greedySwapMaxPartitionsPerNode,
                                                     final int greedySwapMaxPartitionsPerZone,
                                                     List<StoreDefinition> storeDefs) {
        System.out.println("GreedyRandom : nodeIds:" + nodeIds);

        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);
        double currentUtility = new PartitionBalance(returnCluster, storeDefs).getUtility();
        int nodeIdA = -1;
        int nodeIdB = -1;
        int partitionIdA = -1;
        int partitionIdB = -1;

        for(int nodeIdEh: nodeIds) {
            System.out.println("GreedyRandom : processing nodeId:" + nodeIdEh);
            List<Integer> partitionIdsEh = new ArrayList<Integer>();
            partitionIdsEh.addAll(returnCluster.getNodeById(nodeIdEh).getPartitionIds());
            Collections.shuffle(partitionIdsEh);

            int maxPartitionsInEh = Math.min(greedySwapMaxPartitionsPerNode, partitionIdsEh.size());
            for(int offsetEh = 0; offsetEh < maxPartitionsInEh; ++offsetEh) {
                Integer partitionIdEh = partitionIdsEh.get(offsetEh);

                List<Pair<Integer, Integer>> partitionIdsZone = new ArrayList<Pair<Integer, Integer>>();
                for(int nodeIdBee: nodeIds) {
                    if(nodeIdBee == nodeIdEh)
                        continue;
                    for(Integer partitionIdBee: returnCluster.getNodeById(nodeIdBee)
                                                             .getPartitionIds()) {
                        partitionIdsZone.add(new Pair<Integer, Integer>(nodeIdBee, partitionIdBee));
                    }
                }

                Collections.shuffle(partitionIdsZone);
                int maxPartitionsInZone = Math.min(greedySwapMaxPartitionsPerZone,
                                                   partitionIdsZone.size());
                for(int offsetZone = 0; offsetZone < maxPartitionsInZone; offsetZone++) {
                    Integer nodeIdBee = partitionIdsZone.get(offsetZone).getFirst();
                    Integer partitionIdBee = partitionIdsZone.get(offsetZone).getSecond();
                    Cluster swapResult = swapPartitions(returnCluster,
                                                        nodeIdEh,
                                                        partitionIdEh,
                                                        nodeIdBee,
                                                        partitionIdBee);
                    double swapUtility = new PartitionBalance(swapResult, storeDefs).getUtility();
                    if(swapUtility < currentUtility) {
                        currentUtility = swapUtility;
                        System.out.println(" -> " + currentUtility);
                        nodeIdA = nodeIdEh;
                        partitionIdA = partitionIdEh;
                        nodeIdB = nodeIdBee;
                        partitionIdB = partitionIdBee;
                    }
                }
            }
        }

        if(nodeIdA == -1) {
            return returnCluster;
        }
        return swapPartitions(returnCluster, nodeIdA, partitionIdA, nodeIdB, partitionIdB);
    }

    /**
     * Within a single zone, tries swapping some minimum number of random
     * partitions per node with some minimum number of random partitions from
     * other nodes within the zone. Chooses the best swap in each iteration.
     * Large values of the greedSwapMaxPartitions... arguments make this method
     * equivalent to comparing every possible swap. This is very expensive.
     * 
     * Normal case should be :
     * 
     * #zones X #nodes/zone X max partitions/node X max partitions/zone
     * 
     * @param nextCandidateCluster cluster object.
     * @param greedyAttempts See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerNode See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerZone See RebalanceCLI.
     * @param zoneIds The set of zoneIds to do. Each zone is done independently.
     *        null will consider all nodes in entire cluster at once.
     * @param storeDefs
     * @return
     */
    public static Cluster greedyShufflePartitions(final Cluster nextCandidateCluster,
                                                  final int greedyAttempts,
                                                  final int greedySwapMaxPartitionsPerNode,
                                                  final int greedySwapMaxPartitionsPerZone,
                                                  List<Integer> zoneIds,
                                                  List<StoreDefinition> storeDefs) {
        final int specialZoneId = -1;
        if(zoneIds == null) {
            zoneIds = new ArrayList<Integer>();
            zoneIds.add(specialZoneId);
        }
        Cluster returnCluster = ClusterUtils.copyCluster(nextCandidateCluster);
        if(zoneIds.isEmpty()) {
            logger.warn("greedyShufflePartitions invoked with empty list of zone IDs.");
            return returnCluster;
        }

        double currentUtility = new PartitionBalance(returnCluster, storeDefs).getUtility();

        for(int i = 0; i < greedyAttempts; i++) {
            Collections.shuffle(zoneIds, new Random(System.currentTimeMillis()));
            for(Integer zoneId: zoneIds) {
                System.out.println("Greedy swap attempt: zone " + zoneId + " , attempt " + i
                                   + " of " + greedyAttempts);

                List<Integer> nodeIds;
                if(zoneId == specialZoneId) {
                    nodeIds = new ArrayList<Integer>(nextCandidateCluster.getNodeIds());
                } else {
                    nodeIds = new ArrayList<Integer>(nextCandidateCluster.getNodeIdsInZone(zoneId));
                }

                Cluster shuffleResults = swapGreedyRandomPartitions(returnCluster,
                                                                    nodeIds,
                                                                    greedySwapMaxPartitionsPerNode,
                                                                    greedySwapMaxPartitionsPerZone,
                                                                    storeDefs);
                double nextUtility = new PartitionBalance(shuffleResults, storeDefs).getUtility();

                if(nextUtility == currentUtility) {
                    System.out.println("Not improving for zone: " + zoneId);
                } else {
                    System.out.println("Swap improved max-min ratio: " + currentUtility + " -> "
                                       + nextUtility + " (swap attempt " + i + " in zone " + zoneId
                                       + ")");

                    returnCluster = shuffleResults;
                    currentUtility = nextUtility;
                }
            }
        }

        return returnCluster;
    }

}
