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

package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * RepartitionUtils provides functions that balance the distribution of
 * partitions across a cluster.
 * 
 */
public class RepartitionUtils {

    private static Logger logger = Logger.getLogger(RepartitionUtils.class);

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
     * <li>Rebalancing : Distribute partition IDs better for an existing
     * cluster.
     * <li>Cluster expansion : Distribute partition IDs to take advantage of new
     * nodes (added to some of the zones).
     * <li>Zone expansion : Distribute partition IDs into a new zone.
     * </ul>
     * 
     * @param currentCluster current cluster
     * @param currentStoreDefs current store defs
     * @param targetCluster target cluster; needed for cluster or zone
     *        expansion, otherwise pass in same as currentCluster.
     * @param targetStoreDefs target store defs; needed for zone expansion,
     *        otherwise pass in same as currentStores.
     * @param outputDir
     * @param attempts
     * @param disableNodeBalancing
     * @param disableZoneBalancing
     * @param enableRandomSwaps
     * @param randomSwapAttempts
     * @param randomSwapSuccesses
     * @param enableGreedySwaps
     * @param greedyZoneIds
     * @param greedySwapAttempts
     * @param greedySwapMaxPartitionsPerNode
     * @param greedySwapMaxPartitionsPerZone
     * @param maxContiguousPartitionsPerZone
     * @return Cluster that has had all specified balancing algorithms run
     *         against it. The number of zones and number of nodes will match
     *         that of the specified targetCluster.
     */
    public static Cluster repartition(final Cluster currentCluster,
                                      final List<StoreDefinition> currentStoreDefs,
                                      final Cluster targetCluster,
                                      final List<StoreDefinition> targetStoreDefs,
                                      final String outputDir,
                                      final int attempts,
                                      final boolean disableNodeBalancing,
                                      final boolean disableZoneBalancing,
                                      final boolean enableRandomSwaps,
                                      final int randomSwapAttempts,
                                      final int randomSwapSuccesses,
                                      final boolean enableGreedySwaps,
                                      final List<Integer> greedyZoneIds,
                                      final int greedySwapAttempts,
                                      final int greedySwapMaxPartitionsPerNode,
                                      final int greedySwapMaxPartitionsPerZone,
                                      final int maxContiguousPartitionsPerZone) {
        PartitionBalance partitionBalance = new ClusterInstance(currentCluster, currentStoreDefs).getPartitionBalance();
        dumpAnalysisToFile(outputDir,
                           RebalanceUtils.initialClusterFileName + ".analysis",
                           partitionBalance.toString());

        Cluster minCluster = targetCluster;

        double minUtility = Double.MAX_VALUE;

        for(int attempt = 0; attempt < attempts; attempt++) {
            Cluster nextCluster = targetCluster;

            if(maxContiguousPartitionsPerZone > 0) {
                nextCluster = repeatedlyBalanceContiguousPartitionsPerZone(nextCluster,
                                                                           maxContiguousPartitionsPerZone);
            }

            if(!disableNodeBalancing) {
                nextCluster = balancePrimaryPartitions(nextCluster, !disableZoneBalancing);
            }

            if(enableRandomSwaps) {
                nextCluster = randomShufflePartitions(nextCluster,
                                                      randomSwapAttempts,
                                                      randomSwapSuccesses,
                                                      targetStoreDefs);
            }
            if(enableGreedySwaps) {
                nextCluster = greedyShufflePartitions(nextCluster,
                                                      greedySwapAttempts,
                                                      greedySwapMaxPartitionsPerNode,
                                                      greedySwapMaxPartitionsPerZone,
                                                      new ArrayList<Integer>(targetCluster.getZoneIds()),
                                                      targetStoreDefs);
            }

            if(!validateClusterUpdate(currentCluster, nextCluster)) {
                System.err.println("The modified cluster does not pass validation. Reverting to initial cluster...");
                nextCluster = currentCluster;
            }

            System.out.println("-------------------------\n");
            partitionBalance = new ClusterInstance(nextCluster, targetStoreDefs).getPartitionBalance();
            double currentUtility = partitionBalance.getUtility();
            System.out.println("Optimization number " + attempt + ": " + currentUtility
                               + " max/min ratio");

            if(currentUtility <= minUtility) {
                minUtility = currentUtility;
                minCluster = nextCluster;

                dumpClusterToFile(outputDir,
                                  RebalanceUtils.finalClusterFileName + attempt,
                                  minCluster);
                dumpAnalysisToFile(outputDir, RebalanceUtils.finalClusterFileName + attempt
                                              + ".analysis", partitionBalance.toString());
            }
            System.out.println("-------------------------\n");
        }

        System.out.println("\n==========================");
        System.out.println("Final distribution");
        partitionBalance = new ClusterInstance(minCluster, targetStoreDefs).getPartitionBalance();
        System.out.println(partitionBalance);

        dumpClusterToFile(outputDir, RebalanceUtils.finalClusterFileName, minCluster);
        dumpAnalysisToFile(outputDir,
                           RebalanceUtils.finalClusterFileName + ".analysis",
                           partitionBalance.toString());
        return minCluster;
    }

    /**
     * Determines how many primary partitions each node within each zone should
     * have. The list of integers returned per zone is the same length as the
     * number of nodes in that zone.
     * 
     * @param targetCluster
     * @param targetPartitionsPerZone
     * @return A map of zoneId to list of target number of partitions per node
     *         within zone.
     */
    public static HashMap<Integer, List<Integer>> getBalancedNumberOfPrimaryPartitionsPerNode(final Cluster targetCluster,
                                                                                              Map<Integer, Integer> targetPartitionsPerZone) {
        HashMap<Integer, List<Integer>> numPartitionsPerNode = Maps.newHashMap();
        for(Integer zoneId: targetCluster.getZoneIds()) {
            List<Integer> partitionsOnNode = peanutButterList(targetCluster.getNumberOfNodesInZone(zoneId),
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
     * @param targetCluster
     * @param numPartitionsPerNodePerZone
     * @return a Pair. First element is donorNodes, second element is
     *         stealerNodes. Each element in the pair is a HashMap of Node to
     *         Integer where the integer value is the number of partitions to
     *         store.
     */
    public static Pair<HashMap<Node, Integer>, HashMap<Node, Integer>> getDonorsAndStealersForBalance(final Cluster targetCluster,
                                                                                                      Map<Integer, List<Integer>> numPartitionsPerNodePerZone) {
        HashMap<Node, Integer> donorNodes = Maps.newHashMap();
        HashMap<Node, Integer> stealerNodes = Maps.newHashMap();

        HashMap<Integer, Integer> numNodesAssignedInZone = Maps.newHashMap();
        for(Integer zoneId: targetCluster.getZoneIds()) {
            numNodesAssignedInZone.put(zoneId, 0);
        }
        for(Node node: targetCluster.getNodes()) {
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
     * 
     * @param targetCluster
     * @param balanceZones indicates whether or not number of primary partitions
     *        per zone should be balanced.
     * @return
     */
    public static Cluster balancePrimaryPartitions(final Cluster targetCluster, boolean balanceZones) {
        System.out.println("Balance number of partitions across all nodes and zones.");

        Map<Integer, Integer> targetPartitionsPerZone;
        if(balanceZones) {
            targetPartitionsPerZone = peanutButterMap(targetCluster.getZoneIds(),
                                                      targetCluster.getNumberOfPartitions());

            System.out.println("numPartitionsPerZone");
            for(int zoneId: targetCluster.getZoneIds()) {
                System.out.println(zoneId + " : "
                                   + targetCluster.getNumberOfPartitionsInZone(zoneId) + " -> "
                                   + targetPartitionsPerZone.get(zoneId));
            }
            System.out.println("numNodesPerZone");
            for(int zoneId: targetCluster.getZoneIds()) {
                System.out.println(zoneId + " : " + targetCluster.getNumberOfNodesInZone(zoneId));
            }
        } else {
            // Keep number of partitions per zone the same.
            targetPartitionsPerZone = new HashMap<Integer, Integer>();
            for(int zoneId: targetCluster.getZoneIds()) {
                targetPartitionsPerZone.put(zoneId,
                                            targetCluster.getNumberOfPartitionsInZone(zoneId));
            }
        }

        HashMap<Integer, List<Integer>> numPartitionsPerNodeByZone = getBalancedNumberOfPrimaryPartitionsPerNode(targetCluster,
                                                                                                                 targetPartitionsPerZone);

        Pair<HashMap<Node, Integer>, HashMap<Node, Integer>> donorsAndStealers = getDonorsAndStealersForBalance(targetCluster,
                                                                                                                numPartitionsPerNodeByZone);
        HashMap<Node, Integer> donorNodes = donorsAndStealers.getFirst();
        List<Node> donorNodeKeys = new ArrayList<Node>(donorNodes.keySet());

        HashMap<Node, Integer> stealerNodes = donorsAndStealers.getSecond();
        List<Node> stealerNodeKeys = new ArrayList<Node>(stealerNodes.keySet());

        // Go over every stealerNode and steal partitions from donor nodes
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);

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
     * @param nextCluster
     * @param maxContiguousPartitionsPerZone See RebalanceCLI.
     * @return
     */
    public static Cluster repeatedlyBalanceContiguousPartitionsPerZone(final Cluster targetCluster,
                                                                       final int maxContiguousPartitionsPerZone) {
        System.out.println("Looping to evenly balance partitions across zones while limiting contiguous partitions");
        // This loop is hard to make definitive. I.e., there are corner cases
        // for small clusters and/or clusters with few partitions for which it
        // may be impossible to achieve tight limits on contiguous run lenghts.
        // Therefore, a constant number of loops are run. Note that once the
        // goal is reached, the loop becomes a no-op.
        int repeatContigBalance = 10;
        Cluster nextCluster = targetCluster;
        for(int i = 0; i < repeatContigBalance; i++) {
            nextCluster = balanceContiguousPartitionsPerZone(nextCluster,
                                                             maxContiguousPartitionsPerZone);

            nextCluster = balancePrimaryPartitions(nextCluster, false);
            System.out.println("Completed round of balancing contiguous partitions: round "
                               + (i + 1) + " of " + repeatContigBalance);
        }

        return nextCluster;
    }

    /**
     * Ensures that no more than maxContiguousPartitionsPerZone partitions are
     * contiguous within a single zone.
     * 
     * Moves the necessary partitions to break up contiguous runs from each zone
     * to some other random zone/node. There is some chance that such random
     * moves could result in contiguous partitions in other zones.
     * 
     * @param targetCluster Target cluster metadata
     * @param maxContiguousPartitionsPerZone See RebalanceCLI.
     * @return Return a pair of cluster metadata and number of primary
     *         partitions that have moved.
     */
    public static Cluster balanceContiguousPartitionsPerZone(final Cluster targetCluster,
                                                             final int maxContiguousPartitionsPerZone) {
        System.out.println("Balance number of contiguous partitions within a zone.");
        System.out.println("numPartitionsPerZone");
        for(int zoneId: targetCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + targetCluster.getNumberOfPartitionsInZone(zoneId));
        }
        System.out.println("numNodesPerZone");
        for(int zoneId: targetCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + targetCluster.getNumberOfNodesInZone(zoneId));
        }

        // Break up contiguous partitions within each zone
        HashMap<Integer, List<Integer>> partitionsToRemoveFromZone = Maps.newHashMap();
        System.out.println("Contiguous partitions");
        for(Integer zoneId: targetCluster.getZoneIds()) {
            System.out.println("\tZone: " + zoneId);
            Map<Integer, Integer> partitionToRunLength = ClusterUtils.getMapOfContiguousPartitions(targetCluster,
                                                                                                   zoneId);

            List<Integer> partitionsToRemoveFromThisZone = new ArrayList<Integer>();
            for(Map.Entry<Integer, Integer> entry: partitionToRunLength.entrySet()) {
                if(entry.getValue() > maxContiguousPartitionsPerZone) {
                    List<Integer> contiguousPartitions = new ArrayList<Integer>(entry.getValue());
                    for(int partitionId = entry.getKey(); partitionId < entry.getKey()
                                                                        + entry.getValue(); partitionId++) {
                        contiguousPartitions.add(partitionId
                                                 % targetCluster.getNumberOfPartitions());
                    }
                    System.out.println("Contiguous partitions: " + contiguousPartitions);
                    partitionsToRemoveFromThisZone.addAll(removeItemsToSplitListEvenly(contiguousPartitions,
                                                                                       maxContiguousPartitionsPerZone));
                }
            }

            partitionsToRemoveFromZone.put(zoneId, partitionsToRemoveFromThisZone);
            System.out.println("\t\tPartitions to remove: " + partitionsToRemoveFromThisZone);
        }

        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);

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
    public static Cluster swapPartitions(final Cluster targetCluster,
                                         final int nodeIdA,
                                         final int partitionIdA,
                                         final int nodeIdB,
                                         final int partitionIdB) {
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);

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
     * @param targetCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @return
     */
    public static Cluster swapRandomPartitionsWithinZone(final Cluster targetCluster,
                                                         final int zoneId) {
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        Random r = new Random();

        List<Integer> nodeIdsInZone = new ArrayList<Integer>(targetCluster.getNodeIdsInZone(zoneId));

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
            return targetCluster;
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
     * @param targetCluster Target cluster object.
     * @param randomSwapAttempts See RebalanceCLI.
     * @param randomSwapSuccesses See RebalanceCLI.
     * @param storeDefs List of store definitions
     * @return
     */
    public static Cluster randomShufflePartitions(final Cluster targetCluster,
                                                  final int randomSwapAttempts,
                                                  final int randomSwapSuccesses,
                                                  List<StoreDefinition> storeDefs) {
        List<Integer> zoneIds = new ArrayList<Integer>(targetCluster.getZoneIds());
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);

        double currentUtility = new ClusterInstance(returnCluster, storeDefs).getPartitionBalance()
                                                                             .getUtility();

        int successes = 0;
        for(int i = 0; i < randomSwapAttempts; i++) {
            Collections.shuffle(zoneIds, new Random(System.currentTimeMillis()));
            for(Integer zoneId: zoneIds) {
                Cluster shuffleResults = swapRandomPartitionsWithinZone(returnCluster, zoneId);
                double nextUtility = new ClusterInstance(shuffleResults, storeDefs).getPartitionBalance()
                                                                                   .getUtility();
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
     * @param targetCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @param greedySwapMaxPartitionsPerNode See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerZone See RebalanceCLI.
     * @param storeDefs
     * @return
     */
    public static Cluster swapGreedyRandomPartitions(final Cluster targetCluster,
                                                     final List<Integer> nodeIds,
                                                     final int greedySwapMaxPartitionsPerNode,
                                                     final int greedySwapMaxPartitionsPerZone,
                                                     List<StoreDefinition> storeDefs) {
        System.out.println("GreedyRandom : nodeIds:" + nodeIds);

        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        double currentUtility = new ClusterInstance(returnCluster, storeDefs).getPartitionBalance()
                                                                             .getUtility();
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
                    double swapUtility = new ClusterInstance(swapResult, storeDefs).getPartitionBalance()
                                                                                   .getUtility();
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
     * @param targetCluster Target cluster object.
     * @param greedyAttempts See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerNode See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerZone See RebalanceCLI.
     * @param zoneIds The set of zoneIds to do. Each zone is done independently.
     *        null will consider all nodes in entire cluster at once.
     * @param storeDefs
     * @return
     */
    /**
     * 
     * @param targetCluster
     * @param greedyAttempts
     * @param greedySwapMaxPartitionsPerNode
     * @param greedySwapMaxPartitionsPerZone
     * @param zoneIds
     * @param storeDefs
     * @return
     */
    public static Cluster greedyShufflePartitions(final Cluster targetCluster,
                                                  final int greedyAttempts,
                                                  final int greedySwapMaxPartitionsPerNode,
                                                  final int greedySwapMaxPartitionsPerZone,
                                                  List<Integer> zoneIds,
                                                  List<StoreDefinition> storeDefs) {
        final int specialZoneId = -1;
        if(zoneIds == null) {
            zoneIds = new ArrayList<Integer>();
            zoneIds.add(specialZoneId); // Special value.
        }
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        if(zoneIds.isEmpty()) {
            logger.warn("greedyShufflePartitions invoked with empty list of zone IDs.");
            return returnCluster;
        }

        double currentUtility = new ClusterInstance(returnCluster, storeDefs).getPartitionBalance()
                                                                             .getUtility();

        for(int i = 0; i < greedyAttempts; i++) {
            Collections.shuffle(zoneIds, new Random(System.currentTimeMillis()));
            for(Integer zoneId: zoneIds) {
                System.out.println("Greedy swap attempt: zone " + zoneId + " , attempt " + i
                                   + " of " + greedyAttempts);

                List<Integer> nodeIds;
                if(zoneId == specialZoneId) {
                    nodeIds = new ArrayList<Integer>(targetCluster.getNodeIds());
                } else {
                    nodeIds = new ArrayList<Integer>(targetCluster.getNodeIdsInZone(zoneId));
                }

                Cluster shuffleResults = swapGreedyRandomPartitions(returnCluster,
                                                                    nodeIds,
                                                                    greedySwapMaxPartitionsPerNode,
                                                                    greedySwapMaxPartitionsPerZone,
                                                                    storeDefs);
                double nextUtility = new ClusterInstance(shuffleResults, storeDefs).getPartitionBalance()
                                                                                   .getUtility();

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

    /**
     * Validate that two cluster metadata instances are consistent with one
     * another. I.e., that they have the same number of partitions. Note that
     * the Cluster object does additional verification upon construction (e.g.,
     * that partitions are numbered consecutively) and so there is no risk of
     * duplicate partitions.
     * 
     * @param before cluster metadata before any changes
     * @param after cluster metadata after any changes
     * @return false if the 'after' metadata is not consistent with the 'before'
     *         metadata
     */
    public static boolean validateClusterUpdate(final Cluster before, final Cluster after) {
        if(before.getNumberOfPartitions() != after.getNumberOfPartitions()) {
            return false;
        }
        return true;
    }

    // TODO: Move to some more generic util class?
    /**
     * This method breaks the inputList into distinct lists that are no longer
     * than maxContiguous in length. It does so by removing elements from the
     * inputList. This method removes the minimum necessary items to achieve the
     * goal. This method chooses items to remove that minimize the length of the
     * maximum remaining run. E.g. given an inputList of 20 elements and
     * maxContiguous=8, this method will return the 2 elements that break the
     * inputList into 3 runs of 6 items. (As opposed to 2 elements that break
     * the inputList into two runs of eight items and one run of two items.)
     * 
     * @param inputList The list to be broken into separate runs.
     * @param maxContiguous The upper limit on sub-list size
     * @return A list of Integers to be removed from inputList to achieve the
     *         maxContiguous goal.
     */
    public static List<Integer> removeItemsToSplitListEvenly(final List<Integer> inputList,
                                                             int maxContiguous) {
        List<Integer> itemsToRemove = new ArrayList<Integer>();
        int contiguousCount = inputList.size();
        if(contiguousCount > maxContiguous) {
            // Determine how many items must be removed to ensure no contig run
            // longer than maxContiguous
            int numToRemove = contiguousCount / (maxContiguous + 1);
            // Breaking in numToRemove places results in numToRemove+1 runs.
            int numRuns = numToRemove + 1;
            // Num items left to break into numRuns
            int numItemsLeft = contiguousCount - numToRemove;
            // Determine minimum length of each run after items are removed.
            int floorOfEachRun = numItemsLeft / numRuns;
            // Determine how many runs need one extra element to evenly
            // distribute numItemsLeft among all numRuns
            int numOfRunsWithExtra = numItemsLeft - (floorOfEachRun * numRuns);

            int offset = 0;
            for(int i = 0; i < numToRemove; ++i) {
                offset += floorOfEachRun;
                if(i < numOfRunsWithExtra)
                    offset++;
                itemsToRemove.add(inputList.get(offset));
                offset++;
            }
        }
        return itemsToRemove;
    }

    // TODO: Move to some more generic util class?
    /**
     * This method returns a list that "evenly" (within one) distributes some
     * number of elements (peanut butter) over some number of buckets (bread
     * slices).
     * 
     * @param breadSlices The number of buckets over which to evenly distribute
     *        the elements.
     * @param peanutButter The number of elements to distribute.
     * @return A list of size breadSlices each integer entry of which indicates
     *         the number of elements
     */
    public static List<Integer> peanutButterList(int breadSlices, int peanutButter) {
        if(breadSlices < 1) {
            throw new IllegalArgumentException("Argument breadSlices must be greater than 0 : "
                                               + breadSlices);
        }
        if(peanutButter < 0) {
            throw new IllegalArgumentException("Argument peanutButter must be zero or more : "
                                               + peanutButter);
        }
        int floorPB = peanutButter / breadSlices;
        int breadWithMorePB = peanutButter - (breadSlices * floorPB);

        ArrayList<Integer> pbList = new ArrayList<Integer>(breadSlices);
        for(int i = 0; i < breadWithMorePB; i++) {
            pbList.add(i, floorPB + 1);
        }
        for(int i = breadWithMorePB; i < breadSlices; i++) {
            pbList.add(i, floorPB);
        }
        return pbList;
    }

    // TODO: Move to some more generic util class?
    /**
     * This method returns a map that "evenly" (within one) distributes some
     * number of elements (peanut butter) over some number of buckets (bread
     * slices).
     * 
     * @param set The collection of objects over which which to evenly
     *        distribute the elements.
     * @param peanutButter The number of elements to distribute.
     * @return A Map with keys specified by breadSlices each integer entry of
     *         which indicates the number of elements
     */
    public static Map<Integer, Integer> peanutButterMap(Set<Integer> breadSlices, int peanutButter) {
        Map<Integer, Integer> pbMap = new HashMap<Integer, Integer>();
        List<Integer> pbList = peanutButterList(breadSlices.size(), peanutButter);
        int offset = 0;
        for(Integer breadSlice: breadSlices) {
            pbMap.put(breadSlice, pbList.get(offset));
            offset++;
        }
        return pbMap;
    }

    public static void dumpClusterToFile(String outputDir, String fileName, Cluster cluster) {
        if(outputDir != null) {
            try {
                FileUtils.writeStringToFile(new File(outputDir, fileName),
                                            new ClusterMapper().writeCluster(cluster));
            } catch(Exception e) {}
        }
    }

    public static void dumpAnalysisToFile(String outputDir, String fileName, String analysis) {
        if(outputDir != null) {
            try {
                FileUtils.writeStringToFile(new File(outputDir, fileName), analysis);
            } catch(Exception e) {}
        }
    }

}
