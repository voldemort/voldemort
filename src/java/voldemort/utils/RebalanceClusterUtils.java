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
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * RebalanceClusterUtils provides functions that balance the distribution of
 * partitions across a cluster.
 * 
 */
public class RebalanceClusterUtils {

    // TODO: (refactor) Improve upon the name "RebalanceClusterUtils". All of
    // these util methods support moving partitions around a cluster to achieve
    // better balance.

    private static Logger logger = Logger.getLogger(RebalanceClusterUtils.class);

    /**
     * Outputs an optimized cluster based on the existing cluster and the new
     * nodes that are being added.
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster The target cluster metadata which contains the nodes
     *        of the current cluster + new nodes with empty partitions
     * @param storeDefs List of store definitions
     * @param outputDir The output directory where we'll store the cluster
     *        metadata ( if not null )
     * @param maxTriesRebalancing See RebalanceCLI.
     * @param generateEnableXzonePrimary See RebalanceCLI.
     * @param generateEnableXzoneNary See RebalanceCLI.
     * @param enableRandomSwaps See RebalanceCLI.
     * @param randomSwapAttempts See RebalanceCLI.
     * @param randomSwapSuccesses See RebalanceCLI.
     * @param enableGreedySwaps See RebalanceCLI.
     * @param greedySwapAttempts See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerNode See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerZone See RebalanceCLI.
     * @param maxContiguousPartitionsPerZone See RebalanceCLI.
     */
    public static void balanceTargetCluster(final Cluster currentCluster,
                                            final Cluster targetCluster,
                                            final List<StoreDefinition> storeDefs,
                                            final String outputDir,
                                            final int maxTriesRebalancing,
                                            final boolean generateDisablePrimaryBalancing,
                                            final boolean generateEnableXzonePrimary,
                                            final boolean generateEnableAnyXzoneNary,
                                            final boolean generateEnableLastResortXzoneNary,
                                            final boolean enableXzoneShuffle,
                                            final boolean enableRandomSwaps,
                                            final int randomSwapAttempts,
                                            final int randomSwapSuccesses,
                                            final boolean enableGreedySwaps,
                                            final int greedySwapAttempts,
                                            final int greedySwapMaxPartitionsPerNode,
                                            final int greedySwapMaxPartitionsPerZone,
                                            final int maxContiguousPartitionsPerZone) {
        Pair<Double, String> analysis = new ClusterInstance(currentCluster, storeDefs).analyzeBalanceVerbose();
        dumpAnalysisToFile(outputDir,
                           RebalanceUtils.initialClusterFileName + ".analysis",
                           analysis.getSecond());

        Cluster minCluster = targetCluster;
        double minMaxMinRatio = Double.MAX_VALUE;

        for(int numTries = 0; numTries < maxTriesRebalancing; numTries++) {
            Cluster nextCluster = targetCluster;

            if(maxContiguousPartitionsPerZone > 0) {
                nextCluster = repeatedlyBalanceContiguousPartitionsPerZone(nextCluster,
                                                                           maxContiguousPartitionsPerZone);
            }

            if(!generateDisablePrimaryBalancing) {
                nextCluster = balancePrimaryPartitionsPerNode(nextCluster,
                                                              storeDefs,
                                                              generateEnableXzonePrimary,
                                                              generateEnableAnyXzoneNary,
                                                              generateEnableLastResortXzoneNary);
            }

            if(enableRandomSwaps) {
                nextCluster = randomShufflePartitions(nextCluster,
                                                      enableXzoneShuffle,
                                                      randomSwapAttempts,
                                                      randomSwapSuccesses,
                                                      storeDefs);
            }
            if(enableGreedySwaps) {
                nextCluster = greedyShufflePartitions(nextCluster,
                                                      enableXzoneShuffle,
                                                      greedySwapAttempts,
                                                      greedySwapMaxPartitionsPerNode,
                                                      greedySwapMaxPartitionsPerZone,
                                                      storeDefs);
            }

            if(!validateClusterUpdate(currentCluster, nextCluster)) {
                System.err.println("The modified cluster does not pass validation. Reverting to initial cluster...");
                nextCluster = currentCluster;
            }

            System.out.println("-------------------------\n");
            analysis = new ClusterInstance(nextCluster, storeDefs).analyzeBalanceVerbose();
            double currentMaxMinRatio = analysis.getFirst();
            System.out.println("Optimization number " + numTries + ": " + currentMaxMinRatio
                               + " max/min ratio");

            if(currentMaxMinRatio <= minMaxMinRatio) {
                minMaxMinRatio = currentMaxMinRatio;
                minCluster = nextCluster;

                dumpClusterToFile(outputDir,
                                  RebalanceUtils.finalClusterFileName + numTries,
                                  minCluster);
                dumpAnalysisToFile(outputDir, RebalanceUtils.finalClusterFileName + numTries
                                              + ".analysis", analysis.getSecond());
            }
            System.out.println("-------------------------\n");
        }

        System.out.println("\n==========================");
        System.out.println("Final distribution");
        analysis = new ClusterInstance(minCluster, storeDefs).analyzeBalanceVerbose();
        System.out.println(analysis.getSecond());

        dumpClusterToFile(outputDir, RebalanceUtils.finalClusterFileName, minCluster);
        dumpAnalysisToFile(outputDir,
                           RebalanceUtils.finalClusterFileName + ".analysis",
                           analysis.getSecond());
        return;
    }

    /**
     * Determines how many primary partitions each node within each zone should
     * have. The list of integers returned per zone is the same length as the
     * number of nodes in that zone.
     * 
     * @param targetCluster
     * @return A map of zoneId to list of target number of partitions per node
     *         within zone.
     */
    public static HashMap<Integer, List<Integer>> getBalancedNumberOfPrimaryPartitionsPerNodePerZone(final Cluster targetCluster) {
        HashMap<Integer, List<Integer>> numPartitionsPerNodePerZone = Maps.newHashMap();
        for(Integer zoneId: targetCluster.getZoneIds()) {
            int numNodesInZone = targetCluster.getNumberOfNodesInZone(zoneId);
            int numPartitionsInZone = targetCluster.getNumberOfPartitionsInZone(zoneId);
            int floorPartitionsPerNodeInZone = numPartitionsInZone / numNodesInZone;
            int numNodesInZoneWithCeil = numPartitionsInZone
                                         - (numNodesInZone * floorPartitionsPerNodeInZone);

            ArrayList<Integer> partitionsOnNode = new ArrayList<Integer>(numNodesInZone);
            for(int i = 0; i < numNodesInZoneWithCeil; i++) {
                partitionsOnNode.add(i, floorPartitionsPerNodeInZone + 1);
            }
            for(int i = numNodesInZoneWithCeil; i < numNodesInZone; i++) {
                partitionsOnNode.add(i, floorPartitionsPerNodeInZone);
            }
            numPartitionsPerNodePerZone.put(zoneId, partitionsOnNode);
        }
        return numPartitionsPerNodePerZone;
    }

    /**
     * Assign target number of partitions per node to specific node IDs. Then,
     * separates Nodes into donorNodes and stealerNodes based on whether the
     * node needs to donate or steal primary partitions.
     * 
     * @param targetCluster
     * @return a Pair. First element is donorNodes, second element is
     *         stealerNodes. Each element in the pair is a HashMap of Node to
     *         Integer where the integer value is the number of partitions to
     *         store.
     */
    public static Pair<HashMap<Node, Integer>, HashMap<Node, Integer>> getDonorsAndStealersForBalancedPrimaries(final Cluster targetCluster) {
        HashMap<Integer, List<Integer>> numPartitionsPerNodePerZone = getBalancedNumberOfPrimaryPartitionsPerNodePerZone(targetCluster);

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
     * Balance the number of primary partitions per node per zone. Balanced
     * means that the number of primary partitions per node within a zone are
     * all within one of one another. A common usage of this method is to assign
     * partitions to newly added nodes that do not have any partitions yet.
     * 
     * @param targetCluster Target cluster metadata ( which contains old nodes +
     *        new nodes [ empty partitions ])
     * @param storeDefs List of store definitions
     * @param generateEnableXzonePrimary See RebalanceCLI.
     * @param generateEnableAnyXzoneNary See RebalanceCLI.
     * @param generateEnableLastResortXzoneNary See RebalanceCLI.
     * @return Return new cluster metadata
     */
    public static Cluster balancePrimaryPartitionsPerNode(final Cluster targetCluster,
                                                          final List<StoreDefinition> storeDefs,
                                                          final boolean generateEnableXzonePrimary,
                                                          final boolean generateEnableAnyXzoneNary,
                                                          final boolean generateEnableLastResortXzoneNary) {
        System.out.println("Balance number of partitions per node within a zone.");
        System.out.println("numPartitionsPerZone");
        for(int zoneId: targetCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + targetCluster.getNumberOfPartitionsInZone(zoneId));
        }
        System.out.println("numNodesPerZone");
        for(int zoneId: targetCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + targetCluster.getNumberOfNodesInZone(zoneId));
        }

        Pair<HashMap<Node, Integer>, HashMap<Node, Integer>> donorsAndStealers = getDonorsAndStealersForBalancedPrimaries(targetCluster);
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

                    if(!generateEnableXzonePrimary
                       && (currentDonorNode.getZoneId() != stealerNode.getZoneId())) {
                        // Only steal from donor nodes within same zone
                        continue;
                    }
                    // Only steal from donor nodes with extra partitions
                    if(currentDonorNode.getNumberOfPartitions() == donorNodes.get(donorNode)) {
                        continue;
                    }

                    List<Integer> donorPartitions = Lists.newArrayList(currentDonorNode.getPartitionIds());

                    Collections.shuffle(donorPartitions, new Random(System.currentTimeMillis()));
                    for(int donorPartition: donorPartitions) {
                        Cluster intermediateCluster = RebalanceUtils.createUpdatedCluster(returnCluster,
                                                                                          stealerNode.getId(),
                                                                                          Lists.newArrayList(donorPartition));

                        int crossZoneMoves = 0;
                        if(!generateEnableAnyXzoneNary) {
                            // getCrossZoneMoves can be a *slow* call. E.g., for
                            // an 11 node cluster, the call takes ~230 ms,
                            // whereas for a 39 node cluster the call takes ~10
                            // s (40x longer).
                            long startTimeNs = System.nanoTime();
                            crossZoneMoves = RebalanceUtils.getCrossZoneMoves(intermediateCluster,
                                                                              new RebalanceClusterPlan(returnCluster,
                                                                                                       intermediateCluster,
                                                                                                       storeDefs,
                                                                                                       true));
                            System.out.println("getCrossZoneMoves took "
                                               + (System.nanoTime() - startTimeNs) + " ns.");
                        }
                        if(crossZoneMoves == 0) {
                            returnCluster = intermediateCluster;
                            partitionsToSteal--;
                            System.out.println("Stealer node " + stealerNode.getId()
                                               + ", donor node " + currentDonorNode.getId()
                                               + ", partition stolen " + donorPartition);
                            break;
                        } else {
                            System.out.println("Stealer node " + stealerNode.getId()
                                               + ", donor node " + currentDonorNode.getId()
                                               + ", attempted to steal partition " + donorPartition
                                               + " however,  getCrossZoneMoves did NOT return 0!");

                            if(donorPartition == donorPartitions.get(donorPartitions.size() - 1)) {
                                partitionsToSteal--;
                                if(generateEnableLastResortXzoneNary) {
                                    returnCluster = intermediateCluster;
                                    System.out.println("Stealer node "
                                                       + stealerNode.getId()
                                                       + ", donor node "
                                                       + currentDonorNode.getId()
                                                       + ", is stealing partition "
                                                       + donorPartition
                                                       + " in spite of the fact that getCrossZoneMoves did NOT return 0!");
                                } else {
                                    System.out.println("Stealer node "
                                                       + stealerNode.getId()
                                                       + " is reducing number of partitions to steal because getCrossZoneMoves did not return 0 for all possible partitions.");
                                }
                            }
                        }
                    }

                    if(partitionsToSteal == 0)
                        break;
                }
            }
        }

        return returnCluster;
    }

    // TODO: Add a similar method that rebalances a cluster to ensure that no
    // Node hosts contiguous partition IDs (rather than doing so at zone level).
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
        // TODO: Make this loop definitive. I.e., ensure that
        // maxContiguousPartitionsPerZone is truly met.
        System.out.println("Looping to evenly balance partitions across zones while limiting contiguous partitions");
        int repeatContigBalance = 10;
        Cluster nextCluster = targetCluster;
        for(int i = 0; i < repeatContigBalance; i++) {
            nextCluster = balanceContiguousPartitionsPerZone(nextCluster,
                                                             maxContiguousPartitionsPerZone);

            nextCluster = balanceNumPartitionsPerZone(nextCluster);
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
            List<Integer> partitions = new ArrayList<Integer>(targetCluster.getPartitionIdsInZone(zoneId));

            List<Integer> partitionsToRemoveFromThisZone = new ArrayList<Integer>();
            List<Integer> contiguousPartitions = new ArrayList<Integer>();
            int lastPartitionId = partitions.get(0);
            for(int i = 1; i < partitions.size(); ++i) {
                if(partitions.get(i) == lastPartitionId + 1) {
                    contiguousPartitions.add(partitions.get(i));
                } else {
                    if(contiguousPartitions.size() > maxContiguousPartitionsPerZone) {
                        System.out.println("Contiguous partitions: " + contiguousPartitions);
                        partitionsToRemoveFromThisZone.addAll(removeItemsToSplitListEvenly(contiguousPartitions,
                                                                                           maxContiguousPartitionsPerZone));
                    }
                    contiguousPartitions.clear();
                }
                lastPartitionId = partitions.get(i);
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
     * Ensures that all zones have within 1 number of partitions.
     * 
     * Moves some number of partitions from each zone to some other random
     * zone/node. There is some chance that such moves could result in
     * contiguous partitions in other zones.
     * 
     * @param targetCluster Target cluster metadata
     * @return Return a pair of cluster metadata and number of primary
     *         partitions that have moved.
     */
    public static Cluster balanceNumPartitionsPerZone(final Cluster targetCluster) {
        System.out.println("Balance number of partitions per zone.");
        System.out.println("numPartitionsPerZone");
        for(int zoneId: targetCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + targetCluster.getNumberOfPartitionsInZone(zoneId));
        }
        System.out.println("numNodesPerZone");
        for(int zoneId: targetCluster.getZoneIds()) {
            System.out.println(zoneId + " : " + targetCluster.getNumberOfNodesInZone(zoneId));
        }

        int numPartitions = targetCluster.getNumberOfPartitions();

        // Set up a balanced target number of partitions to move per zone
        HashMap<Integer, Integer> targetNumPartitionsPerZone = Maps.newHashMap();
        int numZones = targetCluster.getNumberOfZones();
        int floorPartitions = numPartitions / numZones;
        int numZonesWithCeil = numPartitions - (numZones * floorPartitions);
        int zoneCounter = 0;
        for(Integer zoneId: targetCluster.getZoneIds()) {
            int floorPartitionsInZone = floorPartitions
                                        - targetCluster.getNumberOfPartitionsInZone(zoneId);
            if(zoneCounter < numZonesWithCeil) {
                targetNumPartitionsPerZone.put(zoneId, floorPartitionsInZone + 1);
            } else {
                targetNumPartitionsPerZone.put(zoneId, floorPartitionsInZone);
            }
            zoneCounter++;
        }

        List<Integer> donorZoneIds = new ArrayList<Integer>();
        List<Integer> stealerZoneIds = new ArrayList<Integer>();
        for(Integer zoneId: targetCluster.getZoneIds()) {
            if(targetNumPartitionsPerZone.get(zoneId) > 0) {
                stealerZoneIds.add(zoneId);
            } else if(targetNumPartitionsPerZone.get(zoneId) < 0) {
                donorZoneIds.add(zoneId);
            }
        }

        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        Random r = new Random();

        for(Integer stealerZoneId: stealerZoneIds) {
            while(targetNumPartitionsPerZone.get(stealerZoneId) > 0) {
                for(Integer donorZoneId: donorZoneIds) {
                    if(targetNumPartitionsPerZone.get(donorZoneId) < 0) {
                        // Select random stealer node
                        int stealerNodeOffset = r.nextInt(targetCluster.getNumberOfNodesInZone(stealerZoneId));
                        Integer stealerNodeId = new ArrayList<Integer>(targetCluster.getNodeIdsInZone(stealerZoneId)).get(stealerNodeOffset);

                        // Select random donor partition
                        List<Integer> partitionsThisZone = new ArrayList<Integer>(targetCluster.getPartitionIdsInZone(donorZoneId));
                        int donorPartitionOffset = r.nextInt(partitionsThisZone.size());
                        int donorPartitionId = partitionsThisZone.get(donorPartitionOffset);

                        // Accounting
                        targetNumPartitionsPerZone.put(donorZoneId,
                                                       targetNumPartitionsPerZone.get(donorZoneId) + 1);
                        targetNumPartitionsPerZone.put(stealerZoneId,
                                                       targetNumPartitionsPerZone.get(stealerZoneId) - 1);

                        // Steal it!
                        returnCluster = RebalanceUtils.createUpdatedCluster(returnCluster,
                                                                            stealerNodeId,
                                                                            Lists.newArrayList(donorPartitionId));
                    }
                }
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
        List<Integer> nodeIdsInZone = new ArrayList<Integer>(targetCluster.getNodeIdsInZone(zoneId));

        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        Random r = new Random();

        // Select random stealer node
        int stealerNodeOffset = r.nextInt(nodeIdsInZone.size());
        Integer stealerNodeId = nodeIdsInZone.get(stealerNodeOffset);

        // Select random stealer partition
        List<Integer> stealerPartitions = returnCluster.getNodeById(stealerNodeId)
                                                       .getPartitionIds();
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
                                                  final boolean enableXzoneShuffle,
                                                  final int randomSwapAttempts,
                                                  final int randomSwapSuccesses,
                                                  List<StoreDefinition> storeDefs) {
        List<Integer> zoneIds = new ArrayList<Integer>(targetCluster.getZoneIds());
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);

        double currentMaxMinRatio = new ClusterInstance(returnCluster, storeDefs).analyzeBalance();

        int successes = 0;
        for(int i = 0; i < randomSwapAttempts; i++) {
            Collections.shuffle(zoneIds, new Random(System.currentTimeMillis()));
            for(Integer zoneId: zoneIds) {
                Cluster shuffleResults = swapRandomPartitionsWithinZone(returnCluster, zoneId);
                double nextMaxMinRatio = new ClusterInstance(shuffleResults, storeDefs).analyzeBalance();
                if(nextMaxMinRatio < currentMaxMinRatio) {
                    System.out.println("Swap improved max-min ratio: " + currentMaxMinRatio
                                       + " -> " + nextMaxMinRatio + " (improvement " + successes
                                       + " on swap attempt " + i + " in zone " + zoneId + ")");
                    int xZoneMoves = 0;
                    if(!enableXzoneShuffle) {
                        xZoneMoves = RebalanceUtils.getCrossZoneMoves(shuffleResults,
                                                                      new RebalanceClusterPlan(returnCluster,
                                                                                               shuffleResults,
                                                                                               storeDefs,
                                                                                               true));
                    }
                    if(xZoneMoves == 0) {
                        successes++;
                        returnCluster = shuffleResults;
                        currentMaxMinRatio = nextMaxMinRatio;
                    } else {
                        System.out.println("BUT, swap resulted in a cross zone move and so is ignored.");
                    }
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
     * Within a single zone, tries swapping every partition with every other
     * partition (ignoring those on the same node) and chooses the best swap.
     * This is very expensive and is not feasible for clusters with a desirable
     * number of partitions.
     * 
     * @param targetCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @param storeDefs List of store definitions
     * @return
     */
    public static Cluster swapGreedyPartitionsWithinZone(final Cluster targetCluster,
                                                         final int zoneId,
                                                         List<StoreDefinition> storeDefs) {
        List<Integer> nodeIdsInZone = new ArrayList<Integer>(targetCluster.getNodeIdsInZone(zoneId));

        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        double currentMaxMinRatio = new ClusterInstance(returnCluster, storeDefs).analyzeBalance();
        int nodeIdA = -1;
        int nodeIdB = -1;
        int partitionIdA = -1;
        int partitionIdB = -1;

        // O(n^2) where n is the number of partitions. Yikes!
        int progressCounter = 0;
        for(int nodeIdEh: nodeIdsInZone) {
            List<Integer> partitionIdsEh = returnCluster.getNodeById(nodeIdEh).getPartitionIds();
            for(Integer partitionIdEh: partitionIdsEh) {
                for(int nodeIdBee: nodeIdsInZone) {
                    if(nodeIdBee == nodeIdEh)
                        continue;
                    List<Integer> partitionIdsBee = returnCluster.getNodeById(nodeIdBee)
                                                                 .getPartitionIds();
                    for(Integer partitionIdBee: partitionIdsBee) {
                        progressCounter++;
                        if(progressCounter % 500 == 0)
                            System.out.println("o");
                        else if(progressCounter % 25 == 0)
                            System.out.println(".");

                        Cluster swapResult = swapPartitions(returnCluster,
                                                            nodeIdEh,
                                                            partitionIdEh,
                                                            nodeIdBee,
                                                            partitionIdBee);
                        double swapMaxMinRatio = new ClusterInstance(swapResult, storeDefs).analyzeBalance();
                        if(swapMaxMinRatio < currentMaxMinRatio) {
                            currentMaxMinRatio = swapMaxMinRatio;
                            System.out.println(" -> " + currentMaxMinRatio);
                            nodeIdA = nodeIdEh;
                            partitionIdA = partitionIdEh;
                            nodeIdB = nodeIdBee;
                            partitionIdB = partitionIdBee;
                        }
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
     * @param targetCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @param greedySwapMaxPartitionsPerNode See RebalanceCLI.
     * @param greedySwapMaxPartitionsPerZone See RebalanceCLI.
     * @param storeDefs
     * @return
     */
    public static Cluster swapGreedyRandomPartitionsWithinZone(final Cluster targetCluster,
                                                               final int zoneId,
                                                               final int greedySwapMaxPartitionsPerNode,
                                                               final int greedySwapMaxPartitionsPerZone,
                                                               List<StoreDefinition> storeDefs) {
        List<Integer> nodeIdsInZone = new ArrayList<Integer>(targetCluster.getNodeIdsInZone(zoneId));

        System.out.println("GreedyRandom : nodeIdsInZone:" + nodeIdsInZone);

        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);
        double currentMaxMinRatio = new ClusterInstance(returnCluster, storeDefs).analyzeBalance();
        int nodeIdA = -1;
        int nodeIdB = -1;
        int partitionIdA = -1;
        int partitionIdB = -1;

        for(int nodeIdEh: nodeIdsInZone) {
            System.out.println("GreedyRandom : processing nodeId:" + nodeIdEh);
            List<Integer> partitionIdsEh = new ArrayList<Integer>();
            partitionIdsEh.addAll(returnCluster.getNodeById(nodeIdEh).getPartitionIds());
            Collections.shuffle(partitionIdsEh);

            int maxPartitionsInEh = Math.min(greedySwapMaxPartitionsPerNode, partitionIdsEh.size());
            for(int offsetEh = 0; offsetEh < maxPartitionsInEh; ++offsetEh) {
                Integer partitionIdEh = partitionIdsEh.get(offsetEh);

                List<Pair<Integer, Integer>> partitionIdsZone = new ArrayList<Pair<Integer, Integer>>();
                for(int nodeIdBee: nodeIdsInZone) {
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
                    double swapMaxMinRatio = new ClusterInstance(swapResult, storeDefs).analyzeBalance();
                    if(swapMaxMinRatio < currentMaxMinRatio) {
                        currentMaxMinRatio = swapMaxMinRatio;
                        System.out.println(" -> " + currentMaxMinRatio);
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
     * @param storeDefs
     * @return
     */
    public static Cluster greedyShufflePartitions(final Cluster targetCluster,
                                                  final boolean enableXzoneShuffle,
                                                  final int greedyAttempts,
                                                  final int greedySwapMaxPartitionsPerNode,
                                                  final int greedySwapMaxPartitionsPerZone,
                                                  List<StoreDefinition> storeDefs) {
        List<Integer> zoneIds = new ArrayList<Integer>(targetCluster.getZoneIds());
        Cluster returnCluster = ClusterUtils.copyCluster(targetCluster);

        double currentMaxMinRatio = new ClusterInstance(returnCluster, storeDefs).analyzeBalance();

        for(int i = 0; i < greedyAttempts; i++) {
            Collections.shuffle(zoneIds, new Random(System.currentTimeMillis()));
            for(Integer zoneId: zoneIds) {
                System.out.println("Greedy swap attempt: zone " + zoneId + " , attempt " + i
                                   + " of " + greedyAttempts);
                Cluster shuffleResults = swapGreedyRandomPartitionsWithinZone(returnCluster,
                                                                              zoneId,
                                                                              greedySwapMaxPartitionsPerNode,
                                                                              greedySwapMaxPartitionsPerZone,
                                                                              storeDefs);
                double nextMaxMinRatio = new ClusterInstance(shuffleResults, storeDefs).analyzeBalance();

                if(nextMaxMinRatio == currentMaxMinRatio) {
                    System.out.println("Not improving for zone: " + zoneId);
                } else {
                    System.out.println("Swap improved max-min ratio: " + currentMaxMinRatio
                                       + " -> " + nextMaxMinRatio + " (swap attempt " + i
                                       + " in zone " + zoneId + ")");

                    int xZoneMoves = 0;
                    if(!enableXzoneShuffle) {
                        xZoneMoves = RebalanceUtils.getCrossZoneMoves(shuffleResults,
                                                                      new RebalanceClusterPlan(returnCluster,
                                                                                               shuffleResults,
                                                                                               storeDefs,
                                                                                               true));
                    }
                    if(xZoneMoves == 0) {
                        returnCluster = shuffleResults;
                        currentMaxMinRatio = nextMaxMinRatio;
                    } else {
                        System.out.println("BUT, swap resulted in a cross zone move and so is ignored.");
                    }
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

    /**
     * This method breaks the inputList into distinct lists that are no longer
     * than maxContiguous in length. It does so by removing elements from the
     * inputList. This method removes the minimum necessary items to achieve the
     * goal. This method chooses items to remove that minimize the length of the
     * maximum remaining run. E.g. given an inputList of 20 elements and
     * maxContiguous=8, this method will return the 2 elements that break the
     * inputList into 3 runs of 6 items. (As opposed to 2 elements that break
     * the inputList into two runs of eight items and one run of two items.
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
