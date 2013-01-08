/*
 * Copyright 2008-2012 LinkedIn, Inc
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
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalanceNodePlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.server.VoldemortConfig;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * RebalanceUtils provide basic functionality for rebalancing.
 * 
 */
public class RebalanceUtils {

    private static Logger logger = Logger.getLogger(RebalanceUtils.class);

    public final static List<String> canRebalanceList = Arrays.asList(BdbStorageConfiguration.TYPE_NAME,
                                                                      ReadOnlyStorageConfiguration.TYPE_NAME);

    public final static String initialClusterFileName = "initial-cluster.xml";
    public final static String finalClusterFileName = "final-cluster.xml";

    /**
     * Given the current replica to partition list, try to check if the donor
     * node would already contain that partition and if yes, ignore it
     * 
     * @param stealerNodeId Stealer node id
     * @param cluster Cluster metadata
     * @param storeDef Store definition
     * @param currentReplicaToPartitionList Current replica to partition list
     * @return Optimized replica to partition list
     */
    public static HashMap<Integer, List<Integer>> getOptimizedReplicaToPartitionList(int stealerNodeId,
                                                                                     Cluster cluster,
                                                                                     StoreDefinition storeDef,
                                                                                     HashMap<Integer, List<Integer>> currentReplicaToPartitionList) {

        HashMap<Integer, List<Integer>> optimizedReplicaToPartitionList = Maps.newHashMap();
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);
        for(Entry<Integer, List<Integer>> tuple: currentReplicaToPartitionList.entrySet()) {
            List<Integer> partitionList = Lists.newArrayList();
            for(int partition: tuple.getValue()) {
                List<Integer> preferenceList = strategy.getReplicatingPartitionList(partition);

                // If this node was already in the
                // preference list before, a copy of the
                // data will already exist - Don't copy
                // it!
                if(!RebalanceUtils.containsPreferenceList(cluster, preferenceList, stealerNodeId)) {
                    partitionList.add(partition);
                }
            }

            if(partitionList.size() > 0) {
                optimizedReplicaToPartitionList.put(tuple.getKey(), partitionList);
            }
        }

        return optimizedReplicaToPartitionList;

    }

    /**
     * Get the latest cluster from all available nodes in the cluster<br>
     * 
     * Throws exception if:<br>
     * A) Any node in the required nodes list fails to respond.<br>
     * B) Cluster is in inconsistent state with concurrent versions for cluster
     * metadata on any two nodes.<br>
     * 
     * @param requiredNodes List of nodes from which we definitely need an
     *        answer
     * @param adminClient Admin client used to query the nodes
     * @return Returns the latest cluster metadata
     */
    public static Versioned<Cluster> getLatestCluster(List<Integer> requiredNodes,
                                                      AdminClient adminClient) {
        Versioned<Cluster> latestCluster = new Versioned<Cluster>(adminClient.getAdminClientCluster());
        ArrayList<Versioned<Cluster>> clusterList = new ArrayList<Versioned<Cluster>>();

        clusterList.add(latestCluster);
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            try {
                Versioned<Cluster> versionedCluster = adminClient.metadataMgmtOps.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();
                if(null != newClock && !clusterList.contains(versionedCluster)) {
                    // check no two clocks are concurrent.
                    checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occurred occurred = newClock.compare(latestCluster.getVersion());
                    if(Occurred.AFTER.equals(occurred))
                        latestCluster = versionedCluster;
                }
            } catch(Exception e) {
                if(null != requiredNodes && requiredNodes.contains(node.getId()))
                    throw new VoldemortException("Failed on node " + node.getId(), e);
                else
                    logger.info("Failed on node " + node.getId(), e);
            }
        }

        return latestCluster;
    }

    private static void checkNotConcurrent(ArrayList<Versioned<Cluster>> clockList,
                                           VectorClock newClock) {
        for(Versioned<Cluster> versionedCluster: clockList) {
            VectorClock clock = (VectorClock) versionedCluster.getVersion();
            if(Occurred.CONCURRENTLY.equals(clock.compare(newClock)))
                throw new VoldemortException("Cluster is in inconsistent state because we got conflicting clocks "
                                             + clock + " and on current node " + newClock);

        }
    }

    /**
     * Return the number of cross zone copying that is going to take place
     * 
     * @param targetCluster Target cluster metadata
     * @param plan The rebalance plan
     * @return Number of cross zone moves
     */
    public static int getCrossZoneMoves(final Cluster targetCluster, final RebalanceClusterPlan plan) {

        int crossZoneMoves = 0;
        for(RebalanceNodePlan nodePlan: plan.getRebalancingTaskQueue()) {
            List<RebalancePartitionsInfo> infos = nodePlan.getRebalanceTaskList();
            for(RebalancePartitionsInfo info: infos) {
                Node donorNode = targetCluster.getNodeById(info.getDonorId());
                Node stealerNode = targetCluster.getNodeById(info.getStealerId());

                if(donorNode.getZoneId() != stealerNode.getZoneId()) {
                    crossZoneMoves++;
                }
            }
        }

        return crossZoneMoves;
    }

    /**
     * Return the number of total moves
     * 
     * @param plan The rebalance plan
     * @return Number of moves
     */
    public static int getTotalMoves(final RebalanceClusterPlan plan) {

        int totalMoves = 0;
        for(RebalanceNodePlan nodePlan: plan.getRebalancingTaskQueue()) {
            totalMoves += nodePlan.getRebalanceTaskList().size();
        }

        return totalMoves;
    }

    /**
     * Given a list of partition informations check all of them belong to the
     * same donor node
     * 
     * @param partitionInfos List of partition infos
     * @param expectedDonorId Expected donor node id ( If -1, then just checks
     *        if all are same )
     */
    public static void assertSameDonor(List<RebalancePartitionsInfo> partitionInfos,
                                       int expectedDonorId) {
        int donorId = (expectedDonorId < 0) ? partitionInfos.get(0).getDonorId() : expectedDonorId;
        for(RebalancePartitionsInfo info: partitionInfos) {
            if(info.getDonorId() != donorId) {
                throw new VoldemortException("Found a stealer information " + info
                                             + " having a different donor node from others ( "
                                             + donorId + " )");
            }
        }
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

    // TODO: Refactor balance algorithms into separate class
    // (RebalanceClusterUtils?). (Do this after a commit)
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
        Pair<Double, String> analysis = analyzeBalanceVerbose(currentCluster, storeDefs);
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
                nextCluster = RebalanceUtils.balancePrimaryPartitionsPerNode(nextCluster,
                                                                             storeDefs,
                                                                             generateEnableXzonePrimary,
                                                                             generateEnableAnyXzoneNary,
                                                                             generateEnableLastResortXzoneNary);
            }

            if(enableRandomSwaps) {
                nextCluster = RebalanceUtils.randomShufflePartitions(nextCluster,
                                                                     enableXzoneShuffle,
                                                                     randomSwapAttempts,
                                                                     randomSwapSuccesses,
                                                                     storeDefs);
            }
            if(enableGreedySwaps) {
                nextCluster = RebalanceUtils.greedyShufflePartitions(nextCluster,
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
            analysis = analyzeBalanceVerbose(nextCluster, storeDefs);
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
        analysis = analyzeBalanceVerbose(minCluster, storeDefs);
        System.out.println(analysis.getSecond());

        dumpClusterToFile(outputDir, RebalanceUtils.finalClusterFileName, minCluster);
        dumpAnalysisToFile(outputDir,
                           RebalanceUtils.finalClusterFileName + ".analysis",
                           analysis.getSecond());
        return;
    }

    /**
     * Wrapper that just returns the max/min ratio metric and throws away the
     * verbose string.
     */
    public static double analyzeBalance(final Cluster currentCluster,
                                        final List<StoreDefinition> storeDefs) {
        Pair<Double, String> analysis = analyzeBalanceVerbose(currentCluster, storeDefs);
        return analysis.getFirst();
    }

    /**
     * 
     * @param currentCluster
     * @param nodeIdToPartitionCount
     * @param title
     * @return
     */
    public static Pair<Double, String> summarizeBalance(final Cluster currentCluster,
                                                        final Map<Integer, Integer> nodeIdToPartitionCount,
                                                        String title) {
        StringBuilder builder = new StringBuilder();
        Set<Integer> nodeIds = currentCluster.getNodeIds();

        builder.append("\n" + title + "\n");
        int minVal = Integer.MAX_VALUE;
        int maxVal = Integer.MIN_VALUE;
        int aggCount = 0;
        for(Integer nodeId: nodeIds) {
            int curCount = nodeIdToPartitionCount.get(nodeId);
            builder.append("\tNode ID: " + nodeId + " : " + curCount + " ("
                           + currentCluster.getNodeById(nodeId).getHost() + ")\n");
            aggCount += curCount;
            if(curCount > maxVal)
                maxVal = curCount;
            if(curCount < minVal)
                minVal = curCount;
        }
        double maxMinRatio = maxVal * 1.0 / minVal;
        if(minVal == 0) {
            maxMinRatio = maxVal;
        }
        builder.append("\tMin: " + minVal + "\n");
        builder.append("\tAvg: " + aggCount / nodeIdToPartitionCount.size() + "\n");
        builder.append("\tMax: " + maxVal + "\n");
        builder.append("\t\tMax/Min: " + maxMinRatio + "\n");

        return Pair.create(maxMinRatio, builder.toString());
    }

    /**
     * Outputs an analysis of how balanced the cluster is given the store
     * definitions. The metric max/min ratio is used to describe balance. The
     * max/min ratio is the ratio of largest number of store-partitions to
     * smallest number of store-partitions). If the minimum number of
     * store-partitions is zero, then the max/min ratio is set to max rather
     * than to infinite.
     * 
     * @param currentCluster Current cluster metadata
     * @param storeDefs List of store definitions
     * @return First element of pair is the max/min ratio. Second element of
     *         pair is a string that can be printed to dump all the gory details
     *         of the analysis.
     */
    public static Pair<Double, String> analyzeBalanceVerbose(final Cluster currentCluster,
                                                             final List<StoreDefinition> storeDefs) {
        StringBuilder builder = new StringBuilder();
        HashMap<StoreDefinition, Integer> uniqueStores = KeyDistributionGenerator.getUniqueStoreDefinitionsWithCounts(storeDefs);
        List<ByteArray> keys = KeyDistributionGenerator.generateKeys(KeyDistributionGenerator.DEFAULT_NUM_KEYS);
        Set<Integer> nodeIds = currentCluster.getNodeIds();

        builder.append("CLUSTER XML SUMMARY\n");
        Map<Integer, Integer> zoneIdToPartitionCount = Maps.newHashMap();
        Map<Integer, Integer> zoneIdToNodeCount = Maps.newHashMap();
        for(Zone zone: currentCluster.getZones()) {
            zoneIdToPartitionCount.put(zone.getId(), 0);
            zoneIdToNodeCount.put(zone.getId(), 0);
        }
        for(Node node: currentCluster.getNodes()) {
            zoneIdToPartitionCount.put(node.getZoneId(),
                                       zoneIdToPartitionCount.get(node.getZoneId())
                                               + node.getNumberOfPartitions());
            zoneIdToNodeCount.put(node.getZoneId(), zoneIdToNodeCount.get(node.getZoneId()) + 1);
        }
        builder.append("\n");
        builder.append("Number of partitions per zone:\n");
        for(Zone zone: currentCluster.getZones()) {
            builder.append("\tZone: " + zone.getId() + " - "
                           + zoneIdToPartitionCount.get(zone.getId()) + "\n");
        }
        builder.append("\n");
        builder.append("Number of nodes per zone:\n");
        for(Zone zone: currentCluster.getZones()) {
            builder.append("\tZone: " + zone.getId() + " - " + zoneIdToNodeCount.get(zone.getId())
                           + "\n");
        }
        builder.append("\n");

        builder.append("Number of partitions per node:\n");
        for(Node node: currentCluster.getNodes()) {
            builder.append("\tNode ID: " + node.getId() + " - " + node.getNumberOfPartitions()
                           + " (" + node.getHost() + ")\n");
        }
        builder.append("\n");

        builder.append("PARTITION DUMP\n");
        Map<Integer, Integer> primaryAggNodeIdToPartitionCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            primaryAggNodeIdToPartitionCount.put(nodeId, 0);
        }

        Map<Integer, Integer> allAggNodeIdToPartitionCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            allAggNodeIdToPartitionCount.put(nodeId, 0);
        }

        for(StoreDefinition storeDefinition: uniqueStores.keySet()) {
            builder.append("\n");
            builder.append("Store exemplar: " + storeDefinition.getName() + "\n");
            builder.append("\tReplication factor: " + storeDefinition.getReplicationFactor() + "\n");
            builder.append("\tRouting strategy: " + storeDefinition.getRoutingStrategyType() + "\n");
            builder.append("\tThere are " + uniqueStores.get(storeDefinition)
                           + " other similar stores.\n");

            // Map of node Id to Sets of pairs. Pairs of Integers are of
            // <replica_type, partition_id>
            Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions = getNodeIdToAllPartitions(currentCluster,
                                                                                                       storeDefinition,
                                                                                                       true);
            Map<Integer, Integer> primaryNodeIdToPartitionCount = Maps.newHashMap();
            Map<Integer, Integer> allNodeIdToPartitionCount = Maps.newHashMap();

            // Print out all partitions, by replica type, per node
            builder.append("\n");
            builder.append("\tDetailed Dump:\n");
            for(Integer nodeId: nodeIds) {
                builder.append("\tNode ID: " + nodeId + "\n");
                primaryNodeIdToPartitionCount.put(nodeId, 0);
                allNodeIdToPartitionCount.put(nodeId, 0);
                Set<Pair<Integer, Integer>> partitionPairs = nodeIdToAllPartitions.get(nodeId);

                int replicaType = 0;
                while(partitionPairs.size() > 0) {
                    List<Pair<Integer, Integer>> replicaPairs = new ArrayList<Pair<Integer, Integer>>();
                    for(Pair<Integer, Integer> pair: partitionPairs) {
                        if(pair.getFirst() == replicaType) {
                            replicaPairs.add(pair);
                        }
                    }
                    List<Integer> partitions = new ArrayList<Integer>();
                    for(Pair<Integer, Integer> pair: replicaPairs) {
                        partitionPairs.remove(pair);
                        partitions.add(pair.getSecond());
                    }
                    java.util.Collections.sort(partitions);
                    builder.append("\t\t" + replicaType + " : " + partitions.size() + " : "
                                   + partitions.toString() + "\n");
                    if(replicaType == 0) {
                        primaryNodeIdToPartitionCount.put(nodeId,
                                                          primaryNodeIdToPartitionCount.get(nodeId)
                                                                  + partitions.size());
                    }
                    allNodeIdToPartitionCount.put(nodeId, allNodeIdToPartitionCount.get(nodeId)
                                                          + partitions.size());
                    replicaType++;
                }
            }

            builder.append("\n");
            builder.append("\tSummary Dump:\n");
            for(Integer nodeId: nodeIds) {
                builder.append("\tNode ID: " + nodeId + " : "
                               + allNodeIdToPartitionCount.get(nodeId) + "\n");
                primaryAggNodeIdToPartitionCount.put(nodeId,
                                                     primaryAggNodeIdToPartitionCount.get(nodeId)
                                                             + (primaryNodeIdToPartitionCount.get(nodeId) * uniqueStores.get(storeDefinition)));
                allAggNodeIdToPartitionCount.put(nodeId,
                                                 allAggNodeIdToPartitionCount.get(nodeId)
                                                         + (allNodeIdToPartitionCount.get(nodeId) * uniqueStores.get(storeDefinition)));
            }
        }

        builder.append("\n");
        builder.append("STD DEV ANALYSIS\n");
        builder.append("\n");
        builder.append(KeyDistributionGenerator.printOverallDistribution(currentCluster,
                                                                         storeDefs,
                                                                         keys));
        builder.append("\n");
        builder.append("\n");

        Pair<Double, String> summary = summarizeBalance(currentCluster,
                                                        primaryAggNodeIdToPartitionCount,
                                                        "AGGREGATE PRIMARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());

        summary = summarizeBalance(currentCluster,
                                   allAggNodeIdToPartitionCount,
                                   "AGGREGATE NARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());

        return new Pair<Double, String>(summary.getFirst(), builder.toString());
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
        Cluster returnCluster = copyCluster(targetCluster);

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
                        Cluster intermediateCluster = createUpdatedCluster(returnCluster,
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

    /**
     * Loops over cluster and repeatedly tries to break up contiguous runs of
     * partitions. After each phase of breaking up contiguous partitions, random
     * partitions are selected to move between zones to balance the number of
     * partitions in each zone. This process loops until no partitions move
     * across zones.
     * 
     * @param nextCluster
     * @param maxContiguousPartitionsPerZone See RebalanceCLI.
     * @return
     */
    public static Cluster repeatedlyBalanceContiguousPartitionsPerZone(final Cluster targetCluster,
                                                                       final int maxContiguousPartitionsPerZone) {
        int contigPartitionsMoved = 0;
        Cluster nextCluster = targetCluster;
        do {
            nextCluster = RebalanceUtils.balanceContiguousPartitionsPerZone(nextCluster,
                                                                            maxContiguousPartitionsPerZone);

            nextCluster = RebalanceUtils.balanceNumPartitionsPerZone(nextCluster);

            System.out.println("Looping to evenly balance partitions across zones while limiting contiguous partitions: "
                               + contigPartitionsMoved);
        } while(contigPartitionsMoved > 0);

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

        Cluster returnCluster = copyCluster(targetCluster);

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
                returnCluster = createUpdatedCluster(returnCluster,
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

        Cluster returnCluster = copyCluster(targetCluster);
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
                        returnCluster = createUpdatedCluster(returnCluster,
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
        Cluster returnCluster = copyCluster(targetCluster);

        // Swap partitions between nodes!
        returnCluster = createUpdatedCluster(returnCluster,
                                             nodeIdA,
                                             Lists.newArrayList(partitionIdB));
        returnCluster = createUpdatedCluster(returnCluster,
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

        Cluster returnCluster = copyCluster(targetCluster);
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
        Set<Integer> zoneIds = targetCluster.getZoneIds();
        Cluster returnCluster = copyCluster(targetCluster);

        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs);

        int successes = 0;
        for(int i = 0; i < randomSwapAttempts; i++) {
            for(Integer zoneId: zoneIds) {
                Cluster shuffleResults = swapRandomPartitionsWithinZone(returnCluster, zoneId);
                double nextMaxMinRatio = analyzeBalance(shuffleResults, storeDefs);
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

        Cluster returnCluster = copyCluster(targetCluster);
        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs);
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
                        double swapMaxMinRatio = analyzeBalance(swapResult, storeDefs);
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

        Cluster returnCluster = copyCluster(targetCluster);
        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs);
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
                    double swapMaxMinRatio = analyzeBalance(swapResult, storeDefs);
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
        Set<Integer> zoneIds = targetCluster.getZoneIds();
        Cluster returnCluster = copyCluster(targetCluster);

        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs);

        for(int i = 0; i < greedyAttempts; i++) {
            for(Integer zoneId: zoneIds) {
                System.out.println("Greedy swap attempt: zone " + zoneId + " , attempt " + i
                                   + " of " + greedyAttempts);
                Cluster shuffleResults = swapGreedyRandomPartitionsWithinZone(returnCluster,
                                                                              zoneId,
                                                                              greedySwapMaxPartitionsPerNode,
                                                                              greedySwapMaxPartitionsPerZone,
                                                                              storeDefs);
                double nextMaxMinRatio = analyzeBalance(shuffleResults, storeDefs);

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
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param nodeId Node on which this is running ( generally stealer node )
     * @param key The key to check
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @param cluster Cluster metadata
     * @param storeDef The store definition
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(int nodeId,
                                                     byte[] key,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList,
                                                     Cluster cluster,
                                                     StoreDefinition storeDef) {
        boolean checkResult = false;
        if(storeDef.getRoutingStrategyType().equals(RoutingStrategyType.TO_ALL_STRATEGY)
           || storeDef.getRoutingStrategyType()
                      .equals(RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY)) {
            checkResult = true;
        } else {
            List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                             cluster)
                                                                      .getPartitionList(key);
            List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
            checkResult = checkKeyBelongsToPartition(keyPartitions,
                                                     nodePartitions,
                                                     replicaToPartitionList);
        }
        return checkResult;
    }

    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param keyPartitions Preference list of the key
     * @param nodePartitions Partition list on this node
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(List<Integer> keyPartitions,
                                                     List<Integer> nodePartitions,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList) {
        // Check for null
        replicaToPartitionList = Utils.notNull(replicaToPartitionList);

        for(int replicaNum = 0; replicaNum < keyPartitions.size(); replicaNum++) {

            // If this partition belongs to node partitions + master is in
            // replicaToPartitions list -> match
            if(nodePartitions.contains(keyPartitions.get(replicaNum))) {
                List<Integer> partitionsToMove = replicaToPartitionList.get(replicaNum);
                if(partitionsToMove != null && partitionsToMove.size() > 0) {
                    if(partitionsToMove.contains(keyPartitions.get(0))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Given a key and a list of steal infos give back a list of stealer node
     * ids which will steal this.
     * 
     * @param key Byte array of key
     * @param stealerNodeToMappingTuples Pairs of stealer node id to their
     *        corresponding [ partition - replica ] tuples
     * @param cluster Cluster metadata
     * @param storeDef Store definitions
     * @return List of node ids
     */
    public static List<Integer> checkKeyBelongsToPartition(byte[] key,
                                                           Set<Pair<Integer, HashMap<Integer, List<Integer>>>> stealerNodeToMappingTuples,
                                                           Cluster cluster,
                                                           StoreDefinition storeDef) {
        List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                         cluster)
                                                                  .getPartitionList(key);
        List<Integer> nodesToPush = Lists.newArrayList();
        for(Pair<Integer, HashMap<Integer, List<Integer>>> stealNodeToMap: stealerNodeToMappingTuples) {
            List<Integer> nodePartitions = cluster.getNodeById(stealNodeToMap.getFirst())
                                                  .getPartitionIds();
            if(checkKeyBelongsToPartition(keyPartitions, nodePartitions, stealNodeToMap.getSecond())) {
                nodesToPush.add(stealNodeToMap.getFirst());
            }
        }
        return nodesToPush;
    }

    /***
     * 
     * @return true if the partition belongs to the node with given replicatype
     */
    public static boolean checkPartitionBelongsToNode(int partition,
                                                      int replicaType,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        boolean belongs = false;
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partition);
        // validate replicaType
        if(replicaType < replicatingPartitions.size()) {
            // check if the replicaType'th partition in the replicating list,
            // belongs to the given node
            if(nodePartitions.contains(replicatingPartitions.get(replicaType)))
                belongs = true;
        }

        return belongs;
    }

    /**
     * 
     * @param key
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return true if the key belongs to the node as some replica
     */
    public static boolean checkKeyBelongsToNode(byte[] key,
                                                int nodeId,
                                                Cluster cluster,
                                                StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getPartitionList(key);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    /***
     * Checks if a given partition is stored in the node. (It can be primary or
     * a secondary)
     * 
     * @param partition
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return
     */
    public static boolean checkPartitionBelongsToNode(int partition,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partition);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    /**
     * Check the execution state of the server by checking the state of
     * {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are all in normal state (
     * {@link VoldemortState#NORMAL_SERVER}).
     * 
     * @param cluster Cluster metadata whose nodes we are checking
     * @param adminClient Admin client used to query
     * @throws VoldemortRebalancingException if any node is not in normal state
     */
    public static void validateClusterState(final Cluster cluster, final AdminClient adminClient) {
        for(Node node: cluster.getNodes()) {
            Versioned<VoldemortState> versioned = adminClient.rebalanceOps.getRemoteServerState(node.getId());

            if(!VoldemortState.NORMAL_SERVER.equals(versioned.getValue())) {
                throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                        + node.getId() + " (" + node.getHost()
                                                        + ") is not in normal state, but in "
                                                        + versioned.getValue());
            } else {
                if(logger.isInfoEnabled()) {
                    logger.info("Node " + node.getId() + " (" + node.getHost()
                                + ") is ready for rebalance.");
                }
            }
        }
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
     * Given the current cluster and a target cluster, generates a cluster with
     * new nodes ( which in turn contain empty partition lists )
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster Target cluster metadata
     * @return Returns a new cluster which contains nodes of the current cluster
     *         + new nodes
     */
    public static Cluster getClusterWithNewNodes(Cluster currentCluster, Cluster targetCluster) {
        ArrayList<Node> newNodes = new ArrayList<Node>();
        for(Node node: targetCluster.getNodes()) {
            if(!containsNode(currentCluster, node.getId())) {
                newNodes.add(updateNode(node, new ArrayList<Integer>()));
            }
        }
        return updateCluster(currentCluster, newNodes);
    }

    /**
     * Concatenates the list of current nodes in the given cluster with the new
     * nodes provided and returns an updated cluster metadata. <br>
     * If the nodes being updated already exist in the current metadata, we take
     * the updated ones
     * 
     * @param currentCluster The current cluster metadata
     * @param updatedNodeList The list of new nodes to be added
     * @return New cluster metadata containing both the sets of nodes
     */
    public static Cluster updateCluster(Cluster currentCluster, List<Node> updatedNodeList) {
        List<Node> newNodeList = new ArrayList<Node>(updatedNodeList);
        for(Node currentNode: currentCluster.getNodes()) {
            if(!updatedNodeList.contains(currentNode))
                newNodeList.add(currentNode);
        }

        Collections.sort(newNodeList);
        return new Cluster(currentCluster.getName(),
                           newNodeList,
                           Lists.newArrayList(currentCluster.getZones()));
    }

    /**
     * Creates a new cluster object that is a copy of currentCluster.
     * 
     * @param currentCluster The current cluster metadata
     * @return New cluster metadata which is copy of currentCluster
     */
    public static Cluster copyCluster(Cluster currentCluster) {
        return new Cluster(currentCluster.getName(),
                           new ArrayList<Node>(currentCluster.getNodes()),
                           new ArrayList<Zone>(currentCluster.getZones()));
    }

    /**
     * Given a cluster and a node id checks if the node exists
     * 
     * @param cluster The cluster metadata to check in
     * @param nodeId The node id to search for
     * @return True if cluster contains the node id, else false
     */
    public static boolean containsNode(Cluster cluster, int nodeId) {
        try {
            cluster.getNodeById(nodeId);
            return true;
        } catch(VoldemortException e) {
            return false;
        }
    }

    /**
     * Given a preference list and a node id, check if any one of the partitions
     * is on the node in picture
     * 
     * @param cluster Cluster metadata
     * @param preferenceList Preference list of partition ids
     * @param nodeId Node id which we are checking for
     * @return True if the preference list contains a node whose id = nodeId
     */
    public static boolean containsPreferenceList(Cluster cluster,
                                                 List<Integer> preferenceList,
                                                 int nodeId) {

        for(int partition: preferenceList) {
            if(RebalanceUtils.getNodeByPartitionId(cluster, partition).getId() == nodeId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the existing cluster such that we remove partitions mentioned
     * from the stealer node and add them to the donor node
     * 
     * @param currentCluster Existing cluster metadata. Both stealer and donor
     *        node should already exist in this metadata
     * @param stealerNodeId Id of node from which we are stealing the partitions
     * @param donatedPartitions List of partitions we are moving
     * @param partitionList List of partitions we are moving
     * @return Updated cluster metadata
     */
    public static Cluster createUpdatedCluster(Cluster currentCluster,
                                               int stealerNodeId,
                                               List<Integer> donatedPartitions) {

        // Clone the cluster
        ClusterMapper mapper = new ClusterMapper();
        Cluster updatedCluster = mapper.readCluster(new StringReader(mapper.writeCluster(currentCluster)));

        // Go over every donated partition one by one
        for(int donatedPartition: donatedPartitions) {

            // Gets the donor Node that owns this donated partition
            Node donorNode = RebalanceUtils.getNodeByPartitionId(updatedCluster, donatedPartition);
            Node stealerNode = updatedCluster.getNodeById(stealerNodeId);

            if(donorNode == stealerNode) {
                // Moving to the same location = No-op
                continue;
            }

            // Update the list of partitions for this node
            donorNode = RebalanceUtils.removePartitionToNode(donorNode, donatedPartition);
            stealerNode = RebalanceUtils.addPartitionToNode(stealerNode, donatedPartition);

            // Sort the nodes
            updatedCluster = updateCluster(updatedCluster,
                                           Lists.newArrayList(donorNode, stealerNode));

        }

        return updatedCluster;
    }

    /**
     * Creates a replica of the node with the new partitions list
     * 
     * @param node The node whose replica we are creating
     * @param partitionsList The new partitions list
     * @return Replica of node with new partitions list
     */
    public static Node updateNode(Node node, List<Integer> partitionsList) {
        return new Node(node.getId(),
                        node.getHost(),
                        node.getHttpPort(),
                        node.getSocketPort(),
                        node.getAdminPort(),
                        node.getZoneId(),
                        partitionsList);
    }

    /**
     * Add a partition to the node provided
     * 
     * @param node The node to which we'll add the partition
     * @param donatedPartition The partition to add
     * @return The new node with the new partition
     */
    public static Node addPartitionToNode(final Node node, Integer donatedPartition) {
        return addPartitionToNode(node, Sets.newHashSet(donatedPartition));
    }

    /**
     * Remove a partition from the node provided
     * 
     * @param node The node from which we're removing the partition
     * @param donatedPartition The partitions to remove
     * @return The new node without the partition
     */
    public static Node removePartitionToNode(final Node node, Integer donatedPartition) {
        return removePartitionToNode(node, Sets.newHashSet(donatedPartition));
    }

    /**
     * Add the set of partitions to the node provided
     * 
     * @param node The node to which we'll add the partitions
     * @param donatedPartitions The list of partitions to add
     * @return The new node with the new partitions
     */
    public static Node addPartitionToNode(final Node node, final Set<Integer> donatedPartitions) {
        List<Integer> deepCopy = new ArrayList<Integer>(node.getPartitionIds());
        deepCopy.addAll(donatedPartitions);
        Collections.sort(deepCopy);
        return updateNode(node, deepCopy);
    }

    /**
     * Remove the set of partitions from the node provided
     * 
     * @param node The node from which we're removing the partitions
     * @param donatedPartitions The list of partitions to remove
     * @return The new node without the partitions
     */
    public static Node removePartitionToNode(final Node node, final Set<Integer> donatedPartitions) {
        List<Integer> deepCopy = new ArrayList<Integer>(node.getPartitionIds());
        deepCopy.removeAll(donatedPartitions);
        return updateNode(node, deepCopy);
    }

    /**
     * Given the cluster metadata returns a mapping of partition to node
     * 
     * @param currentCluster Cluster metadata
     * @return Map of partition id to node id
     */
    public static Map<Integer, Integer> getCurrentPartitionMapping(Cluster currentCluster) {

        Map<Integer, Integer> partitionToNode = new LinkedHashMap<Integer, Integer>();

        for(Node node: currentCluster.getNodes()) {
            for(Integer partition: node.getPartitionIds()) {
                // Check if partition is on another node
                Integer previousRegisteredNodeId = partitionToNode.get(partition);
                if(previousRegisteredNodeId != null) {
                    throw new IllegalArgumentException("Partition id " + partition
                                                       + " found on two nodes : " + node.getId()
                                                       + " and " + previousRegisteredNodeId);
                }

                partitionToNode.put(partition, node.getId());
            }
        }

        return partitionToNode;
    }

    /**
     * Attempt to propagate a cluster definition to all nodes. Also rollback is
     * in place in case one of them fails
     * 
     * @param adminClient {@link voldemort.client.protocol.admin.AdminClient}
     *        instance to use.
     * @param cluster Cluster definition to propagate
     */
    public static void propagateCluster(AdminClient adminClient, Cluster cluster) {

        // Contains a mapping of node id to the existing cluster definition
        HashMap<Integer, Cluster> currentClusters = Maps.newHashMap();

        Versioned<Cluster> latestCluster = new Versioned<Cluster>(cluster);
        ArrayList<Versioned<Cluster>> clusterList = new ArrayList<Versioned<Cluster>>();
        clusterList.add(latestCluster);

        for(Node node: cluster.getNodes()) {
            try {
                Versioned<Cluster> versionedCluster = adminClient.metadataMgmtOps.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();

                // Update the current cluster information
                currentClusters.put(node.getId(), versionedCluster.getValue());

                if(null != newClock && !clusterList.contains(versionedCluster)) {
                    // check no two clocks are concurrent.
                    checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occurred occurred = newClock.compare(latestCluster.getVersion());
                    if(Occurred.AFTER.equals(occurred))
                        latestCluster = versionedCluster;
                }

            } catch(Exception e) {
                throw new VoldemortException("Failed to get cluster version from node "
                                             + node.getId(), e);
            }
        }

        // Vector clock to propagate
        VectorClock latestClock = ((VectorClock) latestCluster.getVersion()).incremented(0,
                                                                                         System.currentTimeMillis());

        // Alright, now try updating the values...
        Set<Integer> completedNodeIds = Sets.newHashSet();
        try {
            for(Node node: cluster.getNodes()) {
                logger.info("Updating cluster definition on remote node " + node);
                adminClient.metadataMgmtOps.updateRemoteCluster(node.getId(), cluster, latestClock);
                logger.info("Updated cluster definition " + cluster + " on remote node "
                            + node.getId());
                completedNodeIds.add(node.getId());
            }
        } catch(VoldemortException e) {
            // Fail early...
            for(Integer completedNodeId: completedNodeIds) {
                try {
                    adminClient.metadataMgmtOps.updateRemoteCluster(completedNodeId,
                                                                    currentClusters.get(completedNodeId),
                                                                    latestClock);
                } catch(VoldemortException exception) {
                    logger.error("Could not revert cluster metadata back on node "
                                 + completedNodeId);
                }
            }
            throw e;
        }

    }

    /**
     * For a particular stealer node find all the "primary" <replica, partition>
     * tuples it will steal. In other words, expect the "replica" part to be 0
     * always.
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param targetCluster The target cluster definition
     * @param stealNodeId Node id of the stealer node
     * @return Returns a list of primary partitions which this stealer node will
     *         get
     */
    public static List<Integer> getStolenPrimaryPartitions(final Cluster currentCluster,
                                                           final Cluster targetCluster,
                                                           final int stealNodeId) {
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return targetList;
    }

    /**
     * Find all [replica_type, partition] tuples to be stolen
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster Target cluster metadata
     * @param storeDef Store Definition
     * @return Map of stealer node id to sets of [ replica_type, partition ]
     *         tuples
     */
    public static Map<Integer, Set<Pair<Integer, Integer>>> getStolenPartitionTuples(final Cluster currentCluster,
                                                                                     final Cluster targetCluster,
                                                                                     final StoreDefinition storeDef) {
        Map<Integer, Set<Pair<Integer, Integer>>> currentNodeIdToReplicas = getNodeIdToAllPartitions(currentCluster,
                                                                                                     storeDef,
                                                                                                     true);
        Map<Integer, Set<Pair<Integer, Integer>>> targetNodeIdToReplicas = getNodeIdToAllPartitions(targetCluster,
                                                                                                    storeDef,
                                                                                                    true);

        Map<Integer, Set<Pair<Integer, Integer>>> stealerNodeToStolenPartitionTuples = Maps.newHashMap();
        for(int stealerId: getNodeIds(Lists.newArrayList(targetCluster.getNodes()))) {
            Set<Pair<Integer, Integer>> clusterStealerReplicas = currentNodeIdToReplicas.get(stealerId);
            Set<Pair<Integer, Integer>> targetStealerReplicas = targetNodeIdToReplicas.get(stealerId);

            Set<Pair<Integer, Integer>> diff = Utils.getAddedInTarget(clusterStealerReplicas,
                                                                      targetStealerReplicas);

            if(diff != null && diff.size() > 0) {
                stealerNodeToStolenPartitionTuples.put(stealerId, diff);
            }
        }
        return stealerNodeToStolenPartitionTuples;
    }

    /**
     * Given a mapping of existing node ids to their partition tuples and
     * another new set of node ids to partition tuples, combines them together
     * and puts it into the existing partition tuples
     * 
     * @param existingPartitionTuples Existing partition tuples ( Will include
     *        the new partition tuples at the end of this function )
     * @param newPartitionTuples New partition tuples
     */
    public static void combinePartitionTuples(Map<Integer, Set<Pair<Integer, Integer>>> existingPartitionTuples,
                                              Map<Integer, Set<Pair<Integer, Integer>>> newPartitionTuples) {

        for(int nodeId: newPartitionTuples.keySet()) {
            Set<Pair<Integer, Integer>> tuples = null;
            if(existingPartitionTuples.containsKey(nodeId)) {
                tuples = existingPartitionTuples.get(nodeId);
            } else {
                tuples = Sets.newHashSet();
                existingPartitionTuples.put(nodeId, tuples);
            }
            tuples.addAll(newPartitionTuples.get(nodeId));
        }
    }

    /**
     * For a particular cluster creates a mapping of node id to their
     * corresponding list of [ replicaType, partition ] tuple
     * 
     * @param cluster The cluster metadata
     * @param storeDef The store definition
     * @param includePrimary Include the primary partition?
     * @return Map of node id to set of [ replicaType, partition ] tuple
     */
    public static Map<Integer, Set<Pair<Integer, Integer>>> getNodeIdToAllPartitions(final Cluster cluster,
                                                                                     final StoreDefinition storeDef,
                                                                                     boolean includePrimary) {
        final RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                   cluster);

        final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToReplicas = new HashMap<Integer, Set<Pair<Integer, Integer>>>();
        final Map<Integer, Integer> partitionToNodeIdMap = getCurrentPartitionMapping(cluster);

        // Map initialization.
        for(Node node: cluster.getNodes()) {
            nodeIdToReplicas.put(node.getId(), new HashSet<Pair<Integer, Integer>>());
        }

        // Loops through all nodes
        for(Node node: cluster.getNodes()) {

            // Gets the partitions that this node was configured with.
            for(Integer primary: node.getPartitionIds()) {

                // Gets the list of replicating partitions.
                List<Integer> replicaPartitionList = routingStrategy.getReplicatingPartitionList(primary);

                if(replicaPartitionList.size() != storeDef.getReplicationFactor())
                    throw new VoldemortException("Number of replicas returned ("
                                                 + replicaPartitionList.size()
                                                 + ") is less than the required replication factor ("
                                                 + storeDef.getReplicationFactor() + ")");

                int replicaType = 0;
                if(!includePrimary) {
                    replicaPartitionList.remove(primary);
                    replicaType = 1;
                }

                // Get the node that this replicating partition belongs to.
                for(Integer replicaPartition: replicaPartitionList) {
                    Integer replicaNodeId = partitionToNodeIdMap.get(replicaPartition);

                    // The replicating node will have a copy of primary.
                    nodeIdToReplicas.get(replicaNodeId).add(Pair.create(replicaType, primary));

                    replicaType++;
                }
            }
        }
        return nodeIdToReplicas;
    }

    /**
     * Given the initial and final cluster dumps it into the output directory
     * 
     * @param initialCluster Initial cluster metadata
     * @param finalCluster Final cluster metadata
     * @param outputDir Output directory where to dump this file
     * @throws IOException
     */
    public static void dumpCluster(Cluster initialCluster, Cluster finalCluster, File outputDir) {

        // Create the output directory if it doesn't exist
        if(!outputDir.exists()) {
            Utils.mkdirs(outputDir);
        }

        // Get the file paths
        File initialClusterFile = new File(outputDir, initialClusterFileName);
        File finalClusterFile = new File(outputDir, finalClusterFileName);

        // Write the output
        ClusterMapper mapper = new ClusterMapper();
        try {
            FileUtils.writeStringToFile(initialClusterFile, mapper.writeCluster(initialCluster));
            FileUtils.writeStringToFile(finalClusterFile, mapper.writeCluster(finalCluster));
        } catch(IOException e) {
            logger.error("Error writing cluster metadata to file");
        }

    }

    /**
     * Print log to the following logger ( Info level )
     * 
     * @param taskId Task id
     * @param logger Logger class
     * @param message The message to print
     */
    public static void printLog(int taskId, Logger logger, String message) {
        logger.info("Task id [" + Integer.toString(taskId) + "] " + message);
    }

    /**
     * Print log to the following logger ( Error level )
     * 
     * @param taskId Stealer node id
     * @param logger Logger class
     * @param message The message to print
     */
    public static void printErrorLog(int taskId, Logger logger, String message, Exception e) {
        if(e == null) {
            logger.error("Task id " + Integer.toString(taskId) + "] " + message);
        } else {
            logger.error("Task id " + Integer.toString(taskId) + "] " + message, e);
        }
    }

    /**
     * Returns the Node associated to the provided partition.
     * 
     * @param cluster The cluster in which to find the node
     * @param partitionId Partition id for which we want the corresponding node
     * @return Node that owns the partition
     */
    public static Node getNodeByPartitionId(Cluster cluster, int partitionId) {
        for(Node node: cluster.getNodes()) {
            if(node.getPartitionIds().contains(partitionId)) {
                return node;
            }
        }
        return null;
    }

    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig,
                                                    Cluster cluster,
                                                    int numConnPerNode) {
        AdminClientConfig config = new AdminClientConfig().setMaxConnectionsPerNode(numConnPerNode)
                                                          .setAdminConnectionTimeoutSec(voldemortConfig.getAdminConnectionTimeout())
                                                          .setAdminSocketTimeoutSec(voldemortConfig.getAdminSocketTimeout())
                                                          .setAdminSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new AdminClient(cluster, config);
    }

    /**
     * Given the cluster metadata and admin client, retrieves the list of store
     * definitions.
     * 
     * <br>
     * 
     * It also checks if the store definitions are consistent across the cluster
     * 
     * @param cluster The cluster metadata
     * @param adminClient The admin client to use to retrieve the store
     *        definitions
     * @return List of store definitions
     */
    public static List<StoreDefinition> getStoreDefinition(Cluster cluster, AdminClient adminClient) {
        List<StoreDefinition> storeDefs = null;
        for(Node node: cluster.getNodes()) {
            List<StoreDefinition> storeDefList = adminClient.metadataMgmtOps.getRemoteStoreDefList(node.getId())
                                                                            .getValue();
            if(storeDefs == null) {
                storeDefs = storeDefList;
            } else {

                // Compare against the previous store definitions
                if(!Utils.compareList(storeDefs, storeDefList)) {
                    throw new VoldemortException("Store definitions on node " + node.getId()
                                                 + " does not match those on other nodes");
                }
            }
        }

        if(storeDefs == null) {
            throw new VoldemortException("Could not retrieve list of store definitions correctly");
        } else {
            return storeDefs;
        }
    }

    /**
     * Given a list of store definitions, makes sure that rebalance supports all
     * of them. If not it throws an error.
     * 
     * @param storeDefList List of store definitions
     * @return Filtered list of store definitions which rebalancing supports
     */
    public static List<StoreDefinition> validateRebalanceStore(List<StoreDefinition> storeDefList) {
        List<StoreDefinition> returnList = new ArrayList<StoreDefinition>(storeDefList.size());

        for(StoreDefinition def: storeDefList) {
            if(!def.isView() && !canRebalanceList.contains(def.getType())) {
                throw new VoldemortException("Rebalance does not support rebalancing of stores of type "
                                             + def.getType() + " - " + def.getName());
            } else if(!def.isView()) {
                returnList.add(def);
            } else {
                logger.debug("Ignoring view " + def.getName() + " for rebalancing");
            }
        }
        return returnList;
    }

    /**
     * Given a list of store definitions, cluster and admin client returns a
     * boolean indicating if all RO stores are in the correct format.
     * 
     * <br>
     * 
     * This function also takes into consideration nodes which are being
     * bootstrapped for the first time, in which case we can safely ignore
     * checking them ( as they will have default to ro0 )
     * 
     * @param cluster Cluster metadata
     * @param storeDefs Complete list of store definitions
     * @param adminClient Admin client
     */
    public static void validateReadOnlyStores(Cluster cluster,
                                              List<StoreDefinition> storeDefs,
                                              AdminClient adminClient) {
        List<StoreDefinition> readOnlyStores = filterStores(storeDefs, true);

        if(readOnlyStores.size() == 0) {
            // No read-only stores
            return;
        }

        List<String> storeNames = getStoreNames(readOnlyStores);
        for(Node node: cluster.getNodes()) {
            if(node.getNumberOfPartitions() != 0) {
                for(Entry<String, String> storeToStorageFormat: adminClient.readonlyOps.getROStorageFormat(node.getId(),
                                                                                                           storeNames)
                                                                                       .entrySet()) {
                    if(storeToStorageFormat.getValue()
                                           .compareTo(ReadOnlyStorageFormat.READONLY_V2.getCode()) != 0) {
                        throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                                + node.getId() + " has store "
                                                                + storeToStorageFormat.getKey()
                                                                + " not using format "
                                                                + ReadOnlyStorageFormat.READONLY_V2);
                    }
                }
            }
        }
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
     * @return Returns a string representation of the cluster
     */
    public static String printMap(final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Integer, Set<Pair<Integer, Integer>>> entry: nodeIdToAllPartitions.entrySet()) {
            final Integer nodeId = entry.getKey();
            final Set<Pair<Integer, Integer>> allPartitions = entry.getValue();

            final HashMap<Integer, List<Integer>> replicaTypeToPartitions = flattenPartitionTuples(allPartitions);

            // Put into sorted key order such that primary replicas occur before
            // secondary replicas and so on...
            final TreeMap<Integer, List<Integer>> sortedReplicaTypeToPartitions = new TreeMap<Integer, List<Integer>>(replicaTypeToPartitions);

            sb.append(nodeId);
            if(replicaTypeToPartitions.size() > 0) {
                for(Entry<Integer, List<Integer>> partitions: sortedReplicaTypeToPartitions.entrySet()) {
                    Collections.sort(partitions.getValue());
                    sb.append(" - " + partitions.getValue());
                }
            } else {
                sb.append(" - empty");
            }
            sb.append(Utils.NEWLINE);
        }
        return sb.toString();
    }

    /**
     * Given a list of tuples of [replica_type, partition], flattens it and
     * generates a map of replica_type to partition mapping
     * 
     * @param partitionTuples Set of <replica_type, partition> tuples
     * @return Map of replica_type to set of partitions
     */
    public static HashMap<Integer, List<Integer>> flattenPartitionTuples(Set<Pair<Integer, Integer>> partitionTuples) {
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

    /**
     * Given a map of replica_type to partition mapping gives back a set of
     * tuples of [replica_type, partition]
     * 
     * @param replicaToPartitionList Map of replica_type to set of partitions
     * @return Set of <replica_type, partition> tuples
     */
    public static Set<Pair<Integer, Integer>> flattenPartitionTuples(HashMap<Integer, List<Integer>> replicaToPartitionList) {
        Set<Pair<Integer, Integer>> partitionTuples = Sets.newHashSet();

        for(Entry<Integer, List<Integer>> entry: replicaToPartitionList.entrySet()) {
            for(Iterator<Integer> iter = entry.getValue().iterator(); iter.hasNext();) {
                partitionTuples.add(new Pair<Integer, Integer>(entry.getKey(), iter.next()));
            }
        }

        return partitionTuples;
    }

    /**
     * Given a list of node plans flattens it into a list of partitions info
     * 
     * @param rebalanceNodePlanList Complete list of rebalance node plan
     * @return Flattened list of partition plans
     */
    public static List<RebalancePartitionsInfo> flattenNodePlans(List<RebalanceNodePlan> rebalanceNodePlanList) {
        List<RebalancePartitionsInfo> list = new ArrayList<RebalancePartitionsInfo>();
        for(RebalanceNodePlan rebalanceNodePlan: rebalanceNodePlanList) {
            for(final RebalancePartitionsInfo stealInfo: rebalanceNodePlan.getRebalanceTaskList()) {
                list.add(stealInfo);
            }
        }
        return list;
    }

    /**
     * Given a set of [ replica, partition ] tuples, flatten it to retrieve only
     * the partitions
     * 
     * @param tuples The [ replica, partition ] tuples
     * @return List of partitions
     */
    public static List<Integer> getPartitionsFromTuples(Set<Pair<Integer, Integer>> tuples) {
        List<Integer> partitions = Lists.newArrayList();

        if(tuples != null) {
            for(Pair<Integer, Integer> tuple: tuples) {
                partitions.add(tuple.getSecond());
            }
        }
        return partitions;
    }

    /**
     * Given a list of partition plans and a set of stores, copies the store
     * names to every individual plan and creates a new list
     * 
     * @param existingPlanList Existing partition plan list
     * @param storeDefs List of store names we are rebalancing
     * @return List of updated partition plan
     */
    public static List<RebalancePartitionsInfo> filterPartitionPlanWithStores(List<RebalancePartitionsInfo> existingPlanList,
                                                                              List<StoreDefinition> storeDefs) {
        List<RebalancePartitionsInfo> plans = Lists.newArrayList();
        List<String> storeNames = getStoreNames(storeDefs);

        for(RebalancePartitionsInfo existingPlan: existingPlanList) {
            RebalancePartitionsInfo info = RebalancePartitionsInfo.create(existingPlan.toJsonString());

            // Filter the plans only for stores given
            HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartitions = info.getStoreToReplicaToAddPartitionList();
            HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToDeletePartitions = info.getStoreToReplicaToDeletePartitionList();

            HashMap<String, HashMap<Integer, List<Integer>>> newStoreToReplicaToAddPartitions = Maps.newHashMap();
            HashMap<String, HashMap<Integer, List<Integer>>> newStoreToReplicaToDeletePartitions = Maps.newHashMap();
            for(String storeName: storeNames) {
                if(storeToReplicaToAddPartitions.containsKey(storeName))
                    newStoreToReplicaToAddPartitions.put(storeName,
                                                         storeToReplicaToAddPartitions.get(storeName));
                if(storeToReplicaToDeletePartitions.containsKey(storeName))
                    newStoreToReplicaToDeletePartitions.put(storeName,
                                                            storeToReplicaToDeletePartitions.get(storeName));
            }
            info.setStoreToReplicaToAddPartitionList(newStoreToReplicaToAddPartitions);
            info.setStoreToReplicaToDeletePartitionList(newStoreToReplicaToDeletePartitions);

            plans.add(info);
        }

        return plans;
    }

    /**
     * Given a list of partition infos, generates a map of stealer / donor node
     * to list of partition infos
     * 
     * @param rebalancePartitionPlanList Complete list of partition plans
     * @param groupByStealerNode Boolean indicating if we want to group by
     *        stealer node ( or donor node )
     * @return Flattens it into a map on a per node basis
     */
    public static HashMap<Integer, List<RebalancePartitionsInfo>> groupPartitionsInfoByNode(List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                                                                            boolean groupByStealerNode) {
        HashMap<Integer, List<RebalancePartitionsInfo>> nodeToPartitionsInfo = Maps.newHashMap();
        if(rebalancePartitionPlanList != null) {
            for(RebalancePartitionsInfo partitionInfo: rebalancePartitionPlanList) {
                int nodeId = groupByStealerNode ? partitionInfo.getStealerId()
                                               : partitionInfo.getDonorId();
                List<RebalancePartitionsInfo> partitionInfos = nodeToPartitionsInfo.get(nodeId);
                if(partitionInfos == null) {
                    partitionInfos = Lists.newArrayList();
                    nodeToPartitionsInfo.put(nodeId, partitionInfos);
                }
                partitionInfos.add(partitionInfo);
            }
        }
        return nodeToPartitionsInfo;
    }

    /**
     * Given a store name and a list of store definitions, returns the
     * appropriate store definition ( if it exists )
     * 
     * @param storeDefs List of store definitions
     * @param storeName The store name whose store definition is required
     * @return The store definition
     */
    public static StoreDefinition getStoreDefinitionWithName(List<StoreDefinition> storeDefs,
                                                             String storeName) {
        StoreDefinition def = null;
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().compareTo(storeName) == 0) {
                def = storeDef;
                break;
            }
        }

        if(def == null) {
            throw new VoldemortException("Could not find store " + storeName);
        }
        return def;
    }

    /**
     * Given a list of store definitions, filters the list depending on the
     * boolean
     * 
     * @param storeDefs Complete list of store definitions
     * @param isReadOnly Boolean indicating whether filter on read-only or not?
     * @return List of filtered store definition
     */
    public static List<StoreDefinition> filterStores(List<StoreDefinition> storeDefs,
                                                     final boolean isReadOnly) {
        List<StoreDefinition> filteredStores = Lists.newArrayList();
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getType().equals(ReadOnlyStorageConfiguration.TYPE_NAME) == isReadOnly) {
                filteredStores.add(storeDef);
            }
        }
        return filteredStores;
    }

    /**
     * Given a list of store definitions return a list of store names
     * 
     * @param storeDefList The list of store definitions
     * @return Returns a list of store names
     */
    public static List<String> getStoreNames(List<StoreDefinition> storeDefList) {
        List<String> storeList = new ArrayList<String>();
        for(StoreDefinition def: storeDefList) {
            storeList.add(def.getName());
        }
        return storeList;
    }

    /**
     * Given a list of nodes, retrieves the list of node ids
     * 
     * @param nodes The list of nodes
     * @return Returns a list of node ids
     */
    public static List<Integer> getNodeIds(List<Node> nodes) {
        List<Integer> nodeIds = new ArrayList<Integer>(nodes.size());
        for(Node node: nodes) {
            nodeIds.add(node.getId());
        }
        return nodeIds;
    }

    /**
     * Wait to shutdown service
     * 
     * @param executorService Executor service to shutdown
     * @param timeOutSec Time we wait for
     */
    public static void executorShutDown(ExecutorService executorService, long timeOutSec) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(timeOutSec, TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service.", e);
        }
    }

    public static ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }
}
