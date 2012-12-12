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

    // TODO: balanceTarget
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
     * @param tries Number of times we'll try to optimize the metadata
     *        generation
     * @param keepPrimaryPartitionsInSameZone Checks zone IDs of donor and
     *        stealer nodes and only allow moves within same zone.
     * @param permitCrossZoneMoves Expensive check to prevent primary partition
     *        moves that result in other partitions moving across zones.
     * @param varyNumPartitionsPerNode Allows number of partitions per node
     *        within a zone to vary by +-[0,varyNumPartitionsPerNode]. A value
     *        of zero means that all nodes within a zone should have a similar
     *        (within one) number of primary partitions.
     * @param enableRandomSwaps Tries randomly swapping partitions within zones
     *        to improve balance of cluster.
     * @param randomSwapAttempts Number of random swaps to attempt
     * @param randomSwapSuccesses Early termination condition on random
     *        swapping: once swapSuccesses attempts are successful, stop
     *        randomly swapping partitions.
     * @param maxContiguousPartitionsPerZone
     */
    public static void balanceTargetCluster(final Cluster currentCluster,
                                            final Cluster targetCluster,
                                            final List<StoreDefinition> storeDefs,
                                            final String outputDir,
                                            final int tries,
                                            final boolean keepPrimaryPartitionsInSameZone,
                                            final boolean permitCrossZoneMoves,
                                            final int varyNumPartitionsPerNode,
                                            final boolean enableRandomSwaps,
                                            final int randomSwapAttempts,
                                            final int randomSwapSuccesses,
                                            final boolean enableGreedySwaps,
                                            final int greedySwapAttempts,
                                            final int greedySwapMaxPartitionsPerNode,
                                            final int greedySwapMaxPartitionsPerZone,
                                            final int maxContiguousPartitionsPerZone) {
        List<ByteArray> keys = KeyDistributionGenerator.generateKeys(KeyDistributionGenerator.DEFAULT_NUM_KEYS);
        Cluster minCluster = targetCluster;
        int minMoves = Integer.MAX_VALUE;
        double minMaxMinRatio = Double.MAX_VALUE;

        Cluster nextCluster;
        int xzonePartitionsMoved;

        for(int numTries = 0; numTries < tries; numTries++) {
            nextCluster = targetCluster;
            xzonePartitionsMoved = 0;

            if(maxContiguousPartitionsPerZone > 0) {
                int contigPartitionsMoved = 0;
                do {
                    Pair<Cluster, Integer> contigPartPerZone = RebalanceUtils.balanceContiguousPartitionsPerZone(nextCluster,
                                                                                                                 maxContiguousPartitionsPerZone);
                    nextCluster = contigPartPerZone.getFirst();
                    contigPartitionsMoved = contigPartPerZone.getSecond();
                    xzonePartitionsMoved += contigPartPerZone.getSecond();

                    Pair<Cluster, Integer> balanceXZone = RebalanceUtils.balanceNumPartitionsPerZone(nextCluster);
                    nextCluster = balanceXZone.getFirst();
                    xzonePartitionsMoved += balanceXZone.getSecond();

                    System.out.println("Looping to evenly balance partitions across zones while limiting contiguous partitions: "
                                       + contigPartitionsMoved);
                } while(contigPartitionsMoved > 0);
            }

            Pair<Cluster, Integer> numPartPerZone = RebalanceUtils.balanceNumberOfPartitionsPerNode(nextCluster,
                                                                                                    storeDefs,
                                                                                                    keepPrimaryPartitionsInSameZone,
                                                                                                    permitCrossZoneMoves,
                                                                                                    varyNumPartitionsPerNode);
            nextCluster = numPartPerZone.getFirst();
            int currentMoves = xzonePartitionsMoved + numPartPerZone.getSecond();

            if(enableRandomSwaps) {
                Pair<Cluster, Integer> shuffleCluster = RebalanceUtils.randomShufflePartitions(nextCluster,
                                                                                               randomSwapAttempts,
                                                                                               randomSwapSuccesses,
                                                                                               storeDefs);
                nextCluster = shuffleCluster.getFirst();
                currentMoves += shuffleCluster.getSecond();
            }
            if(enableGreedySwaps) {
                Pair<Cluster, Integer> shuffleCluster = RebalanceUtils.greedyShufflePartitions(nextCluster,
                                                                                               greedySwapAttempts,
                                                                                               greedySwapMaxPartitionsPerNode,
                                                                                               greedySwapMaxPartitionsPerZone,
                                                                                               storeDefs);
                nextCluster = shuffleCluster.getFirst();
                currentMoves += shuffleCluster.getSecond();
            }

            double currentMaxMinRatio = analyzeBalance(nextCluster, storeDefs, false);
            System.out.println("Optimization number " + numTries + ": "
                               + numPartPerZone.getSecond() + " moves, " + currentMaxMinRatio
                               + " max/min ratio");
            System.out.println("Current min moves: " + minMoves + "; current max/min ratio: "
                               + minMaxMinRatio);

            if(currentMaxMinRatio <= minMaxMinRatio) {
                if(currentMoves > minMoves) {
                    System.out.println("Warning: the newly chosen cluster requires "
                                       + (currentMoves - minMoves) + " addition moves!");
                }
                minMoves = currentMoves;
                minMaxMinRatio = currentMaxMinRatio;
                minCluster = nextCluster;

                System.out.println("Current distribution");
                System.out.println(KeyDistributionGenerator.printOverallDistribution(currentCluster,
                                                                                     storeDefs,
                                                                                     keys));
                System.out.println("-------------------------\n");

                System.out.println("Target distribution");
                System.out.println(KeyDistributionGenerator.printOverallDistribution(minCluster,
                                                                                     storeDefs,
                                                                                     keys));
                System.out.println("=========================\n");
                // If output directory exists, output the optimized cluster
                if(outputDir != null) {
                    try {
                        FileUtils.writeStringToFile(new File(outputDir,
                                                             RebalanceUtils.finalClusterFileName
                                                                     + numTries),
                                                    new ClusterMapper().writeCluster(minCluster));
                    } catch(Exception e) {}
                }
            }
        }

        System.out.println("\n==========================");
        System.out.println("Final distribution");
        System.out.println(KeyDistributionGenerator.printOverallDistribution(minCluster,
                                                                             storeDefs,
                                                                             keys));
        System.out.println("=========================\n");

        analyzeBalance(minCluster, storeDefs, true);

        // If output directory exists, output the optimized cluster
        if(outputDir != null) {
            try {
                FileUtils.writeStringToFile(new File(outputDir, RebalanceUtils.finalClusterFileName),
                                            new ClusterMapper().writeCluster(minCluster));
            } catch(Exception e) {}
        }

        return;

    }

    /**
     * Outputs an analysis of how balanced the cluster is given the store defs
     * and a random sampling of keys.
     * 
     * @param currentCluster Current cluster metadata
     * @param storeDefs List of store definitions
     */
    public static double analyzeBalance(final Cluster currentCluster,
                                        final List<StoreDefinition> storeDefs,
                                        boolean verbose) {

        HashMap<StoreDefinition, Integer> uniqueStores = KeyDistributionGenerator.getUniqueStoreDefinitionsWithCounts(storeDefs);

        List<ByteArray> keys = KeyDistributionGenerator.generateKeys(KeyDistributionGenerator.DEFAULT_NUM_KEYS);
        KeyDistributionGenerator.getStdDeviation(KeyDistributionGenerator.generateOverallDistributionWithUniqueStores(currentCluster,
                                                                                                                      uniqueStores,
                                                                                                                      keys));
        if(verbose) {
            System.out.println();
            System.out.println(KeyDistributionGenerator.printOverallDistribution(currentCluster,
                                                                                 storeDefs,
                                                                                 keys));

            System.out.println();
            System.out.println("PARTITION DUMP");
        }

        Map<Integer, Integer> aggNodeIdToPartitionCount = Maps.newHashMap();
        List<Integer> nodeIds = new ArrayList<Integer>();

        for(StoreDefinition storeDefinition: uniqueStores.keySet()) {
            if(verbose) {
                System.out.println();
                System.out.println("Store exemplar: " + storeDefinition.getName());
                System.out.println("\tReplication factor: "
                                   + storeDefinition.getReplicationFactor());
                System.out.println("\tRouting strategy: "
                                   + storeDefinition.getRoutingStrategyType());
                System.out.println("\tThere are " + uniqueStores.get(storeDefinition)
                                   + " other similar stores.");
            }

            // Pairs of Integers are of <replica_type, partition_id>
            Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions = getNodeIdToAllPartitions(currentCluster,
                                                                                                       storeDefinition,
                                                                                                       true);
            // Get sorted NodeIds
            if(nodeIds.size() == 0) {
                nodeIds.addAll(nodeIdToAllPartitions.keySet());
                java.util.Collections.sort(nodeIds);
            }

            if(aggNodeIdToPartitionCount.size() == 0) {
                for(Integer nodeId: nodeIds) {
                    aggNodeIdToPartitionCount.put(nodeId, 0);
                }
            }

            Map<Integer, Integer> nodeIdToPartitionCount = Maps.newHashMap();

            // Print out all partitions, by replica type, per node
            if(verbose) {
                System.out.println();
                System.out.println("\tDetailed Dump:");
            }
            for(Integer nodeId: nodeIds) {
                if(verbose) {
                    System.out.println("\tNode ID: " + nodeId);
                }
                nodeIdToPartitionCount.put(nodeId, 0);
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
                    if(verbose) {
                        System.out.println("\t\t" + replicaType + " : " + partitions.size() + " : "
                                           + partitions.toString());
                    }
                    nodeIdToPartitionCount.put(nodeId, nodeIdToPartitionCount.get(nodeId)
                                                       + partitions.size());
                    replicaType++;
                }
            }

            if(verbose) {
                System.out.println();
                System.out.println("\tSummary Dump:");
            }
            for(Integer nodeId: nodeIds) {
                if(verbose) {
                    System.out.println("\tNode ID: " + nodeId + " : "
                                       + nodeIdToPartitionCount.get(nodeId));
                }
                aggNodeIdToPartitionCount.put(nodeId,
                                              aggNodeIdToPartitionCount.get(nodeId)
                                                      + (nodeIdToPartitionCount.get(nodeId) * uniqueStores.get(storeDefinition)));
            }

        }

        if(verbose) {
            System.out.println();
            System.out.println("AGGREGATE PARTITION COUNT (across all stores)");
        }
        int minVal = Integer.MAX_VALUE;
        int maxVal = Integer.MIN_VALUE;
        int aggCount = 0;
        for(Integer nodeId: nodeIds) {
            int curCount = aggNodeIdToPartitionCount.get(nodeId);
            if(verbose) {
                System.out.println("\tNode ID: " + nodeId + " : " + curCount);
            }
            aggCount += curCount;
            if(curCount > maxVal)
                maxVal = curCount;
            if(curCount < minVal)
                minVal = curCount;
        }
        double maxMinRatio = maxVal * 1.0 / minVal;
        if(verbose) {
            System.out.println("\tMin: " + minVal);
            System.out.println("\tAvg: " + aggCount / aggNodeIdToPartitionCount.size());
            System.out.println("\tMax: " + maxVal);
            System.out.println("\t\tMax/Min: " + maxMinRatio);
        }
        return maxMinRatio;
    }

    /**
     * Balances the number of partitions per node per zone. Balanced means that
     * the number of partitions per node within a zone are all within one of one
     * another. A common usage of this method is to assign partitions to newly
     * added nodes that do not have any partitions yet.
     * 
     * @param targetCluster Target cluster metadata ( which contains old nodes +
     *        new nodes [ empty partitions ])
     * @param storeDefs List of store definitions
     * @param keepPrimaryPartitionsInSameZone Checks zone IDs of donor and
     *        stealer nodes and only allow moves within same zone.
     * @param permitCrossZoneMoves Expensive check to prevent primary partition
     *        moves that result in other partitions moving across zones.
     * @param varyNumPartitionsPerNode Allows number of partitions per node
     *        within a zone to vary by +-[0,varyNumPartitionsPerNode]. A value
     *        of zero means that all nodes within a zone should have a similar
     *        (within one) number of primary partitions.
     * @return Return a pair of cluster metadata and number of primary
     *         partitions that have moved
     */
    public static Pair<Cluster, Integer> balanceNumberOfPartitionsPerNode(final Cluster targetCluster,
                                                                          final List<StoreDefinition> storeDefs,
                                                                          final boolean keepPrimaryPartitionsInSameZone,
                                                                          final boolean permitCrossZoneMoves,
                                                                          final int varyNumPartitionsPerNode) {
        System.out.println("Balance number of partitions per node within a zone.");
        List<Node> allNodes = Lists.newArrayList();

        HashMap<Integer, Integer> numPartitionsPerZone = Maps.newHashMap();
        HashMap<Integer, Integer> numNodesPerZone = Maps.newHashMap();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));

            // Update the number of partitions per zone
            if(numPartitionsPerZone.containsKey(node.getZoneId())) {
                int currentNumPartitionsInZone = numPartitionsPerZone.get(node.getZoneId());
                currentNumPartitionsInZone += node.getNumberOfPartitions();
                numPartitionsPerZone.put(node.getZoneId(), currentNumPartitionsInZone);
            } else {
                numPartitionsPerZone.put(node.getZoneId(), node.getNumberOfPartitions());
            }

            // Update the number of nodes per zone
            if(numNodesPerZone.containsKey(node.getZoneId())) {
                int currentNumNodesInZone = numNodesPerZone.get(node.getZoneId());
                currentNumNodesInZone += 1;
                numNodesPerZone.put(node.getZoneId(), currentNumNodesInZone);
            } else {
                numNodesPerZone.put(node.getZoneId(), 1);
            }
        }

        System.out.println("numPartitionsPerZone");
        for(int zone: numPartitionsPerZone.keySet()) {
            System.out.println(zone + " : " + numPartitionsPerZone.get(zone));
        }
        System.out.println("numNodesPerZone");
        for(int zone: numNodesPerZone.keySet()) {
            System.out.println(zone + " : " + numNodesPerZone.get(zone));
        }

        // Set up a balanced target number of partitions per node per zone
        HashMap<Integer, List<Integer>> numPartitionsPerNodePerZone = Maps.newHashMap();
        for(Integer zoneId: numNodesPerZone.keySet()) {
            int numNodesInZone = numNodesPerZone.get(zoneId);
            int numPartitionsInZone = numPartitionsPerZone.get(zoneId);
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

        // Add randomness to # of partitions per node in zone. (Unclear if this
        // is useful.)
        if(varyNumPartitionsPerNode > 0) {
            Random r = new Random();
            for(Integer zoneId: numPartitionsPerNodePerZone.keySet()) {
                int totalRandom = 0;
                for(int i = 0; i < numPartitionsPerNodePerZone.get(zoneId).size(); i++) {
                    int current = numPartitionsPerNodePerZone.get(zoneId).get(i);
                    int randomVal = r.nextInt(varyNumPartitionsPerNode * 2 + 1);
                    // Shift randomVal down to be centered around 0, so it may
                    // be -ive. Limit how negative randomVal may be to ensure at
                    // least one partition is assigned.
                    randomVal = Math.max(-current + 1, randomVal - varyNumPartitionsPerNode);
                    numPartitionsPerNodePerZone.get(zoneId).set(i, current + randomVal);
                    totalRandom += randomVal;
                }
                while(totalRandom > 0) {
                    int id = r.nextInt(numPartitionsPerNodePerZone.get(zoneId).size());
                    int current = numPartitionsPerNodePerZone.get(zoneId).get(id);
                    if(current > 1) { // Leave at least one partition on node
                        numPartitionsPerNodePerZone.get(zoneId).set(id, current - 1);
                        totalRandom--;
                    }
                }
                while(totalRandom < 0) {
                    int id = r.nextInt(numPartitionsPerNodePerZone.get(zoneId).size());
                    int current = numPartitionsPerNodePerZone.get(zoneId).get(id);
                    numPartitionsPerNodePerZone.get(zoneId).set(id, current + 1);
                    totalRandom++;
                }
            }
        }

        // Assign target number of partitions per node to specific node IDs and
        // separate Nodes into donorNodes and stealerNodes
        List<Node> donorNodes = Lists.newArrayList();
        List<Node> stealerNodes = Lists.newArrayList();

        HashMap<Integer, Integer> numPartitionsOnNode = Maps.newHashMap();
        HashMap<Integer, Integer> numNodesAssignedInZone = Maps.newHashMap();
        for(Integer zoneId: numPartitionsPerNodePerZone.keySet()) {
            numNodesAssignedInZone.put(zoneId, 0);
        }
        for(Node node: allNodes) {
            int zoneId = node.getZoneId();

            int offset = numNodesAssignedInZone.get(zoneId);
            numNodesAssignedInZone.put(zoneId, offset + 1);

            int numPartitions = numPartitionsPerNodePerZone.get(zoneId).get(offset);
            numPartitionsOnNode.put(node.getId(), numPartitions);

            if(numPartitions < node.getNumberOfPartitions()) {
                donorNodes.add(node);
            } else if(numPartitions > node.getNumberOfPartitions()) {
                stealerNodes.add(node);
            }
        }

        // Print out donor/stealer information
        for(Node node: donorNodes) {
            System.out.println("Donor Node: " + node.getId() + ", zoneId " + node.getZoneId()
                               + ", numPartitions " + node.getNumberOfPartitions()
                               + ", target number of partitions "
                               + numPartitionsOnNode.get(node.getId()));
        }
        for(Node node: stealerNodes) {
            System.out.println("Stealer Node: " + node.getId() + ", zoneId " + node.getZoneId()
                               + ", numPartitions " + node.getNumberOfPartitions()
                               + ", target number of partitions "
                               + numPartitionsOnNode.get(node.getId()));
        }

        // Go over every stealerNode and steal partitions from donor nodes
        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        int totalPrimaryPartitionsMoved = 0;
        for(Node stealerNode: stealerNodes) {
            int partitionsToSteal = numPartitionsOnNode.get(stealerNode.getId())
                                    - stealerNode.getNumberOfPartitions();

            System.out.println("Node (" + stealerNode.getId() + ") in zone ("
                               + stealerNode.getZoneId() + ") has partitionsToSteal of "
                               + partitionsToSteal);

            while(partitionsToSteal > 0) {
                // Repeatedly loop over donor nodes to distribute stealing
                for(Node donorNode: donorNodes) {
                    Node currentDonorNode = returnCluster.getNodeById(donorNode.getId());

                    // Only steal from donor nodes within same zone

                    if(keepPrimaryPartitionsInSameZone
                       && (currentDonorNode.getZoneId() != stealerNode.getZoneId())) {
                        continue;
                    }
                    // Only steal from donor nodes with extra partitions
                    if(currentDonorNode.getNumberOfPartitions() == numPartitionsOnNode.get(currentDonorNode.getId())) {
                        continue;
                    }

                    List<Integer> donorPartitions = Lists.newArrayList(currentDonorNode.getPartitionIds());
                    Collections.shuffle(donorPartitions, new Random(System.currentTimeMillis()));

                    for(int donorPartition: donorPartitions) {
                        Cluster intermediateCluster = createUpdatedCluster(returnCluster,
                                                                           stealerNode.getId(),
                                                                           Lists.newArrayList(donorPartition));

                        int crossZoneMoves = 0;
                        if(!permitCrossZoneMoves) {
                            // getCrossZoneMoves can be a *slow* call. E.g., for
                            // an 11 node cluster, the call takes ~230 ms,
                            // whereas for a 39 node cluster the call takes ~10
                            // s (40x longer). Also, unclear how useful this
                            // constraint is in practice.
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
                            totalPrimaryPartitionsMoved++;
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
                        }
                    }

                    if(partitionsToSteal == 0)
                        break;
                }
            }
        }

        return Pair.create(returnCluster, totalPrimaryPartitionsMoved);
    }

    /**
     * Ensures that no more than maxContiguousPartitionsPerZone partitions are
     * contiguous within a single zone.
     * 
     * Moves some number of partitions from each zone to some other random
     * zone/node. There is some chance that such moves could result in
     * contiguous partitions in other zones.
     * 
     * @param targetCluster Target cluster metadata
     * @param maxContiguousPartitionsPerZone
     * @return Return a pair of cluster metadata and number of primary
     *         partitions that have moved.
     */
    public static Pair<Cluster, Integer> balanceContiguousPartitionsPerZone(final Cluster targetCluster,
                                                                            final int maxContiguousPartitionsPerZone) {
        System.out.println("Balance number of contiguous partitions within a zone.");
        List<Node> allNodes = Lists.newArrayList();
        HashMap<Integer, List<Integer>> nodesPerZone = Maps.newHashMap();
        HashMap<Integer, List<Integer>> partitionsPerZone = Maps.newHashMap();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));

            if(nodesPerZone.containsKey(node.getZoneId())) {
                List<Integer> currentNodeList = nodesPerZone.get(node.getZoneId());
                List<Integer> mutableNodeList = new ArrayList<Integer>();
                mutableNodeList.addAll(currentNodeList);
                mutableNodeList.add(node.getId());
                nodesPerZone.put(node.getZoneId(), mutableNodeList);
            } else {
                List<Integer> nodeList = new ArrayList<Integer>();
                nodeList.add(node.getId());
                nodesPerZone.put(node.getZoneId(), nodeList);
            }

            if(partitionsPerZone.containsKey(node.getZoneId())) {
                List<Integer> currentPartitions = partitionsPerZone.get(node.getZoneId());
                List<Integer> mutableCurrentPartitions = new ArrayList<Integer>();
                mutableCurrentPartitions.addAll(currentPartitions);
                mutableCurrentPartitions.addAll(node.getPartitionIds());
                partitionsPerZone.put(node.getZoneId(), mutableCurrentPartitions);
            } else {
                partitionsPerZone.put(node.getZoneId(), node.getPartitionIds());
            }
        }

        System.out.println("numPartitionsPerZone");
        for(int zone: partitionsPerZone.keySet()) {
            System.out.println(zone + " : " + partitionsPerZone.get(zone).size());
        }
        System.out.println("numNodesPerZone");
        for(int zone: nodesPerZone.keySet()) {
            System.out.println(zone + " : " + nodesPerZone.get(zone).size());
        }

        // Break up contiguous partitions within each zone
        HashMap<Integer, List<Integer>> partitionsToRemoveFromZone = Maps.newHashMap();
        System.out.println("Contiguous partitions");
        for(Integer zoneId: partitionsPerZone.keySet()) {
            System.out.println("\tZone: " + zoneId);
            List<Integer> partitions = partitionsPerZone.get(zoneId);
            java.util.Collections.sort(partitions);

            List<Integer> partitionsToRemoveFromThisZone = new ArrayList<Integer>();
            List<Integer> contiguousPartitions = new ArrayList<Integer>();
            int contiguousCount = 0;
            int lastPartitionId = partitions.get(0);
            for(int i = 1; i < partitions.size(); ++i) {
                if(partitions.get(i) == lastPartitionId + 1) {
                    contiguousCount++;
                    contiguousPartitions.add(partitions.get(i));
                } else {
                    if(contiguousCount > maxContiguousPartitionsPerZone) {
                        System.out.println("\tContiguous partitions: " + contiguousPartitions);
                        int lenContig = contiguousPartitions.size();
                        int offset = lenContig % (maxContiguousPartitionsPerZone + 1);
                        // System.out.println("offset: " + offset +
                        // ", lenContig: " + lenContig);
                        if(offset == 0) {
                            // Would land on last partition in contiguous list.
                            // Shift over to only move "inner" partitions from
                            // the contiguous partition list.
                            offset = maxContiguousPartitionsPerZone / 2;
                        }
                        // If still remove last partition, shift by one.
                        if(offset == 0) {
                            offset = 1;
                        }
                        while(offset < lenContig) {
                            partitionsToRemoveFromThisZone.add(contiguousPartitions.get(offset));
                            offset += maxContiguousPartitionsPerZone + 1;
                        }
                    }
                    contiguousPartitions.clear();
                    contiguousCount = 0;
                }
                lastPartitionId = partitions.get(i);
            }
            partitionsToRemoveFromZone.put(zoneId, partitionsToRemoveFromThisZone);
            System.out.println("\t\tPartitions to remove: " + partitionsToRemoveFromThisZone);
        }

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        int totalPrimaryPartitionsMoved = 0;

        Random r = new Random();
        // Use a hash-map that is guaranteed to list all zone IDs
        Set<Integer> zoneIds = nodesPerZone.keySet();
        for(int zoneId: zoneIds) {
            for(int partitionId: partitionsToRemoveFromZone.get(zoneId)) {
                // Pick a random other zone Id
                List<Integer> otherZoneIds = new ArrayList<Integer>();
                for(int otherZoneId: zoneIds) {
                    if(otherZoneId != zoneId) {
                        otherZoneIds.add(otherZoneId);
                    }
                }
                int whichOtherZoneId = otherZoneIds.get(r.nextInt(otherZoneIds.size()));

                // Pick a random node from other zone ID
                int whichNodeOffset = r.nextInt(nodesPerZone.get(whichOtherZoneId).size());
                int whichNodeId = nodesPerZone.get(whichOtherZoneId).get(whichNodeOffset);

                // Steal partition from one zone to another!
                returnCluster = createUpdatedCluster(returnCluster,
                                                     whichNodeId,
                                                     Lists.newArrayList(partitionId));
                totalPrimaryPartitionsMoved++;
            }
        }

        return Pair.create(returnCluster, totalPrimaryPartitionsMoved);
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
    public static Pair<Cluster, Integer> balanceNumPartitionsPerZone(final Cluster targetCluster) {
        System.out.println("Balance number of partitions per zone.");
        List<Node> allNodes = Lists.newArrayList();
        HashMap<Integer, List<Integer>> nodesPerZone = Maps.newHashMap();
        HashMap<Integer, List<Integer>> partitionsPerZone = Maps.newHashMap();
        int numPartitions = 0;

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));
            numPartitions += node.getNumberOfPartitions();

            if(nodesPerZone.containsKey(node.getZoneId())) {
                List<Integer> currentNodeList = nodesPerZone.get(node.getZoneId());
                List<Integer> mutableNodeList = new ArrayList<Integer>();
                mutableNodeList.addAll(currentNodeList);
                mutableNodeList.add(node.getId());
                nodesPerZone.put(node.getZoneId(), mutableNodeList);
            } else {
                List<Integer> nodeList = new ArrayList<Integer>();
                nodeList.add(node.getId());
                nodesPerZone.put(node.getZoneId(), nodeList);
            }

            if(partitionsPerZone.containsKey(node.getZoneId())) {
                List<Integer> currentPartitions = partitionsPerZone.get(node.getZoneId());
                List<Integer> mutableCurrentPartitions = new ArrayList<Integer>();
                mutableCurrentPartitions.addAll(currentPartitions);
                mutableCurrentPartitions.addAll(node.getPartitionIds());
                partitionsPerZone.put(node.getZoneId(), mutableCurrentPartitions);
            } else {
                partitionsPerZone.put(node.getZoneId(), node.getPartitionIds());
            }
        }

        System.out.println("numPartitionsPerZone");
        for(int zone: partitionsPerZone.keySet()) {
            System.out.println(zone + " : " + partitionsPerZone.get(zone).size());
        }
        System.out.println("numNodesPerZone");
        for(int zone: nodesPerZone.keySet()) {
            System.out.println(zone + " : " + nodesPerZone.get(zone).size());
        }

        // Set up a balanced target number of partitions to move per zone
        HashMap<Integer, Integer> targetNumPartitionsPerZone = Maps.newHashMap();
        int numZones = nodesPerZone.size();
        int floorPartitions = numPartitions / numZones;
        int numZonesWithCeil = numPartitions - (numZones * floorPartitions);
        int zoneCounter = 0;
        for(Integer zoneId: nodesPerZone.keySet()) {
            if(zoneCounter < numZonesWithCeil) {
                targetNumPartitionsPerZone.put(zoneId,
                                               floorPartitions + 1
                                                       - partitionsPerZone.get(zoneId).size());
            } else {
                targetNumPartitionsPerZone.put(zoneId,
                                               floorPartitions
                                                       - partitionsPerZone.get(zoneId).size());
            }
            zoneCounter++;
        }

        List<Integer> donorZoneIds = new ArrayList<Integer>();
        List<Integer> stealerZoneIds = new ArrayList<Integer>();
        for(Integer zoneId: nodesPerZone.keySet()) {
            if(targetNumPartitionsPerZone.get(zoneId) > 0) {
                stealerZoneIds.add(zoneId);
            } else if(targetNumPartitionsPerZone.get(zoneId) < 0) {
                donorZoneIds.add(zoneId);
            }
        }

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        int totalPrimaryPartitionsMoved = 0;
        Random r = new Random();

        for(Integer stealerZoneId: stealerZoneIds) {
            while(targetNumPartitionsPerZone.get(stealerZoneId) > 0) {
                for(Integer donorZoneId: donorZoneIds) {
                    if(targetNumPartitionsPerZone.get(donorZoneId) < 0) {
                        // Select random stealer node
                        int stealerNodeOffset = r.nextInt(nodesPerZone.get(stealerZoneId).size());
                        Integer stealerNodeId = nodesPerZone.get(stealerZoneId)
                                                            .get(stealerNodeOffset);

                        // Select random donor partition
                        List<Integer> partitionsThisZone = partitionsPerZone.get(donorZoneId);
                        int donorPartitionOffset = r.nextInt(partitionsThisZone.size());
                        int donorPartitionId = partitionsThisZone.get(donorPartitionOffset);

                        // Accounting
                        partitionsThisZone.remove(donorPartitionOffset);
                        partitionsPerZone.put(donorZoneId, partitionsThisZone);
                        targetNumPartitionsPerZone.put(donorZoneId,
                                                       targetNumPartitionsPerZone.get(donorZoneId) + 1);
                        targetNumPartitionsPerZone.put(stealerZoneId,
                                                       targetNumPartitionsPerZone.get(stealerZoneId) - 1);
                        totalPrimaryPartitionsMoved++;

                        // Steal it!
                        returnCluster = createUpdatedCluster(returnCluster,
                                                             stealerNodeId,
                                                             Lists.newArrayList(donorPartitionId));
                    }
                }
            }
        }

        return Pair.create(returnCluster, totalPrimaryPartitionsMoved);
    }

    /**
     * Swaps two specified partitions.
     * 
     * @param targetCluster
     * @param zoneId
     * @return
     */
    public static Pair<Cluster, Integer> swapPartitions(final Cluster targetCluster,
                                                        final int nodeIdA,
                                                        final int partitionIdA,
                                                        final int nodeIdB,
                                                        final int partitionIdB) {
        List<Node> allNodes = Lists.newArrayList();
        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));
        }
        Cluster returnCluster = updateCluster(targetCluster, allNodes);

        // Swap partitions between nodes!
        returnCluster = createUpdatedCluster(returnCluster,
                                             nodeIdA,
                                             Lists.newArrayList(partitionIdB));
        returnCluster = createUpdatedCluster(returnCluster,
                                             nodeIdB,
                                             Lists.newArrayList(partitionIdA));

        return Pair.create(returnCluster, 2);
    }

    /**
     * Within a single zone, swaps one random partition on one random node with
     * another random partition on different random node.
     * 
     * @param targetCluster
     * @param zoneId Zone ID within which to shuffle partitions
     * @return
     */
    public static Pair<Cluster, Integer> swapRandomPartitionsWithinZone(final Cluster targetCluster,
                                                                        final int zoneId) {
        List<Node> allNodes = Lists.newArrayList();
        List<Integer> nodeIdsInZone = new ArrayList<Integer>();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));

            if(node.getZoneId() == zoneId) {
                nodeIdsInZone.add(node.getId());
            }
        }

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
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
            return Pair.create(returnCluster, 0);
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
     * @param randomSwapAttempts Number of random swaps to attempt.
     * @param randomSwapSuccesses Early exit condition. I.e., stop attempting
     *        random swaps after swapSuccesses swaps have improved the std dev.
     * @param storeDefs List of store definitions
     * @return
     */
    public static Pair<Cluster, Integer> randomShufflePartitions(final Cluster targetCluster,
                                                                 final int randomSwapAttempts,
                                                                 final int randomSwapSuccesses,
                                                                 List<StoreDefinition> storeDefs) {
        List<Node> allNodes = Lists.newArrayList();
        Set<Integer> zoneIds = new HashSet<Integer>();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));
            zoneIds.add(node.getZoneId());
        }

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        int totalPrimaryPartitionsMoved = 0;

        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs, false);

        int successes = 0;
        for(int i = 0; i < randomSwapAttempts; i++) {
            for(Integer zoneId: zoneIds) {
                Pair<Cluster, Integer> shuffleResults = swapRandomPartitionsWithinZone(returnCluster,
                                                                                       zoneId);
                double nextMaxMinRatio = analyzeBalance(shuffleResults.getFirst(), storeDefs, false);
                if(nextMaxMinRatio < currentMaxMinRatio) {
                    successes++;
                    System.out.println("Swap improved max-min ratio: " + currentMaxMinRatio
                                       + " -> " + nextMaxMinRatio + " (improvement " + successes
                                       + " on swap attempt " + i + " in zone " + zoneId + ")");
                    returnCluster = shuffleResults.getFirst();
                    currentMaxMinRatio = nextMaxMinRatio;
                    totalPrimaryPartitionsMoved += shuffleResults.getSecond();
                }
            }
            if(successes >= randomSwapSuccesses) {
                // Enough successes, move on.
                break;
            }
        }

        return Pair.create(returnCluster, totalPrimaryPartitionsMoved);
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
    public static Pair<Cluster, Integer> swapGreedyPartitionsWithinZone(final Cluster targetCluster,
                                                                        final int zoneId,
                                                                        List<StoreDefinition> storeDefs) {
        List<Node> allNodes = Lists.newArrayList();
        List<Integer> nodeIdsInZone = new ArrayList<Integer>();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));

            if(node.getZoneId() == zoneId) {
                nodeIdsInZone.add(node.getId());
            }
        }

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs, false);
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

                        Pair<Cluster, Integer> swapResult = swapPartitions(returnCluster,
                                                                           nodeIdEh,
                                                                           partitionIdEh,
                                                                           nodeIdBee,
                                                                           partitionIdBee);
                        double swapMaxMinRatio = analyzeBalance(swapResult.getFirst(),
                                                                storeDefs,
                                                                false);
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
            return new Pair<Cluster, Integer>(returnCluster, 0);
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
     * @param greedySwapMaxPartitionsPerNode Max number of partitions to try
     *        swapping per node.
     * @param greedySwapMaxPartitionsPerZone Max number of partitions from the
     *        rest of the zone to try swapping.
     * @param storeDefs
     * @return
     */
    public static Pair<Cluster, Integer> swapGreedyRandomPartitionsWithinZone(final Cluster targetCluster,
                                                                              final int zoneId,
                                                                              final int greedySwapMaxPartitionsPerNode,
                                                                              final int greedySwapMaxPartitionsPerZone,
                                                                              List<StoreDefinition> storeDefs) {
        List<Node> allNodes = Lists.newArrayList();
        List<Integer> nodeIdsInZone = new ArrayList<Integer>();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));

            if(node.getZoneId() == zoneId) {
                nodeIdsInZone.add(node.getId());
            }
        }
        System.out.println("GreedyRandom : nodeIdsInZone:" + nodeIdsInZone);

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs, false);
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
                    Pair<Cluster, Integer> swapResult = swapPartitions(returnCluster,
                                                                       nodeIdEh,
                                                                       partitionIdEh,
                                                                       nodeIdBee,
                                                                       partitionIdBee);
                    double swapMaxMinRatio = analyzeBalance(swapResult.getFirst(), storeDefs, false);
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
            return new Pair<Cluster, Integer>(returnCluster, 0);
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
     * @param greedyAttempts Number of greedy-random swaps to attempt.
     * @param greedySwapMaxPartitionsPerNode Max number of partitions to try
     *        swapping per node.
     * @param greedySwapMaxPartitionsPerZone Max number of partitions from the
     *        rest of the zone to try swapping.
     * @param storeDefs
     * @return
     */
    public static Pair<Cluster, Integer> greedyShufflePartitions(final Cluster targetCluster,
                                                                 final int greedyAttempts,
                                                                 final int greedySwapMaxPartitionsPerNode,
                                                                 final int greedySwapMaxPartitionsPerZone,
                                                                 List<StoreDefinition> storeDefs) {
        List<Node> allNodes = Lists.newArrayList();
        Set<Integer> zoneIds = new HashSet<Integer>();

        for(Node node: targetCluster.getNodes()) {
            allNodes.add(updateNode(node, Lists.newArrayList(node.getPartitionIds())));
            zoneIds.add(node.getZoneId());
        }

        Cluster returnCluster = updateCluster(targetCluster, allNodes);
        int totalPrimaryPartitionsMoved = 0;

        double currentMaxMinRatio = analyzeBalance(returnCluster, storeDefs, false);

        for(int i = 0; i < greedyAttempts; i++) {
            for(Integer zoneId: zoneIds) {
                System.out.println("Greedy swap attempt: zone " + zoneId + " , attempt " + i
                                   + " of " + greedyAttempts);
                Pair<Cluster, Integer> shuffleResults = swapGreedyRandomPartitionsWithinZone(returnCluster,
                                                                                             zoneId,
                                                                                             greedySwapMaxPartitionsPerNode,
                                                                                             greedySwapMaxPartitionsPerZone,
                                                                                             storeDefs);
                double nextMaxMinRatio = analyzeBalance(shuffleResults.getFirst(), storeDefs, false);

                if(nextMaxMinRatio == currentMaxMinRatio) {
                    System.out.println("Not improving for zone: " + zoneId);
                } else {
                    System.out.println("Swap improved max-min ratio: " + currentMaxMinRatio
                                       + " -> " + nextMaxMinRatio + " (swap attempt " + i
                                       + " in zone " + zoneId + ")");
                    returnCluster = shuffleResults.getFirst();
                    currentMaxMinRatio = nextMaxMinRatio;
                    totalPrimaryPartitionsMoved += shuffleResults.getSecond();
                }
            }
        }

        return Pair.create(returnCluster, totalPrimaryPartitionsMoved);
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
