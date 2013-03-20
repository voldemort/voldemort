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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;

import com.google.common.collect.Maps;

// TODO: Add ClusterInstanceTest unit test for these helper methods.

/**
 * This class wraps up a Cluster object and a List<StoreDefinition>. The methods
 * are effectively helper or util style methods for analyzing partitions and so
 * on which are a function of both Cluster and List<StoreDefinition>.
 */
public class ClusterInstance {

    // TODO: (refactor) Improve upon the name "ClusterInstance". Object-oriented
    // meaning of 'instance' is too easily confused with system notion of an
    // "instance of a cluster" (the intended usage in this class name).

    private final Cluster cluster;
    private final List<StoreDefinition> storeDefs;

    public ClusterInstance(Cluster cluster, List<StoreDefinition> storeDefs) {
        this.cluster = cluster;
        this.storeDefs = storeDefs;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public List<StoreDefinition> getStoreDefs() {
        return storeDefs;
    }

    /**
     * Wrapper that just returns the max/min ratio metric and throws away the
     * verbose string.
     */
    public double analyzeBalance() {
        Pair<Double, String> analysis = analyzeBalanceVerbose();
        return analysis.getFirst();
    }

    /**
     * 
     * @param nodeIdToPartitionCount
     * @param title
     * @return
     */
    public Pair<Double, String> summarizeBalance(final Map<Integer, Integer> nodeIdToPartitionCount,
                                                 String title) {
        StringBuilder builder = new StringBuilder();
        Set<Integer> nodeIds = cluster.getNodeIds();

        builder.append("\n" + title + "\n");
        int minVal = Integer.MAX_VALUE;
        int maxVal = Integer.MIN_VALUE;
        int aggCount = 0;
        for(Integer nodeId: nodeIds) {
            int curCount = nodeIdToPartitionCount.get(nodeId);
            builder.append("\tNode ID: " + nodeId + " : " + curCount + " ("
                           + cluster.getNodeById(nodeId).getHost() + ")\n");
            aggCount += curCount;
            if(curCount > maxVal)
                maxVal = curCount;
            if(curCount < minVal)
                minVal = curCount;
        }
        int avgVal = aggCount / nodeIdToPartitionCount.size();
        double maxAvgRatio = maxVal * 1.0 / avgVal;
        if(avgVal == 0) {
            maxAvgRatio = maxVal;
        }
        double maxMinRatio = maxVal * 1.0 / minVal;
        if(minVal == 0) {
            maxMinRatio = maxVal;
        }
        builder.append("\tMin: " + minVal + "\n");
        builder.append("\tAvg: " + avgVal + "\n");
        builder.append("\tMax: " + maxVal + "\n");
        builder.append("\t\tMax/Avg: " + maxAvgRatio + "\n");
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
     * @return First element of pair is the max/min ratio. Second element of
     *         pair is a string that can be printed to dump all the gory details
     *         of the analysis.
     */
    public Pair<Double, String> analyzeBalanceVerbose() {
        StringBuilder builder = new StringBuilder();
        builder.append(ClusterUtils.verboseClusterDump(cluster));

        HashMap<StoreDefinition, Integer> uniqueStores = KeyDistributionGenerator.getUniqueStoreDefinitionsWithCounts(storeDefs);
        List<ByteArray> keys = KeyDistributionGenerator.generateKeys(KeyDistributionGenerator.DEFAULT_NUM_KEYS);
        Set<Integer> nodeIds = cluster.getNodeIds();
        Set<Integer> zoneIds = cluster.getZoneIds();

        builder.append("PARTITION DUMP\n");
        Map<Integer, Integer> primaryAggNodeIdToPartitionCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            primaryAggNodeIdToPartitionCount.put(nodeId, 0);
        }

        Map<Integer, Integer> aggNodeIdToZonePrimaryCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            aggNodeIdToZonePrimaryCount.put(nodeId, 0);
        }

        Map<Integer, Integer> allAggNodeIdToPartitionCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            allAggNodeIdToPartitionCount.put(nodeId, 0);
        }

        for(StoreDefinition storeDefinition: uniqueStores.keySet()) {
            StoreInstance storeInstance = new StoreInstance(cluster, storeDefinition);

            builder.append("\n");
            builder.append("Store exemplar: " + storeDefinition.getName() + "\n");
            builder.append("\tReplication factor: " + storeDefinition.getReplicationFactor() + "\n");
            builder.append("\tRouting strategy: " + storeDefinition.getRoutingStrategyType() + "\n");
            builder.append("\tThere are " + uniqueStores.get(storeDefinition)
                           + " other similar stores.\n");

            // Map of node Id to Sets of pairs. Pairs of Integers are of
            // <replica_type, partition_id>
            Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions = RebalanceUtils.getNodeIdToAllPartitions(cluster,
                                                                                                                      storeDefinition,
                                                                                                                      true);
            Map<Integer, Integer> primaryNodeIdToPartitionCount = Maps.newHashMap();
            Map<Integer, Integer> nodeIdToZonePrimaryCount = Maps.newHashMap();
            Map<Integer, Integer> allNodeIdToPartitionCount = Maps.newHashMap();

            // Print out all partitions, by replica type, per node
            builder.append("\n");
            builder.append("\tDetailed Dump:\n");
            for(Integer nodeId: nodeIds) {
                builder.append("\tNode ID: " + nodeId + "in zone "
                               + cluster.getNodeById(nodeId).getZoneId() + "\n");
                primaryNodeIdToPartitionCount.put(nodeId, 0);
                nodeIdToZonePrimaryCount.put(nodeId, 0);
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

                    builder.append("\t\t" + replicaType);
                    for(int zoneId: zoneIds) {
                        builder.append(" : z" + zoneId + " : ");
                        List<Integer> zonePartitions = new ArrayList<Integer>();
                        for(int partitionId: partitions) {
                            if(cluster.getPartitionIdsInZone(zoneId).contains(partitionId)) {
                                zonePartitions.add(partitionId);
                            }
                        }
                        builder.append(zonePartitions.toString());

                    }
                    builder.append("\n");
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

            // Go through all partition IDs and determine which node is "first"
            // in the replicating node list for every zone. This determines the
            // number of "zone primaries" each node hosts.
            for(int partitionId = 0; partitionId < cluster.getNumberOfPartitions(); partitionId++) {
                for(int zoneId: zoneIds) {
                    for(int nodeId: storeInstance.getReplicationNodeList(partitionId)) {
                        if(cluster.getNodeById(nodeId).getZoneId() == zoneId) {
                            nodeIdToZonePrimaryCount.put(nodeId,
                                                         nodeIdToZonePrimaryCount.get(nodeId) + 1);
                            break;
                        }
                    }
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
                aggNodeIdToZonePrimaryCount.put(nodeId, aggNodeIdToZonePrimaryCount.get(nodeId)
                                                        + nodeIdToZonePrimaryCount.get(nodeId)
                                                        * uniqueStores.get(storeDefinition));
                allAggNodeIdToPartitionCount.put(nodeId,
                                                 allAggNodeIdToPartitionCount.get(nodeId)
                                                         + (allNodeIdToPartitionCount.get(nodeId) * uniqueStores.get(storeDefinition)));
            }
        }

        builder.append("\n");
        builder.append("STD DEV ANALYSIS\n");
        builder.append("\n");
        builder.append(KeyDistributionGenerator.printOverallDistribution(cluster, storeDefs, keys));
        builder.append("\n");
        builder.append("\n");

        Pair<Double, String> summary = summarizeBalance(primaryAggNodeIdToPartitionCount,
                                                        "AGGREGATE PRIMARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());

        summary = summarizeBalance(aggNodeIdToZonePrimaryCount,
                                   "AGGREGATE ZONEPRIMARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());

        summary = summarizeBalance(allAggNodeIdToPartitionCount,
                                   "AGGREGATE NARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());

        return new Pair<Double, String>(summary.getFirst(), builder.toString());
    }
}
