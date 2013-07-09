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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

public class PartitionBalance {

    /**
     * Multiplier in utility method to weight the balance of "IOPS" (get QPS &
     * pseudo-master put QPS) relative to "CAPACITY".
     */
    private final static int UTILITY_MULTIPLIER_IOPS = 1;
    /**
     * Multiplier in utility method to weight the balance of "CAPACITY" (put QPS
     * and therefore amount of data stored) relative to "IOPS".
     * 
     * Currently, we bias towards balancing capacity over iops.
     */
    private final static int UTILITY_MULTIPLIER_CAPACITY = 2;

    private final Cluster cluster;

    private final double primaryMaxMin;
    private final double zonePrimaryMaxMin;
    private final double naryMaxMin;
    private final String verbose;

    private final Map<Integer, Integer> primaryAggNodeIdToPartitionCount;
    private final Map<Integer, Integer> aggNodeIdToZonePrimaryCount;
    private final Map<Integer, Integer> allAggNodeIdToPartitionCount;
    private final Map<Integer, Integer> zoneIdToPartitionStoreCount;

    public PartitionBalance(Cluster cluster, List<StoreDefinition> storeDefs) {
        this.cluster = cluster;

        StringBuilder builder = new StringBuilder();
        builder.append(ClusterUtils.verboseClusterDump(cluster));

        HashMap<StoreDefinition, Integer> uniqueStores = StoreDefinitionUtils.getUniqueStoreDefinitionsWithCounts(storeDefs);
        Set<Integer> nodeIds = cluster.getNodeIds();
        Set<Integer> zoneIds = cluster.getZoneIds();

        builder.append("PARTITION DUMP\n");
        this.primaryAggNodeIdToPartitionCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            primaryAggNodeIdToPartitionCount.put(nodeId, 0);
        }

        this.aggNodeIdToZonePrimaryCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            aggNodeIdToZonePrimaryCount.put(nodeId, 0);
        }

        this.allAggNodeIdToPartitionCount = Maps.newHashMap();
        for(Integer nodeId: nodeIds) {
            allAggNodeIdToPartitionCount.put(nodeId, 0);
        }

        this.zoneIdToPartitionStoreCount = Maps.newHashMap();
        for(Integer zoneId: zoneIds) {
            zoneIdToPartitionStoreCount.put(zoneId, 0);
        }

        for(StoreDefinition storeDefinition: uniqueStores.keySet()) {
            StoreRoutingPlan storeRoutingPlan = new StoreRoutingPlan(cluster, storeDefinition);

            // High level information about the store def exemplar
            builder.append(Utils.NEWLINE)
                   .append("Store exemplar: " + storeDefinition.getName())
                   .append(Utils.NEWLINE)
                   .append("\tReplication factor: " + storeDefinition.getReplicationFactor())
                   .append(Utils.NEWLINE)
                   .append("\tRouting strategy: " + storeDefinition.getRoutingStrategyType())
                   .append(Utils.NEWLINE)
                   .append("\tThere are " + uniqueStores.get(storeDefinition)
                           + " other similar stores.")
                   .append(Utils.NEWLINE)
                   .append(Utils.NEWLINE);

            // Detailed dump of partitions on nodes
            builder.append(dumpZoneNAryDetails(storeRoutingPlan));
            builder.append(Utils.NEWLINE);

            // Per-node counts of various partition types (primary,
            // zone-primary, and n-ary)
            Map<Integer, Integer> nodeIdToPrimaryCount = getNodeIdToPrimaryCount(cluster);
            Map<Integer, Integer> nodeIdToZonePrimaryCount = getNodeIdToZonePrimaryCount(cluster,
                                                                                         storeRoutingPlan);
            Map<Integer, Integer> nodeIdToNaryCount = getNodeIdToNaryCount(cluster,
                                                                           storeRoutingPlan);

            builder.append("\tSummary of NAry counts:").append(Utils.NEWLINE);
            for(Integer nodeId: nodeIds) {
                builder.append("\tNode ID: " + nodeId + " : " + nodeIdToNaryCount.get(nodeId)
                               + "\n");
                primaryAggNodeIdToPartitionCount.put(nodeId,
                                                     primaryAggNodeIdToPartitionCount.get(nodeId)
                                                             + (nodeIdToPrimaryCount.get(nodeId) * uniqueStores.get(storeDefinition)));
                aggNodeIdToZonePrimaryCount.put(nodeId, aggNodeIdToZonePrimaryCount.get(nodeId)
                                                        + nodeIdToZonePrimaryCount.get(nodeId)
                                                        * uniqueStores.get(storeDefinition));
                allAggNodeIdToPartitionCount.put(nodeId,
                                                 allAggNodeIdToPartitionCount.get(nodeId)
                                                         + (nodeIdToNaryCount.get(nodeId) * uniqueStores.get(storeDefinition)));

                // Count partition-stores per-zone
                int zoneId = cluster.getNodeById(nodeId).getZoneId();
                zoneIdToPartitionStoreCount.put(zoneId,
                                                zoneIdToPartitionStoreCount.get(zoneId)
                                                        + (nodeIdToNaryCount.get(nodeId) * uniqueStores.get(storeDefinition)));
            }
        }

        builder.append(Utils.NEWLINE).append(Utils.NEWLINE);

        builder.append("PARTITION-STORES PER-ZONE:").append(Utils.NEWLINE);
        for(Integer zoneId: zoneIds) {
            builder.append("\tZone ID: " + zoneId + " : " + zoneIdToPartitionStoreCount.get(zoneId))
                   .append(Utils.NEWLINE);
        }

        builder.append(Utils.NEWLINE).append(Utils.NEWLINE);

        Pair<Double, String> summary = summarizeBalance(primaryAggNodeIdToPartitionCount,
                                                        "AGGREGATE PRIMARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());
        this.primaryMaxMin = summary.getFirst();

        summary = summarizeBalance(aggNodeIdToZonePrimaryCount,
                                   "AGGREGATE ZONEPRIMARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());
        this.zonePrimaryMaxMin = summary.getFirst();

        summary = summarizeBalance(allAggNodeIdToPartitionCount,
                                   "AGGREGATE NARY-PARTITION COUNT (across all stores)");
        builder.append(summary.getSecond());
        this.naryMaxMin = summary.getFirst();
        builder.append("Utility value " + getUtility()).append(Utils.NEWLINE);
        this.verbose = builder.toString();
    }

    /**
     * Go through all nodes and determine how many partition Ids each node
     * hosts.
     * 
     * @param cluster
     * @return map of nodeId to number of primary partitions hosted on node.
     */
    private Map<Integer, Integer> getNodeIdToPrimaryCount(Cluster cluster) {
        Map<Integer, Integer> nodeIdToPrimaryCount = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            nodeIdToPrimaryCount.put(node.getId(), node.getPartitionIds().size());
        }

        return nodeIdToPrimaryCount;
    }

    /**
     * Go through all partition IDs and determine which node is "first" in the
     * replicating node list for every zone. This determines the number of
     * "zone primaries" each node hosts.
     * 
     * @return map of nodeId to number of zone-primaries hosted on node.
     */
    private Map<Integer, Integer> getNodeIdToZonePrimaryCount(Cluster cluster,
                                                              StoreRoutingPlan storeRoutingPlan) {
        Map<Integer, Integer> nodeIdToZonePrimaryCount = Maps.newHashMap();
        for(Integer nodeId: cluster.getNodeIds()) {
            nodeIdToZonePrimaryCount.put(nodeId,
                                         storeRoutingPlan.getZonePrimaryPartitionIds(nodeId).size());
        }

        return nodeIdToZonePrimaryCount;
    }

    /**
     * Go through all node IDs and determine which node
     * 
     * @param cluster
     * @param storeRoutingPlan
     * @return
     */
    private Map<Integer, Integer> getNodeIdToNaryCount(Cluster cluster,
                                                       StoreRoutingPlan storeRoutingPlan) {
        Map<Integer, Integer> nodeIdToNaryCount = Maps.newHashMap();

        for(int nodeId: cluster.getNodeIds()) {
            nodeIdToNaryCount.put(nodeId, storeRoutingPlan.getZoneNAryPartitionIds(nodeId).size());
        }

        return nodeIdToNaryCount;
    }

    /**
     * Dumps the partition IDs per node in terms of zone n-ary type.
     * 
     * @param cluster
     * @param storeRoutingPlan
     * @return pretty printed string of detailed zone n-ary type.
     */
    private String dumpZoneNAryDetails(StoreRoutingPlan storeRoutingPlan) {
        StringBuilder sb = new StringBuilder();

        sb.append("\tDetailed Dump (Zone N-Aries):").append(Utils.NEWLINE);
        for(Node node: storeRoutingPlan.getCluster().getNodes()) {
            int zoneId = node.getZoneId();
            int nodeId = node.getId();
            sb.append("\tNode ID: " + nodeId + " in zone " + zoneId).append(Utils.NEWLINE);
            List<Integer> naries = storeRoutingPlan.getZoneNAryPartitionIds(nodeId);
            Map<Integer, List<Integer>> zoneNaryTypeToPartitionIds = new HashMap<Integer, List<Integer>>();
            for(int nary: naries) {
                int zoneReplicaType = storeRoutingPlan.getZoneNaryForNodesPartition(zoneId,
                                                                                    nodeId,
                                                                                    nary);
                if(!zoneNaryTypeToPartitionIds.containsKey(zoneReplicaType)) {
                    zoneNaryTypeToPartitionIds.put(zoneReplicaType, new ArrayList<Integer>());
                }
                zoneNaryTypeToPartitionIds.get(zoneReplicaType).add(nary);
            }

            for(int replicaType: new TreeSet<Integer>(zoneNaryTypeToPartitionIds.keySet())) {
                sb.append("\t\t" + replicaType + " : ");
                sb.append(zoneNaryTypeToPartitionIds.get(replicaType).toString());
                sb.append(Utils.NEWLINE);
            }
        }

        return sb.toString();
    }

    public double getPrimaryMaxMin() {
        return primaryMaxMin;
    }

    public int getPrimaryPartitionCount(int nodeId) {
        return primaryAggNodeIdToPartitionCount.get(nodeId);
    }

    public double getZonePrimaryMaxMin() {
        return zonePrimaryMaxMin;
    }

    public int getZonePrimaryPartitionCount(int nodeId) {
        return aggNodeIdToZonePrimaryCount.get(nodeId);
    }

    public double getNaryMaxMin() {
        return naryMaxMin;
    }

    public int getNaryPartitionCount(int nodeId) {
        return allAggNodeIdToPartitionCount.get(nodeId);
    }

    /**
     * Return utility of current partition balance. The utility value ought to
     * be minimized.
     * 
     * In the future, could offer different utility functions (and select
     * between them via enumeration value passed into method or constant
     * provided to constructor). The exact utility function is a combination of
     * this method and of ZoneBalanceStats.getUtility.
     * 
     * The current utility function is biased towards balancing IOPS (zone
     * primary balance) over capacity (Nary partition balance). Such bias
     * affects repartitioning algorithms that consider multiple options before
     * selecting "the best" option (e.g., greedy repartitioning algorithms for a
     * repartitioning and deciding among multiple repartitioning attempts).
     * 
     * The current utility function can hit local minima if there are two or
     * more nodes having the min value for a zone and two or more nodes having
     * the max value for a zone. Such a situation means that pair-wise swaps may
     * not be sufficient to improve utility. Could consider swapping more than
     * two partitions at a time. Could also consider a utility method which has
     * "low bits" of the number of nodes that have either min or max value per
     * zone. If these low bits are greater than 2, then a partition swap which
     * keeps high bits same, but reduces low bits should be kept.
     * 
     * @return A measure of utility that ought to be minimized.
     */
    public double getUtility() {

        return (UTILITY_MULTIPLIER_IOPS * getZonePrimaryMaxMin())
               + (UTILITY_MULTIPLIER_CAPACITY * getNaryMaxMin());
    }

    @Override
    public String toString() {
        return verbose;
    }

    private class ZoneBalanceStats {

        int minVal;
        int maxVal;
        int partitionCount;
        int nodeCount;

        ZoneBalanceStats() {
            minVal = Integer.MAX_VALUE;
            maxVal = Integer.MIN_VALUE;
            partitionCount = 0;
            nodeCount = 0;
        }

        public void addPartitions(int partitions) {
            if(partitions > maxVal)
                maxVal = partitions;
            if(partitions < minVal)
                minVal = partitions;

            partitionCount += partitions;
            nodeCount++;
        }

        private double getAvg() {
            if(nodeCount > 0) {
                return partitionCount / nodeCount;
            }
            return 0;
        }

        private double getMaxAvgRatio() {
            if(getAvg() == 0) {
                return maxVal;
            }
            return maxVal * 1.0 / getAvg();
        }

        private double getMaxMinRatio() {
            if(minVal == 0) {
                return maxVal;
            }
            return maxVal * 1.0 / minVal;
        }

        /**
         * Determins utility metric to be minimized.
         * 
         * @return utility metric that ought to be minimized.
         */
        public double getUtility() {
            return getMaxMinRatio();
        }

        public String dumpStats() {
            StringBuilder builder = new StringBuilder();

            builder.append("\tMin: " + minVal + "\n");
            builder.append("\tAvg: " + getAvg() + "\n");
            builder.append("\tMax: " + maxVal + "\n");
            builder.append("\t\tMax/Avg: " + getMaxAvgRatio() + "\n");
            builder.append("\t\tMax/Min: " + getMaxMinRatio() + "\n");
            return builder.toString();
        }

    }

    /**
     * Summarizes balance for the given nodeId to PartitionCount.
     * 
     * @param nodeIdToPartitionCount
     * @param title for use in pretty string
     * @return Pair: getFirst() is utility value to be minimized, getSecond() is
     *         pretty summary string of balance
     */
    private Pair<Double, String>
            summarizeBalance(final Map<Integer, Integer> nodeIdToPartitionCount, String title) {
        StringBuilder builder = new StringBuilder();
        builder.append("\n" + title + "\n");

        Map<Integer, ZoneBalanceStats> zoneToBalanceStats = new HashMap<Integer, ZoneBalanceStats>();
        for(Integer zoneId: cluster.getZoneIds()) {
            zoneToBalanceStats.put(zoneId, new ZoneBalanceStats());
        }

        for(Node node: cluster.getNodes()) {
            int curCount = nodeIdToPartitionCount.get(node.getId());
            builder.append("\tNode ID: " + node.getId() + " : " + curCount + " (" + node.getHost()
                           + ")\n");
            zoneToBalanceStats.get(node.getZoneId()).addPartitions(curCount);
        }

        // double utilityToBeMinimized = Double.MIN_VALUE;
        double utilityToBeMinimized = 0;
        for(Integer zoneId: cluster.getZoneIds()) {
            builder.append("Zone " + zoneId + "\n");
            builder.append(zoneToBalanceStats.get(zoneId).dumpStats());
            utilityToBeMinimized += zoneToBalanceStats.get(zoneId).getUtility();
            /*- 
             * Another utility function to consider 
            if(zoneToBalanceStats.get(zoneId).getMaxMinRatio() > utilityToBeMinimized) {
                utilityToBeMinimized = zoneToBalanceStats.get(zoneId).getUtility();
            }
             */
        }

        return Pair.create(utilityToBeMinimized, builder.toString());
    }
}