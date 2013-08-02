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
import java.util.TreeSet;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;

import com.google.common.collect.Maps;

/**
 * PartitionBalanceUtils provides helper methods for interpreting, analyzing,
 * and printing partition information.
 * 
 * Most of these helper methods take one Cluster object, and possibly some other
 * minor, simple arguments. The Cluster object defines the partition layout
 * which is being interpreted/analzyed/printed.
 */
public class PartitionBalanceUtils {

    /**
     * Compress contiguous partitions into format "e-i" instead of
     * "e, f, g, h, i". This helps illustrate contiguous partitions within a
     * zone.
     * 
     * @param cluster
     * @param zoneId
     * @return
     */
    public static String compressedListOfPartitionsInZone(final Cluster cluster, int zoneId) {
        Map<Integer, Integer> idToRunLength = PartitionBalanceUtils.getMapOfContiguousPartitions(cluster,
                                                                                                 zoneId);

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        Set<Integer> sortedInitPartitionIds = new TreeSet<Integer>(idToRunLength.keySet());
        for(int initPartitionId: sortedInitPartitionIds) {
            if(!first) {
                sb.append(", ");
            } else {
                first = false;
            }

            int runLength = idToRunLength.get(initPartitionId);
            if(runLength == 1) {
                sb.append(initPartitionId);
            } else {
                int endPartitionId = (initPartitionId + runLength - 1)
                                     % cluster.getNumberOfPartitions();
                sb.append(initPartitionId).append("-").append(endPartitionId);
            }
        }
        sb.append("]");

        return sb.toString();
    }

    /**
     * Determines run length for each 'initial' partition ID. Note that a
     * contiguous run may "wrap around" the end of the ring.
     * 
     * @param cluster
     * @param zoneId
     * @return map of initial partition Id to length of contiguous run of
     *         partition IDs within the same zone..
     */
    public static Map<Integer, Integer> getMapOfContiguousPartitions(final Cluster cluster,
                                                                     int zoneId) {
        List<Integer> partitionIds = new ArrayList<Integer>(cluster.getPartitionIdsInZone(zoneId));
        Map<Integer, Integer> partitionIdToRunLength = Maps.newHashMap();

        if(partitionIds.isEmpty()) {
            return partitionIdToRunLength;
        }

        int lastPartitionId = partitionIds.get(0);
        int initPartitionId = lastPartitionId;

        for(int offset = 1; offset < partitionIds.size(); offset++) {
            int partitionId = partitionIds.get(offset);
            if(partitionId == lastPartitionId + 1) {
                lastPartitionId = partitionId;
                continue;
            }
            int runLength = lastPartitionId - initPartitionId + 1;

            partitionIdToRunLength.put(initPartitionId, runLength);

            initPartitionId = partitionId;
            lastPartitionId = initPartitionId;
        }

        int runLength = lastPartitionId - initPartitionId + 1;
        if(lastPartitionId == cluster.getNumberOfPartitions() - 1
           && partitionIdToRunLength.containsKey(0)) {
            // special case of contiguity that wraps around the ring.
            partitionIdToRunLength.put(initPartitionId, runLength + partitionIdToRunLength.get(0));
            partitionIdToRunLength.remove(0);
        } else {
            partitionIdToRunLength.put(initPartitionId, runLength);
        }

        return partitionIdToRunLength;
    }

    /**
     * Determines a histogram of contiguous runs of partitions within a zone.
     * I.e., for each run length of contiguous partitions, how many such runs
     * are there.
     * 
     * Does not correctly address "wrap around" of partition IDs (i.e., the fact
     * that partition ID 0 is "next" to partition ID 'max')
     * 
     * @param cluster
     * @param zoneId
     * @return map of length of contiguous run of partitions to count of number
     *         of such runs.
     */
    public static Map<Integer, Integer>
            getMapOfContiguousPartitionRunLengths(final Cluster cluster, int zoneId) {
        Map<Integer, Integer> idToRunLength = getMapOfContiguousPartitions(cluster, zoneId);
        Map<Integer, Integer> runLengthToCount = Maps.newHashMap();

        if(idToRunLength.isEmpty()) {
            return runLengthToCount;
        }

        for(int runLength: idToRunLength.values()) {
            if(!runLengthToCount.containsKey(runLength)) {
                runLengthToCount.put(runLength, 0);
            }
            runLengthToCount.put(runLength, runLengthToCount.get(runLength) + 1);
        }

        return runLengthToCount;
    }

    /**
     * Pretty prints the output of getMapOfContiguousPartitionRunLengths
     * 
     * @param cluster
     * @param zoneId
     * @return
     */
    public static String getPrettyMapOfContiguousPartitionRunLengths(final Cluster cluster,
                                                                     int zoneId) {
        Map<Integer, Integer> runLengthToCount = getMapOfContiguousPartitionRunLengths(cluster,
                                                                                       zoneId);
        String prettyHistogram = "[";
        boolean first = true;
        Set<Integer> runLengths = new TreeSet<Integer>(runLengthToCount.keySet());
        for(int runLength: runLengths) {
            if(first) {
                first = false;
            } else {
                prettyHistogram += ", ";
            }
            prettyHistogram += "{" + runLength + " : " + runLengthToCount.get(runLength) + "}";
        }
        prettyHistogram += "]";
        return prettyHistogram;
    }

    /**
     * Returns a pretty printed string of nodes that host specific "hot"
     * partitions, where hot is defined as following a contiguous run of
     * partitions of some length in another zone.
     * 
     * @param cluster The cluster to analyze
     * @param hotContiguityCutoff cutoff below which a contiguous run is not
     *        hot.
     * @return
     */
    public static String getHotPartitionsDueToContiguity(final Cluster cluster,
                                                         int hotContiguityCutoff) {
        StringBuilder sb = new StringBuilder();

        for(int zoneId: cluster.getZoneIds()) {
            Map<Integer, Integer> idToRunLength = getMapOfContiguousPartitions(cluster, zoneId);
            for(Integer initialPartitionId: idToRunLength.keySet()) {
                int runLength = idToRunLength.get(initialPartitionId);
                if(runLength < hotContiguityCutoff)
                    continue;

                int hotPartitionId = (initialPartitionId + runLength)
                                     % cluster.getNumberOfPartitions();
                Node hotNode = cluster.getNodeForPartitionId(hotPartitionId);
                sb.append("\tNode " + hotNode.getId() + " (" + hotNode.getHost()
                          + ") has hot primary partition " + hotPartitionId
                          + " that follows contiguous run of length " + runLength + Utils.NEWLINE);
            }
        }

        return sb.toString();
    }

    /**
     * Prints the details of cluster xml in various formats. Some information is
     * repeated in different forms. This is intentional so that it is easy to
     * find the specific view of the cluster xml that you want.
     * 
     * @param cluster
     * @return
     */
    public static String verboseClusterDump(final Cluster cluster) {
        StringBuilder builder = new StringBuilder();

        builder.append("CLUSTER XML SUMMARY\n");
        Map<Integer, Integer> zoneIdToPartitionCount = Maps.newHashMap();
        Map<Integer, Integer> zoneIdToNodeCount = Maps.newHashMap();
        for(Zone zone: cluster.getZones()) {
            zoneIdToPartitionCount.put(zone.getId(), 0);
            zoneIdToNodeCount.put(zone.getId(), 0);
        }
        for(Node node: cluster.getNodes()) {
            zoneIdToPartitionCount.put(node.getZoneId(),
                                       zoneIdToPartitionCount.get(node.getZoneId())
                                               + node.getNumberOfPartitions());
            zoneIdToNodeCount.put(node.getZoneId(), zoneIdToNodeCount.get(node.getZoneId()) + 1);
        }
        builder.append("\n");

        builder.append("Number of partitions per zone:\n");
        for(Zone zone: cluster.getZones()) {
            builder.append("\tZone: " + zone.getId() + " - "
                           + zoneIdToPartitionCount.get(zone.getId()) + "\n");
        }
        builder.append("\n");

        builder.append("Number of nodes per zone:\n");
        for(Zone zone: cluster.getZones()) {
            builder.append("\tZone: " + zone.getId() + " - " + zoneIdToNodeCount.get(zone.getId())
                           + "\n");
        }
        builder.append("\n");

        builder.append("Nodes in each zone:\n");
        for(Zone zone: cluster.getZones()) {
            builder.append("\tZone: " + zone.getId() + " - "
                           + cluster.getNodeIdsInZone(zone.getId()) + "\n");
        }
        builder.append("\n");

        builder.append("Number of partitions per node:\n");
        for(Node node: cluster.getNodes()) {
            builder.append("\tNode ID: " + node.getId() + " - " + node.getNumberOfPartitions()
                           + " (" + node.getHost() + ")\n");
        }
        builder.append("\n");

        if(cluster.getZones().size() > 1) {
            builder.append("ZONE-PARTITION SUMMARY:\n");
            builder.append("\n");

            builder.append("Partitions in each zone:\n");
            for(Zone zone: cluster.getZones()) {
                builder.append("\tZone: " + zone.getId() + " - "
                               + compressedListOfPartitionsInZone(cluster, zone.getId()) + "\n");
            }
            builder.append("\n");

            builder.append("Contiguous partition run lengths in each zone ('{run length : count}'):\n");
            for(Zone zone: cluster.getZones()) {
                builder.append("\tZone: " + zone.getId() + " - "
                               + getPrettyMapOfContiguousPartitionRunLengths(cluster, zone.getId())
                               + "\n");
            }
            builder.append("\n");

            builder.append("The following nodes have hot partitions:\n");
            builder.append(getHotPartitionsDueToContiguity(cluster, 5));
            builder.append("\n");
        }

        return builder.toString();
    }

    // TODO: (refactor) separate analysis from pretty printing and add a unit
    // test for the analysis sub-method.
    /**
     * Compares current cluster with final cluster. Uses pertinent store defs
     * for each cluster to determine if a node that hosts a zone-primary in the
     * current cluster will no longer host any zone-nary in the final cluster.
     * This check is the precondition for a server returning an invalid metadata
     * exception to a client on a normal-case put or get. Normal-case being that
     * the zone-primary receives the pseudo-master put or the get operation.
     * 
     * @param currentCluster
     * @param currentStoreDefs
     * @param finalCluster
     * @param finalStoreDefs
     * @return pretty-printed string documenting invalid metadata rates for each
     *         zone.
     */
    public static String analyzeInvalidMetadataRate(final Cluster currentCluster,
                                                    List<StoreDefinition> currentStoreDefs,
                                                    final Cluster finalCluster,
                                                    List<StoreDefinition> finalStoreDefs) {
        StringBuilder sb = new StringBuilder();
        sb.append("Dump of invalid metadata rates per zone").append(Utils.NEWLINE);

        HashMap<StoreDefinition, Integer> uniqueStores = StoreDefinitionUtils.getUniqueStoreDefinitionsWithCounts(currentStoreDefs);

        for(StoreDefinition currentStoreDef: uniqueStores.keySet()) {
            sb.append("Store exemplar: " + currentStoreDef.getName())
              .append(Utils.NEWLINE)
              .append("\tThere are " + uniqueStores.get(currentStoreDef) + " other similar stores.")
              .append(Utils.NEWLINE);

            StoreRoutingPlan currentSRP = new StoreRoutingPlan(currentCluster, currentStoreDef);
            StoreDefinition finalStoreDef = StoreUtils.getStoreDef(finalStoreDefs,
                                                                   currentStoreDef.getName());
            StoreRoutingPlan finalSRP = new StoreRoutingPlan(finalCluster, finalStoreDef);

            // Only care about existing zones
            for(int zoneId: currentCluster.getZoneIds()) {
                int zonePrimariesCount = 0;
                int invalidMetadata = 0;

                // Examine nodes in current cluster in existing zone.
                for(int nodeId: currentCluster.getNodeIdsInZone(zoneId)) {
                    // For every zone-primary in current cluster
                    for(int zonePrimaryPartitionId: currentSRP.getZonePrimaryPartitionIds(nodeId)) {
                        zonePrimariesCount++;
                        // Determine if original zone-primary node is still some
                        // form of n-ary in final cluster. If not,
                        // InvalidMetadataException will fire.
                        if(!finalSRP.getZoneNAryPartitionIds(nodeId)
                                    .contains(zonePrimaryPartitionId)) {
                            invalidMetadata++;
                        }
                    }
                }
                float rate = invalidMetadata / (float) zonePrimariesCount;
                sb.append("\tZone " + zoneId)
                  .append(" : total zone primaries " + zonePrimariesCount)
                  .append(", # that trigger invalid metadata " + invalidMetadata)
                  .append(" => " + rate)
                  .append(Utils.NEWLINE);
            }
        }

        return sb.toString();
    }

}
