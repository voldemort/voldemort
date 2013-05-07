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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;

import com.google.common.collect.Maps;

// TODO: (refactor) Move all of the static "util" methods for which Cluster is
// the only complex type that the method operates on to be members of the
// Cluster class. Unclear whether 'nodeid' and 'partitionid' should be treated
// as complex types since they are proxies for complicated concepts.
/**
 * ClusterUtils provides basic tools for manipulating and inspecting a cluster.
 * 
 * Methods in this util module should take exactly one Cluster object, and
 * possibly some other minor, simple arguments. A method that takes other
 * complicated types such as StoreDefs or RebalancePlans should not be included
 * in this module.
 */
public class ClusterUtils {

    private static Logger logger = Logger.getLogger(ClusterUtils.class);

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
            if(cluster.getNodeForPartitionId(partition).getId() == nodeId) {
                return true;
            }
        }
        return false;
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
     * Compress contiguous partitions into format "e-i" instead of
     * "e, f, g, h, i". This helps illustrate contiguous partitions within a
     * zone.
     * 
     * @param cluster
     * @param zoneId
     * @return
     */
    public static String compressedListOfPartitionsInZone(final Cluster cluster, int zoneId) {
        Map<Integer, Integer> idToRunLength = ClusterUtils.getMapOfContiguousPartitions(cluster,
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
    public static Map<Integer, Integer> getMapOfContiguousPartitionRunLengths(final Cluster cluster,
                                                                              int zoneId) {
        Map<Integer, Integer> idToRunLength = ClusterUtils.getMapOfContiguousPartitions(cluster,
                                                                                        zoneId);
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
            Map<Integer, Integer> idToRunLength = ClusterUtils.getMapOfContiguousPartitions(cluster,
                                                                                            zoneId);
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
                builder.append("\tZone: "
                               + zone.getId()
                               + " - "
                               + ClusterUtils.compressedListOfPartitionsInZone(cluster,
                                                                               zone.getId()) + "\n");
            }
            builder.append("\n");

            builder.append("Contiguous partition run lengths in each zone ('{run length : count}'):\n");
            for(Zone zone: cluster.getZones()) {
                builder.append("\tZone: "
                               + zone.getId()
                               + " - "
                               + ClusterUtils.getPrettyMapOfContiguousPartitionRunLengths(cluster,
                                                                                          zone.getId())
                               + "\n");
            }
            builder.append("\n");

            builder.append("The following nodes have hot partitions:\n");
            builder.append(ClusterUtils.getHotPartitionsDueToContiguity(cluster, 5));
            builder.append("\n");
        }

        return builder.toString();
    }
}
