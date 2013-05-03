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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;

import com.google.common.collect.Lists;

/**
 * This test focuses on constructing ClusterInstances and then invoking
 * analyzeBalanceVerbose(). This method heavily exercises much of the
 * partition/replicaType code paths.
 * 
 * The positive test cases cover expected configurations:
 * <ul>
 * <li>2 or 3 zones
 * <li>0 or 1 zones without partitions
 * <li>one or more zones with new nodes ('new' meaning no partitions on that
 * node)
 * </ul>
 * 
 * The "negative test cases" cover:
 * <ul>
 * <li>Store definition mis-match with cluster in terms of number of zones.
 * <li>Insufficient nodes in new zone to reach desired replication level.
 * </ul>
 * 
 */
public class ClusterInstanceTest {

    // TODO: Rename class/file to PartitionBalanceTest. Change tests to directly
    // use PartitionBalance rather than go through ClusterInstance.

    // TODO: Move these storeDefs and cluster helper test methods into
    // ClusterTestUtils.
    public static List<StoreDefinition> getZZ111StoreDefs(String storageType) {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep111 = new HashMap<Integer, Integer>();
        zoneRep111.put(0, 1);
        zoneRep111.put(1, 1);
        StoreDefinition storeDef111 = new StoreDefinitionBuilder().setName("ZZ111")
                                                                  .setType(storageType)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(2)
                                                                  .setZoneReplicationFactor(zoneRep111)
                                                                  .setRequiredReads(1)
                                                                  .setRequiredWrites(1)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        storeDefs.add(storeDef111);
        return storeDefs;
    }

    public static List<StoreDefinition> getZZ211StoreDefs(String storageType) {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep211 = new HashMap<Integer, Integer>();
        zoneRep211.put(0, 2);
        zoneRep211.put(1, 2);
        StoreDefinition storeDef211 = new StoreDefinitionBuilder().setName("ZZ211")
                                                                  .setType(storageType)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(4)
                                                                  .setZoneReplicationFactor(zoneRep211)
                                                                  .setRequiredReads(1)
                                                                  .setRequiredWrites(1)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        storeDefs.add(storeDef211);
        return storeDefs;
    }

    public static List<StoreDefinition> getZZ322StoreDefs(String storageType) {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep322 = new HashMap<Integer, Integer>();
        zoneRep322.put(0, 3);
        zoneRep322.put(1, 3);
        StoreDefinition storeDef322 = new StoreDefinitionBuilder().setName("ZZ322")
                                                                  .setType(storageType)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(6)
                                                                  .setZoneReplicationFactor(zoneRep322)
                                                                  .setRequiredReads(2)
                                                                  .setRequiredWrites(2)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        storeDefs.add(storeDef322);
        return storeDefs;
    }

    /**
     * Store defs for zoned clusters with 2 zones. Covers the three store
     * definitions of interest: 3/2/2, 2/1/1, and
     */
    public static List<StoreDefinition> getZZStoreDefsInMemory() {
        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        storeDefs.addAll(getZZ111StoreDefs(InMemoryStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZ211StoreDefs(InMemoryStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZ322StoreDefs(InMemoryStorageConfiguration.TYPE_NAME));
        return storeDefs;
    }

    public static List<StoreDefinition> getZZStoreDefsBDB() {
        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        storeDefs.addAll(getZZ111StoreDefs(BdbStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZ211StoreDefs(BdbStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZ322StoreDefs(BdbStorageConfiguration.TYPE_NAME));
        return storeDefs;
    }

    public static List<StoreDefinition> getZZZ111StoreDefs(String storageType) {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep111 = new HashMap<Integer, Integer>();
        zoneRep111.put(0, 1);
        zoneRep111.put(1, 1);
        zoneRep111.put(2, 1);
        StoreDefinition storeDef111 = new StoreDefinitionBuilder().setName("ZZ111")
                                                                  .setType(storageType)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(3)
                                                                  .setZoneReplicationFactor(zoneRep111)
                                                                  .setRequiredReads(1)
                                                                  .setRequiredWrites(1)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        storeDefs.add(storeDef111);
        return storeDefs;
    }

    public static List<StoreDefinition> getZZZ211StoreDefs(String storageType) {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep211 = new HashMap<Integer, Integer>();
        zoneRep211.put(0, 2);
        zoneRep211.put(1, 2);
        zoneRep211.put(2, 2);
        StoreDefinition storeDef211 = new StoreDefinitionBuilder().setName("ZZ211")
                                                                  .setType(storageType)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(6)
                                                                  .setZoneReplicationFactor(zoneRep211)
                                                                  .setRequiredReads(1)
                                                                  .setRequiredWrites(1)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        storeDefs.add(storeDef211);
        return storeDefs;
    }

    public static List<StoreDefinition> getZZZ322StoreDefs(String storageType) {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep322 = new HashMap<Integer, Integer>();
        zoneRep322.put(0, 3);
        zoneRep322.put(1, 3);
        zoneRep322.put(2, 3);
        StoreDefinition storeDef322 = new StoreDefinitionBuilder().setName("ZZ322")
                                                                  .setType(storageType)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(9)
                                                                  .setZoneReplicationFactor(zoneRep322)
                                                                  .setRequiredReads(2)
                                                                  .setRequiredWrites(2)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        storeDefs.add(storeDef322);
        return storeDefs;
    }

    /**
     * Store defs for zoned clusters with 2 zones. Covers the three store
     * definitions of interest: 3/2/2, 2/1/1, and
     */
    public static List<StoreDefinition> getZZZStoreDefsInMemory() {
        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        storeDefs.addAll(getZZZ111StoreDefs(InMemoryStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZZ211StoreDefs(InMemoryStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZZ322StoreDefs(InMemoryStorageConfiguration.TYPE_NAME));
        return storeDefs;
    }

    public static List<StoreDefinition> getZZZStoreDefsBDB() {
        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        storeDefs.addAll(getZZZ111StoreDefs(BdbStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZZ211StoreDefs(BdbStorageConfiguration.TYPE_NAME));
        storeDefs.addAll(getZZZ322StoreDefs(BdbStorageConfiguration.TYPE_NAME));
        return storeDefs;
    }

    // NOTE: All clusters must have 18 partitions in them! This allows the
    // clusters to be used in rebalancing tests. Skewed distributions of
    // partitions is also intentional (i.e., some servers having more
    // partitions, and some zones having contiguous runs of partitions).

    /**
     * The 'Z' and 'E' prefixes in these method names indicate zones with
     * partitions and zones without partitions.
     */
    public static Cluster getZZCluster() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZClusterWithExtraPartitions() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 11, 18 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZClusterWithSwappedPartitions() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 6, 16, 17 }, { 1, 7, 15 }, { 2, 8, 11, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 12 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZCluster() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7, 8 } };
        int partitionMap[][] = new int[][] { { 0, 9, 15, 16, 17 }, { 1, 10 }, { 2, 11 }, { 3, 12 },
                { 4, 13 }, { 5, 14 }, { 6 }, { 7 }, { 8 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZClusterWithSwappedPartitions() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7, 8 } };
        int partitionMap[][] = new int[][] { { 0, 9, 17 }, { 1, 10 }, { 2, 11 }, { 3, 12 },
                { 4, 13 }, { 5, 14 }, { 6 }, { 7, 15 }, { 8, 16 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZECluster() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 1, 6, 7, 12, 13, 16, 17 },
                { 2, 3, 8, 9, 14, 15 }, { 4, 5, 10, 11 }, {}, {}, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZEZCluster() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 6, 7, 8 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, {}, {},
                {}, { 3, 12, 16 }, { 4, 13, 8 }, { 5, 14 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZECluster() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7, 8 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 }, {}, {}, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    /**
     * The 'N' and 'X' suffixes indicate whether there are nodes added in a zone
     * ('N') or not ('X'). By definition, an 'E' zone is labeled with 'N.
     */
    public static Cluster getZZClusterWithNN() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2, 6 }, { 3, 4, 5, 7 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 }, {},
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZClusterWithNNWithSwappedNodeIds() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 6, 2 }, { 3, 4, 5, 7 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 }, {},
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZClusterWithNNN() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2, 9 }, { 3, 4, 5, 10 }, { 6, 7, 8, 11 } };
        int partitionMap[][] = new int[][] { { 0, 9, 15, 16, 17 }, { 1, 10 }, { 2, 11 }, {},
                { 3, 12 }, { 4, 13 }, { 5, 14 }, {}, { 6 }, { 7 }, { 8 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZEZClusterWithXNN() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 6, 7, 8 }, { 3, 4, 5, 9 } };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, {}, {},
                {}, { 3, 12, 16 }, { 4, 13, 8 }, { 5, 14 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    /**
     * The 'P' and 'X' suffixes indicate whether there are nodes added in a zone
     * ('N') that have been populated with partitions. ('X' indicates no new
     * nodes in zone).
     */
    public static Cluster getZZClusterWithPP() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2, 6 }, { 3, 4, 5, 7 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 7, 13 }, { 8, 14 }, { 1, 2 },
                { 9, 15 }, { 10 }, { 5, 11 }, { 3, 4 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZClusterWithPPWithSwappedNodeIds() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 6, 2 }, { 3, 4, 5, 7 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 7, 13 }, { 8, 14 }, { 1, 2 },
                { 9, 15 }, { 10 }, { 5, 11 }, { 3, 4 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZClusterWithPPWithTooManyNodes() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 6, 2 }, { 3, 4, 5, 7, 8 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 7, 13 }, { 8, 14 }, { 1, 2 },
                { 9, 15 }, { 10 }, { 5, 11 }, { 3 }, { 4 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZClusterWithPPP() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2, 9 }, { 3, 4, 5, 10 }, { 6, 7, 8, 11 } };
        int partitionMap[][] = new int[][] { { 0, 15, 16, 17 }, { 1, 10 }, { 11 }, { 2 }, { 12 },
                { 4, 13 }, { 5, 14 }, { 3 }, { 6 }, { 7 }, { 8 }, { 9 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZEZClusterWithXPP() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 6, 7, 8 }, { 3, 4, 5, 9 } };
        int partitionMap[][] = new int[][] { { 0, 17 }, { 1, 15 }, { 2, 11, 7 }, { 6 }, { 9 },
                { 10 }, { 3, 12, 16 }, { 4, 8 }, { 5, 14 }, { 13 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZEClusterXXP() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7, 8 } };
        int partitionMap[][] = new int[][] { { 16, 17 }, { 1, 13 }, { 8, 14 }, { 3, 15 },
                { 4, 10 }, { 5, 11 }, { 0, 6 }, { 2, 12 }, { 7, 9 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    /**
     * This cluster is interesting because a single node cannot replicate
     * partitions to meet 3/2/2 or 2/1/1 store defs...
     */
    public static Cluster getZEZClusterWithOnlyOneNodeInNewZone() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 6 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, {},
                { 3, 12, 16 }, { 4, 13, 8 }, { 5, 14 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZClusterWithOnlyOneNodeInNewZone() {
        int numberOfZones = 3;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 6 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, { 14 },
                { 3, 12, 16 }, { 4, 13, 8 }, { 5 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    /**
     * Construct 2 zones with zone IDs 0 and 2 respectively. The node ids are
     * remapped to be contiguous though. This partially emulates "shrinking"
     * zones...
     */
    public static Cluster getZZClusterWithNonContiguousZoneIDsButContiguousNodeIDs() {

        // Hand construct zones 0 and 2
        List<Zone> zones = Lists.newArrayList();
        LinkedList<Integer> proximityList0 = Lists.newLinkedList();
        proximityList0.add(2);
        zones.add(new Zone(0, proximityList0));
        LinkedList<Integer> proximityList2 = Lists.newLinkedList();
        proximityList2.add(0);
        zones.add(new Zone(2, proximityList2));

        // Use getZEZCluster because zone 1 does not have any partitions in it!
        Cluster cluster = getZEZCluster();
        List<Node> nodeList = new ArrayList<Node>();

        int nodeId = 0; // Needed because node IDs must be contiguous?
        for(Node node: cluster.getNodes()) {
            // Do *not* add node from zone 1.
            if(node.getZoneId() != 1) {
                Node newNode = new Node(nodeId,
                                        node.getHost(),
                                        node.getHttpPort(),
                                        node.getSocketPort(),
                                        node.getAdminPort(),
                                        node.getPartitionIds());
                nodeList.add(newNode);
                nodeId++;
            }
        }

        Collections.sort(nodeList);

        return new Cluster(cluster.getName(), nodeList, zones);
    }

    /**
     * Construct 2 zones with zone IDs 0 and 2 respectively and with nodes that
     * are not contiguously numbered. This fully emulates "shrinking" zones...
     */
    public static Cluster getZZClusterWithNonContiguousZoneIDsAndNonContiguousNodeIDs() {

        // Hand construct zones 0 and 2
        List<Zone> zones = Lists.newArrayList();
        LinkedList<Integer> proximityList0 = Lists.newLinkedList();
        proximityList0.add(2);
        zones.add(new Zone(0, proximityList0));
        LinkedList<Integer> proximityList2 = Lists.newLinkedList();
        proximityList2.add(0);
        zones.add(new Zone(2, proximityList2));

        // Use getZEZCluster because zone 1 does not have any partitions in it!
        Cluster cluster = getZEZCluster();
        List<Node> nodeList = new ArrayList<Node>();
        for(Node node: cluster.getNodes()) {
            // Do *not* add node from zone 1.
            if(node.getZoneId() != 1) {
                nodeList.add(node);
            }
        }
        Collections.sort(nodeList);

        return new Cluster(cluster.getName(), nodeList, zones);
    }

    @Test
    public void testBasicThingsThatShouldWork() {
        ClusterInstance ci;

        ci = new ClusterInstance(getZZCluster(), getZZStoreDefsInMemory());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZZZCluster(), getZZZStoreDefsInMemory());
        ci.getPartitionBalance();
    }

    @Test
    public void testEmptyZoneThingsThatShouldWork() {
        ClusterInstance ci;

        ci = new ClusterInstance(getZECluster(), getZZStoreDefsInMemory());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZEZCluster(), getZZZStoreDefsInMemory());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZEZClusterWithOnlyOneNodeInNewZone(), getZZZStoreDefsInMemory());
        ci.getPartitionBalance();
    }

    @Test
    public void testNewNodeThingsThatShouldWork() {
        ClusterInstance ci;

        ci = new ClusterInstance(getZZClusterWithNN(), getZZStoreDefsInMemory());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZEZClusterWithXNN(), getZZZStoreDefsInMemory());
        ci.getPartitionBalance();
    }

    @Test
    public void testClusterStoreZoneCountMismatch() {
        ClusterInstance ci;
        boolean veCaught;

        veCaught = false;
        try {
            ci = new ClusterInstance(getZZCluster(), getZZZStoreDefsInMemory());
            ci.getPartitionBalance();
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);

        veCaught = false;
        try {
            ci = new ClusterInstance(getZZZCluster(), getZZStoreDefsInMemory());
            ci.getPartitionBalance();
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }

    @Test
    public void testClusterWithZoneThatCannotFullyReplicate() {
        ClusterInstance ci;

        boolean veCaught = false;
        try {
            ci = new ClusterInstance(getZZZClusterWithOnlyOneNodeInNewZone(),
                                     getZZZStoreDefsInMemory());
            ci.getPartitionBalance();
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }

    /**
     * Confirm that zone Ids need not be contiguous. This tests for the ability
     * to shrink zones.
     */
    @Test
    public void testNonContiguousZonesThatShouldWork() {
        ClusterInstance ci;

        ci = new ClusterInstance(getZZClusterWithNonContiguousZoneIDsButContiguousNodeIDs(),
                                 getZZStoreDefsInMemory());
        ci.getPartitionBalance();
    }

    // TODO: Fix handling of node Ids so that they do not need to be contiguous.
    /**
     * This should be a positive test. But, for now, is a negative test to
     * confirm that we require nodeIds to be contiguous. This may become a
     * problem if we ever shrink the number of zones.
     */
    @Test
    public void testNonContiguousZonesThatShouldWorkButDoNot() {
        ClusterInstance ci;

        boolean veCaught = false;
        try {
            ci = new ClusterInstance(getZZClusterWithNonContiguousZoneIDsAndNonContiguousNodeIDs(),
                                     getZZStoreDefsInMemory());
            ci.getPartitionBalance();
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }
}
