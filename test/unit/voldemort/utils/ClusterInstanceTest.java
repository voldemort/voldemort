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
    public static List<StoreDefinition> getZZ111StoreDefs() {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep111 = new HashMap<Integer, Integer>();
        zoneRep111.put(0, 1);
        zoneRep111.put(1, 1);
        StoreDefinition storeDef111 = new StoreDefinitionBuilder().setName("ZZ111")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
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

    public static List<StoreDefinition> getZZ211StoreDefs() {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep211 = new HashMap<Integer, Integer>();
        zoneRep211.put(0, 2);
        zoneRep211.put(1, 2);
        StoreDefinition storeDef211 = new StoreDefinitionBuilder().setName("ZZ211")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
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

    public static List<StoreDefinition> getZZ322StoreDefs() {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep322 = new HashMap<Integer, Integer>();
        zoneRep322.put(0, 3);
        zoneRep322.put(1, 3);
        StoreDefinition storeDef322 = new StoreDefinitionBuilder().setName("ZZ322")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
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
    public static List<StoreDefinition> getZZStoreDefs() {
        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        storeDefs.addAll(getZZ111StoreDefs());
        storeDefs.addAll(getZZ211StoreDefs());
        storeDefs.addAll(getZZ322StoreDefs());
        return storeDefs;
    }

    public static List<StoreDefinition> getZZZ111StoreDefs() {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep111 = new HashMap<Integer, Integer>();
        zoneRep111.put(0, 1);
        zoneRep111.put(1, 1);
        zoneRep111.put(2, 1);
        StoreDefinition storeDef111 = new StoreDefinitionBuilder().setName("ZZ111")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
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

    public static List<StoreDefinition> getZZZ211StoreDefs() {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep211 = new HashMap<Integer, Integer>();
        zoneRep211.put(0, 2);
        zoneRep211.put(1, 2);
        zoneRep211.put(2, 2);
        StoreDefinition storeDef211 = new StoreDefinitionBuilder().setName("ZZ211")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
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

    public static List<StoreDefinition> getZZZ322StoreDefs() {

        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        HashMap<Integer, Integer> zoneRep322 = new HashMap<Integer, Integer>();
        zoneRep322.put(0, 3);
        zoneRep322.put(1, 3);
        zoneRep322.put(2, 3);
        StoreDefinition storeDef322 = new StoreDefinitionBuilder().setName("ZZ322")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
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
    public static List<StoreDefinition> getZZZStoreDefs() {
        List<StoreDefinition> storeDefs = new LinkedList<StoreDefinition>();
        storeDefs.addAll(getZZZ111StoreDefs());
        storeDefs.addAll(getZZZ211StoreDefs());
        storeDefs.addAll(getZZZ322StoreDefs());
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
        int nodesPerZone[] = new int[] { 3, 3 };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZCluster() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 3, 3, 3 };
        int partitionMap[][] = new int[][] { { 0, 9, 15, 16, 17 }, { 1, 10 }, { 2, 11 }, { 3, 12 },
                { 4, 13 }, { 5, 14 }, { 6 }, { 7 }, { 8 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZECluster() {
        int numberOfZones = 2;
        int nodesPerZone[] = new int[] { 3, 3 };
        int partitionMap[][] = new int[][] { { 0, 1, 6, 7, 12, 13, 16, 17 },
                { 2, 3, 8, 9, 14, 15 }, { 4, 5, 10, 11 }, {}, {}, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZEZCluster() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 3, 3, 3 };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, {}, {},
                {}, { 3, 12, 16 }, { 4, 13, 8 }, { 5, 14 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    /**
     * The 'N' and 'X' suffixes indicate whether there are nodes added in a zone
     * ('N') or not ('X'). By definition, an 'E' zone is labeled with 'N.
     */
    public static Cluster getZZClusterWithNN() {
        int numberOfZones = 2;
        int nodesPerZone[] = new int[] { 4, 4 };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 13 }, { 2, 8, 14 }, {},
                { 3, 9, 15 }, { 4, 10 }, { 5, 11 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZClusterWithNNN() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 4, 4, 4 };
        int partitionMap[][] = new int[][] { { 0, 9, 15, 16, 17 }, { 1, 10 }, { 2, 11 }, {},
                { 3, 12 }, { 4, 13 }, { 5, 14 }, {}, { 6 }, { 7 }, { 8 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZEZClusterWithXNN() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 3, 3, 4 };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, {}, {},
                {}, { 3, 12, 16 }, { 4, 13, 8 }, { 5, 14 }, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZECluster() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 3, 3, 3 };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 13 }, { 2, 8, 14 },
                { 3, 9, 15 }, { 4, 10 }, { 5, 11 }, {}, {}, {} };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    /**
     * This cluster is interesting because a single node cannot replicate
     * partitions to meet 3/2/2 or 2/1/1 store defs...
     */
    public static Cluster getZEZClusterWithOnlyOneNodeInNewZone() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 3, 1, 3 };
        int partitionMap[][] = new int[][] { { 0, 9, 6, 17 }, { 1, 10, 15 }, { 2, 11, 7 }, {},
                { 3, 12, 16 }, { 4, 13, 8 }, { 5, 14 } };
        return ServerTestUtils.getLocalZonedCluster(numberOfZones, nodesPerZone, partitionMap);
    }

    public static Cluster getZZZClusterWithOnlyOneNodeInNewZone() {
        int numberOfZones = 3;
        int nodesPerZone[] = new int[] { 3, 1, 3 };
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

        ci = new ClusterInstance(getZZCluster(), getZZStoreDefs());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZZZCluster(), getZZZStoreDefs());
        ci.getPartitionBalance();
    }

    @Test
    public void testEmptyZoneThingsThatShouldWork() {
        ClusterInstance ci;

        ci = new ClusterInstance(getZECluster(), getZZStoreDefs());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZEZCluster(), getZZZStoreDefs());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZEZClusterWithOnlyOneNodeInNewZone(), getZZZStoreDefs());
        ci.getPartitionBalance();
    }

    @Test
    public void testNewNodeThingsThatShouldWork() {
        ClusterInstance ci;

        ci = new ClusterInstance(getZZClusterWithNN(), getZZStoreDefs());
        ci.getPartitionBalance();

        ci = new ClusterInstance(getZEZClusterWithXNN(), getZZZStoreDefs());
        ci.getPartitionBalance();
    }

    @Test
    public void testClusterStoreZoneCountMismatch() {
        ClusterInstance ci;
        boolean veCaught;

        veCaught = false;
        try {
            ci = new ClusterInstance(getZZCluster(), getZZZStoreDefs());
            ci.getPartitionBalance();
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);

        veCaught = false;
        try {
            ci = new ClusterInstance(getZZZCluster(), getZZStoreDefs());
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
            ci = new ClusterInstance(getZZZClusterWithOnlyOneNodeInNewZone(), getZZZStoreDefs());
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
                                 getZZStoreDefs());
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
                                     getZZStoreDefs());
            ci.getPartitionBalance();
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }
}
