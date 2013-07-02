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

package voldemort.routing;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;

import com.google.common.collect.Lists;

// TODO: Break this test into part that tests BaseStoreRoutingPlan and
// StoreRoutingPlan.
public class StoreRoutingPlanTest {

    // plan for 2 zones
    BaseStoreRoutingPlan zzBaseRoutingPlan;
    BaseStoreRoutingPlan nonZonedBaseRoutingPlan;
    // plan for 3 zones
    BaseStoreRoutingPlan zzzBaseRoutingPlan;

    // plan for 2 zones
    StoreRoutingPlan zzStoreRoutingPlan;
    StoreRoutingPlan nonZonedStoreRoutingPlan;
    // plan for 3 zones
    StoreRoutingPlan zzzStoreRoutingPlan;

    public StoreRoutingPlanTest() {}

    @Before
    public void setup() {
        Cluster nonZonedCluster = ServerTestUtils.getLocalCluster(3, new int[] { 1000, 2000, 3000,
                1000, 2000, 3000, 1000, 2000, 3000 }, new int[][] { { 0 }, { 1, 3 }, { 2 } });
        StoreDefinition nonZoned211StoreDef = new StoreDefinitionBuilder().setName("non-zoned")
                                                                          .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                          .setKeySerializer(new SerializerDefinition("string"))
                                                                          .setValueSerializer(new SerializerDefinition("string"))
                                                                          .setRoutingPolicy(RoutingTier.CLIENT)
                                                                          .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                          .setReplicationFactor(2)
                                                                          .setPreferredReads(1)
                                                                          .setRequiredReads(1)
                                                                          .setPreferredWrites(1)
                                                                          .setRequiredWrites(1)
                                                                          .build();
        nonZonedBaseRoutingPlan = new BaseStoreRoutingPlan(nonZonedCluster, nonZoned211StoreDef);
        nonZonedStoreRoutingPlan = new StoreRoutingPlan(nonZonedCluster, nonZoned211StoreDef);

        int[] dummyZonedPorts = new int[] { 1000, 2000, 3000, 1000, 2000, 3000, 1000, 2000, 3000,
                1000, 2000, 3000, 1000, 2000, 3000, 1000, 2000, 3000 };
        Cluster zzCluster = ServerTestUtils.getLocalZonedCluster(6,
                                                                 2,
                                                                 new int[] { 0, 0, 0, 1, 1, 1 },
                                                                 new int[][] { { 0 }, { 1, 6 },
                                                                         { 2 }, { 3 }, { 4, 7 },
                                                                         { 5 } },
                                                                 dummyZonedPorts);
        HashMap<Integer, Integer> zrfRWStoreWithReplication = new HashMap<Integer, Integer>();
        zrfRWStoreWithReplication.put(0, 2);
        zrfRWStoreWithReplication.put(1, 2);
        StoreDefinition zz211StoreDef = new StoreDefinitionBuilder().setName("zoned")
                                                                    .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                    .setKeySerializer(new SerializerDefinition("string"))
                                                                    .setValueSerializer(new SerializerDefinition("string"))
                                                                    .setRoutingPolicy(RoutingTier.CLIENT)
                                                                    .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                    .setReplicationFactor(4)
                                                                    .setPreferredReads(1)
                                                                    .setRequiredReads(1)
                                                                    .setPreferredWrites(1)
                                                                    .setRequiredWrites(1)
                                                                    .setZoneCountReads(0)
                                                                    .setZoneCountWrites(0)
                                                                    .setZoneReplicationFactor(zrfRWStoreWithReplication)
                                                                    .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                    .build();
        zzBaseRoutingPlan = new BaseStoreRoutingPlan(zzCluster, zz211StoreDef);
        zzStoreRoutingPlan = new StoreRoutingPlan(zzCluster, zz211StoreDef);

        Cluster zzzCluster = ServerTestUtils.getLocalZonedCluster(9, 3, new int[] { 0, 0, 0, 1, 1,
                1, 2, 2, 2 }, new int[][] { { 0 }, { 10 }, { 1, 2 }, { 3 }, { 4 }, { 6 }, { 5, 7 },
                { 9 }, { 8 } }, new int[] { 1000, 2000, 3000, 1000, 2000, 3000, 1000, 2000, 3000,
                1000, 2000, 3000, 1000, 2000, 3000, 1000, 2000, 3000, 1000, 2000, 3000, 1000, 2000,
                3000, 1000, 2000, 3000 });

        HashMap<Integer, Integer> zoneRep211 = new HashMap<Integer, Integer>();
        zoneRep211.put(0, 2);
        zoneRep211.put(1, 2);
        zoneRep211.put(2, 2);

        StoreDefinition zzz211StoreDef = new StoreDefinitionBuilder().setName("zzz")
                                                                     .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                     .setKeySerializer(new SerializerDefinition("string"))
                                                                     .setValueSerializer(new SerializerDefinition("string"))
                                                                     .setRoutingPolicy(RoutingTier.CLIENT)
                                                                     .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                     .setReplicationFactor(6)
                                                                     .setPreferredReads(1)
                                                                     .setRequiredReads(1)
                                                                     .setPreferredWrites(1)
                                                                     .setRequiredWrites(1)
                                                                     .setZoneCountReads(0)
                                                                     .setZoneCountWrites(0)
                                                                     .setZoneReplicationFactor(zoneRep211)
                                                                     .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                     .build();

        zzzBaseRoutingPlan = new BaseStoreRoutingPlan(zzzCluster, zzz211StoreDef);
        zzzStoreRoutingPlan = new StoreRoutingPlan(zzzCluster, zzz211StoreDef);
    }

    @Test
    public void testZZStoreRoutingPlan() {
        HashMap<Integer, List<byte[]>> samplePartitionKeysMap = TestUtils.createPartitionsKeys(zzStoreRoutingPlan,
                                                                                               1);
        assertEquals("Node 1 does not contain p5?",
                     (Integer) 6,
                     zzStoreRoutingPlan.getNodesPartitionIdForKey(1, samplePartitionKeysMap.get(5)
                                                                                      .get(0)));
        assertEquals("Node 4 does not contain p5?",
                     (Integer) 7,
                     zzStoreRoutingPlan.getNodesPartitionIdForKey(4, samplePartitionKeysMap.get(5)
                                                                                      .get(0)));
        assertEquals("Replication list does not match up",
                     Lists.newArrayList(0, 1, 3, 4),
                     zzStoreRoutingPlan.getReplicationNodeList(0));

        assertEquals("Zone replica type should be 1",
                     1,
                     zzBaseRoutingPlan.getZoneNAry(0, 0, samplePartitionKeysMap.get(6).get(0)));
        assertEquals("Zone replica type should be 0",
                     0,
                     zzBaseRoutingPlan.getZoneNAry(0, 1, samplePartitionKeysMap.get(6).get(0)));
        assertEquals("Zone replica type should be 1",
                     1,
                     zzBaseRoutingPlan.getZoneNAry(1, 3, samplePartitionKeysMap.get(7).get(0)));
        assertEquals("Zone replica type should be 0",
                     0,
                     zzBaseRoutingPlan.getZoneNAry(1, 4, samplePartitionKeysMap.get(7).get(0)));

        assertEquals("Replica owner should be 1",
                     1,
                     zzBaseRoutingPlan.getNodeIdForZoneNary(0, 1, samplePartitionKeysMap.get(2)
                                                                                        .get(0)));
        assertEquals("Replica owner should be 1",
                     1,
                     zzBaseRoutingPlan.getNodeIdForZoneNary(0, 0, samplePartitionKeysMap.get(3)
                                                                                        .get(0)));
        assertEquals("Replica owner should be 4",
                     4,
                     zzBaseRoutingPlan.getNodeIdForZoneNary(1, 1, samplePartitionKeysMap.get(1)
                                                                                        .get(0)));
        assertEquals("Replica owner should be 3",
                     3,
                     zzBaseRoutingPlan.getNodeIdForZoneNary(1, 0, samplePartitionKeysMap.get(2)
                                                                                        .get(0)));

        assertEquals("Does Zone 1 have a replica",
                     true,
                     zzzStoreRoutingPlan.zoneNAryExists(1, 0, 1));
    }


    @Test
    public void testZZZStoreRoutingPlan() {
        HashMap<Integer, List<byte[]>> samplePartitionKeysMap = TestUtils.createPartitionsKeys(zzzStoreRoutingPlan,
                                                                                               1);

        assertEquals("Node 1 does not contain p8?",
                     (Integer) 10,
                     zzzStoreRoutingPlan.getNodesPartitionIdForKey(1,
                                                              samplePartitionKeysMap.get(8).get(0)));

        assertEquals("Node 3 does not contain p1?",
                     (Integer) 3,
                     zzzStoreRoutingPlan.getNodesPartitionIdForKey(3,
                                                              samplePartitionKeysMap.get(1).get(0)));
        assertEquals("Replication list does not match up",
                     Lists.newArrayList(0, 2, 3, 4, 6, 8),
                     zzzStoreRoutingPlan.getReplicationNodeList(0));

        assertEquals("Replication list does not match up",
                     Lists.newArrayList(3, 4, 6, 8, 1, 0),
                     zzzStoreRoutingPlan.getReplicationNodeList(3));

        assertEquals("Zone replica type should be 0",
                     0,
                     zzzBaseRoutingPlan.getZoneNAry(0, 1, samplePartitionKeysMap.get(6).get(0)));

        assertEquals("Zone replica type should be 0 in zone 2",
                     0,
                     zzzBaseRoutingPlan.getZoneNAry(2, 6, samplePartitionKeysMap.get(6).get(0)));

        assertEquals("Replica owner should be 3",
                     3,
                     zzzBaseRoutingPlan.getNodeIdForZoneNary(1, 0, samplePartitionKeysMap.get(1)
                                                                                         .get(0)));

        assertEquals("Replica secondary should be 8",
                     8,
                     zzzBaseRoutingPlan.getNodeIdForZoneNary(2, 1, samplePartitionKeysMap.get(0)
                                                                                         .get(0)));

        assertEquals("Does Zone 2 have a replica",
                     true,
                     zzzStoreRoutingPlan.zoneNAryExists(2, 1, 0));

    }

    @Test
    public void testNonZonedStoreRoutingPlan() {
        HashMap<Integer, List<byte[]>> samplePartitionKeysMap = TestUtils.createPartitionsKeys(nonZonedStoreRoutingPlan,
                                                                                               1);

        assertEquals("Node 1 does not contain p2 as secondary?",
                     (Integer) 3,
                     nonZonedStoreRoutingPlan.getNodesPartitionIdForKey(1,
                                                                        samplePartitionKeysMap.get(2)
                                                                                            .get(0)));
        assertEquals("Replication list does not match up",
                     Lists.newArrayList(1, 2),
                     nonZonedStoreRoutingPlan.getReplicationNodeList(1));

        assertEquals("Zone replica type should be 1",
                     1,
                     nonZonedBaseRoutingPlan.getZoneNAry(Zone.DEFAULT_ZONE_ID,
                                                     2,
                                                     samplePartitionKeysMap.get(1).get(0)));
        assertEquals("Zone replica type should be 0",
                     0,
                     nonZonedBaseRoutingPlan.getZoneNAry(Zone.DEFAULT_ZONE_ID,
                                                     1,
                                                     samplePartitionKeysMap.get(3).get(0)));
        assertEquals("Replica owner should be 2",
                     2,
                     nonZonedBaseRoutingPlan.getNodeIdForZoneNary(Zone.DEFAULT_ZONE_ID,
                                                              1,
                                                              samplePartitionKeysMap.get(1).get(0)));
        assertEquals("Replica owner should be 1",
                     1,
                     nonZonedBaseRoutingPlan.getNodeIdForZoneNary(Zone.DEFAULT_ZONE_ID,
                                                              0,
                                                              samplePartitionKeysMap.get(3).get(0)));
    }

    @After
    public void teardown() {

    }
}
