/*
 * Copyright 2008-2009 LinkedIn, Inc
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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalanceNodePlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalanceUtilsTest extends TestCase {

    private static String storeDefFile = "test/common/voldemort/config/stores.xml";

    private Cluster currentCluster;
    private Cluster targetCluster;
    List<StoreDefinition> storeDefList;

    @Override
    public void setUp() {
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });

        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
        } catch(FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    public void testUniqueStoreDefinitions() {
        List<StoreDefinition> storeDefs = Lists.newArrayList();

        // Put zero store
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 0);

        // Put one store
        StoreDefinition consistentStore1 = ServerTestUtils.getStoreDef("a",
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       RoutingStrategyType.CONSISTENT_STRATEGY);
        storeDefs.add(consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);

        // Put another store with same rep-factor + strategy but different name
        StoreDefinition consistentStore2 = ServerTestUtils.getStoreDef("b",
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       RoutingStrategyType.CONSISTENT_STRATEGY);
        storeDefs.add(consistentStore2);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);

        // Put another store with different rep-factor
        StoreDefinition consistentStore3 = ServerTestUtils.getStoreDef("c",
                                                                       2,
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       1,
                                                                       RoutingStrategyType.CONSISTENT_STRATEGY);
        storeDefs.add(consistentStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 2);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(1), consistentStore3);

        // Put another store with same rep-factor but zone routing
        HashMap<Integer, Integer> zoneRepFactor = Maps.newHashMap();
        zoneRepFactor.put(0, 1);
        zoneRepFactor.put(1, 1);
        StoreDefinition zoneStore1 = ServerTestUtils.getStoreDef("d",
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 0,
                                                                 0,
                                                                 zoneRepFactor,
                                                                 HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                 RoutingStrategyType.ZONE_STRATEGY);
        storeDefs.add(zoneStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(1), consistentStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(2), zoneStore1);

        // Put another store with different rep-factor but zone routing
        zoneRepFactor = Maps.newHashMap();
        zoneRepFactor.put(0, 2);
        zoneRepFactor.put(1, 1);
        StoreDefinition zoneStore2 = ServerTestUtils.getStoreDef("e",
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 0,
                                                                 0,
                                                                 zoneRepFactor,
                                                                 HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                 RoutingStrategyType.ZONE_STRATEGY);
        storeDefs.add(zoneStore2);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 4);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(1), consistentStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(2), zoneStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(3), zoneStore2);

        // Put another zone store with same rep factor
        zoneRepFactor = Maps.newHashMap();
        zoneRepFactor.put(0, 1);
        zoneRepFactor.put(1, 1);
        StoreDefinition zoneStore3 = ServerTestUtils.getStoreDef("f",
                                                                 1,
                                                                 1,
                                                                 2,
                                                                 1,
                                                                 0,
                                                                 0,
                                                                 zoneRepFactor,
                                                                 HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                 RoutingStrategyType.ZONE_STRATEGY);
        storeDefs.add(zoneStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 4);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(1), consistentStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(2), zoneStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(3), zoneStore2);

        // Put another zone store with same rep factor
        zoneRepFactor = Maps.newHashMap();
        zoneRepFactor.put(0, 2);
        zoneRepFactor.put(1, 1);
        StoreDefinition zoneStore4 = ServerTestUtils.getStoreDef("g",
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 0,
                                                                 0,
                                                                 zoneRepFactor,
                                                                 HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                 RoutingStrategyType.ZONE_STRATEGY);
        storeDefs.add(zoneStore4);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 4);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(1), consistentStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(2), zoneStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(3), zoneStore2);

        // Put another store with same total rep-factor but different indiv
        // rep-factor, zone routing
        zoneRepFactor = Maps.newHashMap();
        zoneRepFactor.put(0, 1);
        zoneRepFactor.put(1, 2);
        StoreDefinition zoneStore5 = ServerTestUtils.getStoreDef("h",
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 1,
                                                                 0,
                                                                 0,
                                                                 zoneRepFactor,
                                                                 HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                 RoutingStrategyType.ZONE_STRATEGY);
        storeDefs.add(zoneStore5);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).size(), 5);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(0), consistentStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(1), consistentStore3);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(2), zoneStore1);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(3), zoneStore2);
        assertEquals(RebalanceUtils.getUniqueStoreDefinitions(storeDefs).get(4), zoneStore5);

    }

    public void testRebalancePlan() {
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true,
                                                                      null);

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node",
                     1,
                     rebalancePlan.getRebalancingTaskQueue().size());
        for(RebalanceNodePlan rebalanceNodeInfo: rebalancePlan.getRebalancingTaskQueue()) {
            assertEquals("rebalanceInfo should have exactly one item",
                         1,
                         rebalanceNodeInfo.getRebalanceTaskList().size());
            RebalancePartitionsInfo expected = new RebalancePartitionsInfo(rebalanceNodeInfo.getStealerNode(),
                                                                           0,
                                                                           Arrays.asList(2, 3),
                                                                           Arrays.asList(2, 3),
                                                                           Arrays.asList(2, 3),
                                                                           RebalanceUtils.getStoreNames(storeDefList),
                                                                           new HashMap<String, String>(),
                                                                           new HashMap<String, String>(),
                                                                           0);

            assertEquals("rebalanceStealInfo should match",
                         expected.toJsonString(),
                         rebalanceNodeInfo.getRebalanceTaskList().get(0).toJsonString());
        }
    }

    public void testRebalancePlanWithReplicationChanges() {
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6 }, { 7, 8, 9 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 3 }, { 4, 6 },
                { 7, 8, 9 }, { 1, 5 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true,
                                                                      null);

        // the rebalancing plan should have exactly 1 node
        assertEquals("There should have exactly one rebalancing node",
                     1,
                     rebalancePlan.getRebalancingTaskQueue().size());

        RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();

        assertEquals("Rebalancing node 3 should have 2 entries", 2, nodePlan.getRebalanceTaskList()
                                                                            .size());
        RebalancePartitionsInfo stealInfo0 = new RebalancePartitionsInfo(3,
                                                                         0,
                                                                         Arrays.asList(0, 1),
                                                                         Arrays.asList(1),
                                                                         Arrays.asList(1),
                                                                         RebalanceUtils.getStoreNames(storeDefList),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);
        RebalancePartitionsInfo stealInfo1 = new RebalancePartitionsInfo(3,
                                                                         1,
                                                                         Arrays.asList(4, 5),
                                                                         Arrays.asList(5),
                                                                         Arrays.asList(5),
                                                                         RebalanceUtils.getStoreNames(storeDefList),
                                                                         new HashMap<String, String>(),
                                                                         new HashMap<String, String>(),
                                                                         0);

        checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(stealInfo0, stealInfo1));
    }

    private void checkAllRebalanceInfoPresent(RebalanceNodePlan nodePlan,
                                              List<RebalancePartitionsInfo> rebalanceInfoList) {
        for(RebalancePartitionsInfo rebalanceInfo: rebalanceInfoList) {
            boolean match = false;
            for(RebalancePartitionsInfo nodeRebalanceInfo: nodePlan.getRebalanceTaskList()) {
                if(rebalanceInfo.getDonorId() == nodeRebalanceInfo.getDonorId()) {
                    assertEquals("partitions should match",
                                 true,
                                 rebalanceInfo.getPartitionList()
                                              .containsAll(nodeRebalanceInfo.getPartitionList()));
                    assertEquals("delete partitions should match",
                                 true,
                                 rebalanceInfo.getDeletePartitionsList()
                                              .containsAll(nodeRebalanceInfo.getDeletePartitionsList()));
                    assertEquals("store list should match",
                                 true,
                                 rebalanceInfo.getUnbalancedStoreList()
                                              .containsAll(nodeRebalanceInfo.getUnbalancedStoreList()));
                    assertEquals("steal master partitions should match",
                                 true,
                                 rebalanceInfo.getStealMasterPartitions()
                                              .containsAll(nodeRebalanceInfo.getStealMasterPartitions()));
                    match = true;
                }
            }

            assertNotSame("rebalancePartition Info " + rebalanceInfo
                                  + " should be present in the nodePlan "
                                  + nodePlan.getRebalanceTaskList(),
                          false,
                          match);
        }
    }

    public void testUpdateCluster() {
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    public void testRebalanceStealInfo() {
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   Arrays.asList(1, 2, 3, 4),
                                                                   new ArrayList<Integer>(0),
                                                                   Arrays.asList("test1", "test2"),
                                                                   new HashMap<String, String>(),
                                                                   new HashMap<String, String>(),
                                                                   0);
        assertEquals("RebalanceStealInfo fromString --> toString should match.",
                     info.toString(),
                     (RebalancePartitionsInfo.create(info.toJsonString())).toString());
    }

}
