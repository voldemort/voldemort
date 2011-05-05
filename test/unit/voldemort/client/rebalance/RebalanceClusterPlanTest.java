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

package voldemort.client.rebalance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalanceClusterPlanTest extends TestCase {

    private static String storeDefFile = "test/common/voldemort/config/stores.xml";
    private Cluster currentCluster;
    private Cluster targetCluster;
    private List<StoreDefinition> storeDefList;
    private List<StoreDefinition> storeDefList2;

    @Override
    public void setUp() {
        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
            storeDefList2 = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
        } catch(FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    /**
     * Tests the scenario where-in a migration causes a drop in the number of
     * replicas
     */
    public void testRebalancePlanInsufficientReplicas() {
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0 }, { 1 }, { 2 } });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 1 }, { 0 }, { 2 } });

        try {
            new RebalanceClusterPlan(currentCluster, targetCluster, storeDefList, true);
            fail("Should have thrown an exception since the migration should result in decrease in replication factor");
        } catch(VoldemortException e) {}

    }

    public void testRebalancePlanDelete() {

        // CASE 1
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 1, 2, 3 },
                { 4, 5, 6, 7 }, { 0 } });

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                                              targetCluster,
                                                                                              Lists.newArrayList(ServerTestUtils.getStoreDef("test",
                                                                                                                                             2,
                                                                                                                                             1,
                                                                                                                                             1,
                                                                                                                                             1,
                                                                                                                                             1,
                                                                                                                                             RoutingStrategyType.CONSISTENT_STRATEGY))).getOrderedRebalanceNodePlanList();
        assertEquals("There should be exactly 2 rebalancing node",
                     2,
                     orderedRebalanceNodePlanList.size());
        assertEquals("Stealer 2 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        HashMap<Integer, List<Integer>> partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(0, Lists.newArrayList(0));
        partitionsToMove.put(1, Lists.newArrayList(5, 4, 7, 6));
        HashMap<Integer, List<Integer>> partitionsToDelete = Maps.newHashMap();
        partitionsToDelete.put(1, Lists.newArrayList(5, 4, 7, 6));

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(2,
                                                                               0,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               Lists.newArrayList("test"),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(1, Lists.newArrayList(0));
        partitionsToDelete = Maps.newHashMap();
        partitionsToDelete.put(1, Lists.newArrayList(0));

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               1,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               Lists.newArrayList("test"),
                                                                               currentCluster,
                                                                               0)));

        // CASE 2

        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 1, 2, 3 },
                { 4, 5, 6, 7, 0 } });

        orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                      targetCluster,
                                                                      Lists.newArrayList(ServerTestUtils.getStoreDef("test",
                                                                                                                     2,
                                                                                                                     1,
                                                                                                                     1,
                                                                                                                     1,
                                                                                                                     1,
                                                                                                                     RoutingStrategyType.CONSISTENT_STRATEGY))).getOrderedRebalanceNodePlanList();
        assertEquals("There should be exactly 2 rebalancing node",
                     2,
                     orderedRebalanceNodePlanList.size());
        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(0, Lists.newArrayList(0));
        partitionsToDelete = Maps.newHashMap();

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               Lists.newArrayList("test"),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(1, Lists.newArrayList(0));
        partitionsToDelete = Maps.newHashMap();

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               1,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               Lists.newArrayList("test"),
                                                                               currentCluster,
                                                                               0)));

    }

    /**
     * Tests the case where-in we delete all the partitions from the last node
     */
    public void testRebalancePlanDeleteLastNode() {

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 3, 6, 9, 12, 15 },
                { 1, 4, 7, 10, 13, 16 }, { 2, 5, 8, 11, 14, 17 }, { 0 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 3, 6, 9, 12, 15 },
                { 1, 4, 7, 10, 13, 16 }, { 2, 5, 8, 11, 14, 17 }, {} });

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                                              targetCluster,
                                                                                              storeDefList2).getOrderedRebalanceNodePlanList();
        assertEquals("There should be exactly 1 rebalancing node",
                     1,
                     orderedRebalanceNodePlanList.size());

        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        HashMap<Integer, List<Integer>> partitionsToMove = Maps.newHashMap();
        partitionsToMove.clear();
        partitionsToMove.put(0, Lists.newArrayList(0));
        partitionsToMove.put(1, Lists.newArrayList(17));
        partitionsToMove.put(2, Lists.newArrayList(16));

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               3,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
    }

    /**
     * Tests the scenario where-in we delete the first node
     */
    public void testRebalancePlanDeleteFirstNode() {

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 4 }, { 0, 1, 5 },
                { 2, 6 }, { 3, 7 } });

        // PHASE 1

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                                              targetCluster,
                                                                                              storeDefList2).getOrderedRebalanceNodePlanList();

        assertEquals("There should be exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());
        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());

        HashMap<Integer, List<Integer>> partitionsToMove = Maps.newHashMap();
        partitionsToMove.clear();
        partitionsToMove.put(0, Lists.newArrayList(0));
        partitionsToMove.put(1, Lists.newArrayList(7));
        partitionsToMove.put(2, Lists.newArrayList(6));

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 2 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(1, Lists.newArrayList(0));
        partitionsToMove.put(2, Lists.newArrayList(7));
        HashMap<Integer, List<Integer>> partitionsToDelete = Maps.newHashMap();
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(2,
                                                                               1,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(0));
        partitionsToDelete.clear();
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

        // PHASE 2

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 4 }, { 0, 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { {}, { 0, 1, 5 },
                { 4, 2, 6 }, { 3, 7 } });

        orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList2).getOrderedRebalanceNodePlanList();

        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        assertEquals("Stealer 2 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(0, Lists.newArrayList(4));
        partitionsToMove.put(1, Lists.newArrayList(3));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(2,
                                                                               0,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(2));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(4));
        partitionsToDelete.clear();
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

    }

    public void testRebalanceDeletingMiddleNode() {
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 2, 1, 5 },
                { 6 }, { 3, 7 } });

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                                              targetCluster,
                                                                                              storeDefList2).getOrderedRebalanceNodePlanList();

        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        HashMap<Integer, List<Integer>> partitionsToMove = Maps.newHashMap();
        partitionsToMove.clear();
        partitionsToMove.put(0, Lists.newArrayList(2));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               2,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(1));
        HashMap<Integer, List<Integer>> partitionsToDelete = Maps.newHashMap();
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               3,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(1, Lists.newArrayList(1));
        partitionsToMove.put(2, Lists.newArrayList(0));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 2, 1, 5 },
                { 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 2, 1, 5 }, {},
                { 6, 3, 7 } });

        orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList2).getOrderedRebalanceNodePlanList();

        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(0, Lists.newArrayList(6));
        partitionsToMove.put(1, Lists.newArrayList(5));
        partitionsToMove.put(2, Lists.newArrayList(4));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               partitionsToMove,
                                                                               partitionsToMove,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(1, Lists.newArrayList(6));
        partitionsToMove.put(2, Lists.newArrayList(5));
        partitionsToDelete.clear();
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               3,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(6));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               partitionsToMove,
                                                                               partitionsToDelete,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
    }

    public void testRebalancePlanWithReplicationChanges() {
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6 }, { 7, 8, 9 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 3 }, { 4, 6 },
                { 7, 8, 9 }, { 1, 5 } });

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                                              targetCluster,
                                                                                              storeDefList).getOrderedRebalanceNodePlanList();

        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        assertEquals("Stealer 3 should have 3 entry",
                     3,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        HashMap<Integer, List<Integer>> partitionsToMove1 = Maps.newHashMap(), partitionsToMove2 = Maps.newHashMap(), partitionsToMove3 = Maps.newHashMap();
        HashMap<Integer, List<Integer>> partitionsToDelete1 = Maps.newHashMap(), partitionsToDelete2 = Maps.newHashMap(), partitionsToDelete3 = Maps.newHashMap();
        partitionsToMove1.put(0, Lists.newArrayList(1));
        partitionsToMove2.put(0, Lists.newArrayList(5));
        partitionsToMove2.put(1, Lists.newArrayList(0));
        partitionsToMove3.put(1, Lists.newArrayList(4));
        partitionsToDelete2.put(1, Lists.newArrayList(0));
        partitionsToDelete3.put(1, Lists.newArrayList(4));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               0,
                                                                               partitionsToMove1,
                                                                               partitionsToDelete1,
                                                                               RebalanceUtils.getStoreNames(storeDefList),
                                                                               currentCluster,
                                                                               0),
                                                   new RebalancePartitionsInfo(3,
                                                                               1,
                                                                               partitionsToMove2,
                                                                               partitionsToDelete2,
                                                                               RebalanceUtils.getStoreNames(storeDefList),
                                                                               currentCluster,
                                                                               0),
                                                   new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               partitionsToMove3,
                                                                               partitionsToDelete3,
                                                                               RebalanceUtils.getStoreNames(storeDefList),
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove1.clear();
        partitionsToMove1.put(1, Lists.newArrayList(1));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               1,
                                                                               partitionsToMove1,
                                                                               partitionsToMove1,
                                                                               RebalanceUtils.getStoreNames(storeDefList),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove1.clear();
        partitionsToMove1.put(1, Lists.newArrayList(5));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               2,
                                                                               partitionsToMove1,
                                                                               partitionsToMove1,
                                                                               RebalanceUtils.getStoreNames(storeDefList),
                                                                               currentCluster,
                                                                               0)));

    }

    /**
     * Issue 288
     */
    public void testRebalanceAllReplicasBeingMigrated() {
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 2, 3 },
                { 1, 5 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 4 }, { 2, 3 }, { 1, 5 },
                { 0 } });

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = createOrderedClusterTransition(currentCluster,
                                                                                              targetCluster,
                                                                                              storeDefList2).getOrderedRebalanceNodePlanList();

        assertEquals("There should have exactly 3 rebalancing node",
                     1,
                     orderedRebalanceNodePlanList.size());

        assertEquals("Stealer 3 should have 2 entry",
                     2,
                     orderedRebalanceNodePlanList.get(0).getRebalanceTaskList().size());
        HashMap<Integer, List<Integer>> partitionsToMove1 = Maps.newHashMap(), partitionsToMove2 = Maps.newHashMap();
        partitionsToMove1.put(0, Lists.newArrayList(0));
        partitionsToMove1.put(1, Lists.newArrayList(5));
        partitionsToMove2.put(2, Lists.newArrayList(4));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               0,
                                                                               partitionsToMove1,
                                                                               partitionsToMove1,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0),
                                                   new RebalancePartitionsInfo(3,
                                                                               1,
                                                                               partitionsToMove2,
                                                                               partitionsToMove2,
                                                                               RebalanceUtils.getStoreNames(storeDefList2),
                                                                               currentCluster,
                                                                               0)));
    }

    private void checkAllRebalanceInfoPresent(RebalanceNodePlan nodePlan,
                                              List<RebalancePartitionsInfo> rebalanceInfoList) {
        for(RebalancePartitionsInfo rebalanceInfo: rebalanceInfoList) {
            boolean match = false;
            for(RebalancePartitionsInfo nodeRebalanceInfo: nodePlan.getRebalanceTaskList()) {
                if(rebalanceInfo.getDonorId() == nodeRebalanceInfo.getDonorId()) {
                    assertEquals("partitions should match",
                                 true,
                                 Utils.compareList(rebalanceInfo.getPartitions(),
                                                   nodeRebalanceInfo.getPartitions()));

                    assertEquals("store list should match",
                                 true,
                                 Utils.compareList(rebalanceInfo.getUnbalancedStoreList(),
                                                   nodeRebalanceInfo.getUnbalancedStoreList()));
                    for(Entry<Integer, List<Integer>> entry: rebalanceInfo.getReplicaToPartitionList()
                                                                          .entrySet()) {
                        assertEquals("partition moved should match for " + entry.getKey()
                                             + " replica type",
                                     true,
                                     Utils.compareList(entry.getValue(),
                                                       nodeRebalanceInfo.getReplicaToPartitionList()
                                                                        .get(entry.getKey())));
                    }
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

    /**
     * Given the current and target cluster metadata, along with your store
     * definition generates the ordered transition
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster Target cluster metadata
     * @param storeDef List of store definitions
     * @return Ordered cluster transition
     */
    private OrderedClusterTransition createOrderedClusterTransition(Cluster currentCluster,
                                                                    Cluster targetCluster,
                                                                    List<StoreDefinition> storeDef) {
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDef,
                                                                      true);
        final OrderedClusterTransition orderedClusterTransition = new OrderedClusterTransition(currentCluster,
                                                                                               targetCluster,
                                                                                               storeDef,
                                                                                               rebalancePlan);
        return orderedClusterTransition;
    }

}