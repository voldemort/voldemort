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

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;
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

        // Partitions to move
        HashMap<Integer, List<Integer>> partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(0, Lists.newArrayList(0));
        partitionsToMove.put(1, Lists.newArrayList(5, 4, 7, 6));
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);

        // Partitions to delete
        HashMap<Integer, List<Integer>> partitionsToDelete = Maps.newHashMap();
        partitionsToDelete.put(1, Lists.newArrayList(5, 4, 7, 6));
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToDelete = Maps.newHashMap();
        storeToPartitionsToDelete.put("test", partitionsToDelete);

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(2,
                                                                               0,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToDelete,
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(1, Lists.newArrayList(0));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);

        partitionsToDelete = Maps.newHashMap();
        partitionsToDelete.put(1, Lists.newArrayList(0));
        storeToPartitionsToDelete = Maps.newHashMap();
        storeToPartitionsToDelete.put("test", partitionsToDelete);

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               1,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToDelete,
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
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);

        partitionsToDelete = Maps.newHashMap();
        storeToPartitionsToDelete = Maps.newHashMap();

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToDelete,
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove = Maps.newHashMap();
        partitionsToMove.put(1, Lists.newArrayList(0));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);

        partitionsToDelete = Maps.newHashMap();
        storeToPartitionsToDelete = Maps.newHashMap();

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               1,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToDelete,
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
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               3,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
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
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 2 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(1, Lists.newArrayList(0));
        partitionsToMove.put(2, Lists.newArrayList(7));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(2,
                                                                               1,
                                                                               storeToPartitionsToMove,
                                                                               new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(0));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               storeToPartitionsToMove,
                                                                               new HashMap<String, HashMap<Integer, List<Integer>>>(),
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
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(2,
                                                                               0,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(2));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(4));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               storeToPartitionsToMove,
                                                                               new HashMap<String, HashMap<Integer, List<Integer>>>(),
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
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               2,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(1));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               3,
                                                                               storeToPartitionsToMove,
                                                                               new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 3 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(1, Lists.newArrayList(1));
        partitionsToMove.put(2, Lists.newArrayList(0));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
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
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               storeToPartitionsToMove,
                                                                               storeToPartitionsToMove,
                                                                               currentCluster,
                                                                               0)));
        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(1, Lists.newArrayList(6));
        partitionsToMove.put(2, Lists.newArrayList(5));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               3,
                                                                               storeToPartitionsToMove,
                                                                               new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                                               currentCluster,
                                                                               0)));

        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());
        partitionsToMove.clear();
        partitionsToMove.put(2, Lists.newArrayList(6));
        storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", partitionsToMove);
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               0,
                                                                               storeToPartitionsToMove,
                                                                               new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                                               currentCluster,
                                                                               0)));
    }

    @SuppressWarnings("unchecked")
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
        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(1).getRebalanceTaskList().size());
        assertEquals("Stealer 1 should have 1 entry",
                     1,
                     orderedRebalanceNodePlanList.get(2).getRebalanceTaskList().size());

        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove[] = new HashMap[5];
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToDelete[] = new HashMap[5];

        for(int numPlan = 0; numPlan < 5; numPlan++) {
            storeToPartitionsToMove[numPlan] = new HashMap<String, HashMap<Integer, List<Integer>>>();
            storeToPartitionsToDelete[numPlan] = new HashMap<String, HashMap<Integer, List<Integer>>>();
        }

        for(StoreDefinition storeDef: storeDefList) {
            if(storeDef.getReplicationFactor() == 2) {

                // All moves
                HashMap<Integer, List<Integer>> partitions = Maps.newHashMap();
                partitions.put(0, Lists.newArrayList(1));
                storeToPartitionsToMove[0].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(0, Lists.newArrayList(5));
                partitions.put(1, Lists.newArrayList(0));
                storeToPartitionsToMove[1].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(4));
                storeToPartitionsToMove[2].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(1));
                storeToPartitionsToMove[3].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(5));
                storeToPartitionsToMove[4].put(storeDef.getName(), partitions);

                // All deletes
                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(0));
                storeToPartitionsToDelete[1].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(4));
                storeToPartitionsToDelete[2].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(1));
                storeToPartitionsToDelete[3].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(1, Lists.newArrayList(5));
                storeToPartitionsToDelete[4].put(storeDef.getName(), partitions);
            } else if(storeDef.getReplicationFactor() == 1) {

                // All moves
                HashMap<Integer, List<Integer>> partitions = Maps.newHashMap();
                partitions.put(0, Lists.newArrayList(1));
                storeToPartitionsToMove[0].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(0, Lists.newArrayList(5));
                storeToPartitionsToMove[1].put(storeDef.getName(), partitions);

                // All deletes
                partitions = Maps.newHashMap();
                partitions.put(0, Lists.newArrayList(1));
                storeToPartitionsToDelete[0].put(storeDef.getName(), partitions);

                partitions = Maps.newHashMap();
                partitions.put(0, Lists.newArrayList(5));
                storeToPartitionsToDelete[1].put(storeDef.getName(), partitions);

            } else {
                throw new VoldemortException("Change in store definitions file found");
            }
        }

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               0,
                                                                               storeToPartitionsToMove[0],
                                                                               storeToPartitionsToDelete[0],
                                                                               currentCluster,
                                                                               0),
                                                   new RebalancePartitionsInfo(3,
                                                                               1,
                                                                               storeToPartitionsToMove[1],
                                                                               storeToPartitionsToDelete[1],
                                                                               currentCluster,
                                                                               0),
                                                   new RebalancePartitionsInfo(3,
                                                                               2,
                                                                               storeToPartitionsToMove[2],
                                                                               storeToPartitionsToDelete[2],
                                                                               currentCluster,
                                                                               0)));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(1),
                                     Arrays.asList(new RebalancePartitionsInfo(0,
                                                                               1,
                                                                               storeToPartitionsToMove[3],
                                                                               storeToPartitionsToDelete[3],
                                                                               currentCluster,
                                                                               0)));
        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(2),
                                     Arrays.asList(new RebalancePartitionsInfo(1,
                                                                               2,
                                                                               storeToPartitionsToMove[4],
                                                                               storeToPartitionsToDelete[4],
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

        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove1 = Maps.newHashMap();
        storeToPartitionsToMove1.put("test", partitionsToMove1);
        HashMap<String, HashMap<Integer, List<Integer>>> storeToPartitionsToMove2 = Maps.newHashMap();
        storeToPartitionsToMove2.put("test", partitionsToMove2);

        checkAllRebalanceInfoPresent(orderedRebalanceNodePlanList.get(0),
                                     Arrays.asList(new RebalancePartitionsInfo(3,
                                                                               0,
                                                                               storeToPartitionsToMove1,
                                                                               storeToPartitionsToMove1,
                                                                               currentCluster,
                                                                               0),
                                                   new RebalancePartitionsInfo(3,
                                                                               1,
                                                                               storeToPartitionsToMove2,
                                                                               storeToPartitionsToMove2,
                                                                               currentCluster,
                                                                               0)));
    }

    private void checkAllRebalanceInfoPresent(RebalanceNodePlan nodePlan,
                                              List<RebalancePartitionsInfo> rebalanceInfoList) {
        for(RebalancePartitionsInfo rebalanceInfo: rebalanceInfoList) {
            boolean match = false;
            for(RebalancePartitionsInfo nodeRebalanceInfo: nodePlan.getRebalanceTaskList()) {
                if(rebalanceInfo.getDonorId() == nodeRebalanceInfo.getDonorId()) {
                    assertEquals("Store lists should match",
                                 rebalanceInfo.getUnbalancedStoreList(),
                                 nodeRebalanceInfo.getUnbalancedStoreList());

                    assertEquals("Clusters to be same",
                                 rebalanceInfo.getInitialCluster(),
                                 nodeRebalanceInfo.getInitialCluster());

                    for(String storeName: rebalanceInfo.getUnbalancedStoreList()) {
                        assertEquals("add partition mapping for store " + storeName
                                             + " should be same ",
                                     rebalanceInfo.getReplicaToAddPartitionList(storeName),
                                     nodeRebalanceInfo.getReplicaToAddPartitionList(storeName));
                        assertEquals("delete partition mapping for store " + storeName
                                             + " should be same ",
                                     rebalanceInfo.getReplicaToDeletePartitionList(storeName),
                                     nodeRebalanceInfo.getReplicaToDeletePartitionList(storeName));
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