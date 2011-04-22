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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalanceController;
import voldemort.client.rebalance.RebalanceNodePlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.client.rebalance.RebalanceController.OrderedClusterTransition;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalanceUtilsTest extends TestCase {

    private static String storeDefFile = "test/common/voldemort/config/stores.xml";
    private static final ArrayList<Integer> empty = new ArrayList<Integer>();
    private Cluster currentCluster;
    private Cluster targetCluster;
    List<StoreDefinition> storeDefList;

    @Override
    public void setUp() {

        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
        } catch(FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    @Test
    public void testRebalancePartitionsInfoCreate() {
        Map<Integer, List<Integer>> partitionsMap = Maps.newHashMap();
        partitionsMap.put(0, Lists.newArrayList(1, 2, 3));
        partitionsMap.put(1, Lists.newArrayList(4, 5, 6));

        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   Lists.newArrayList(1),
                                                                   Lists.newArrayList(7),
                                                                   Lists.newArrayList(3),
                                                                   Lists.newArrayList("test"),
                                                                   0);
        String jsonString = info.toJsonString();
        RebalancePartitionsInfo info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

    }

    public void testRebalancePlan() {
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });

        System.out.println("testRebalancePlan() running...");
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true);
        // The store has a replication factor = 2
        // Current Cluster:
        // 0 - [0, 1, 2, 3, 4, 5, 6, 7, 8] + []
        // 1 - [] + []
        //
        // Target Cluster:
        // 0 - [0, 1, 4, 5, 6, 7, 8] + [2, 3]
        // 1 - [2, 3] + [0, 1, 4, 5, 6, 7, 8]
        //
        //
        // We have to move primaries but also replicas without deleting 2 and 3
        // otherwise you will lease the cluster unbalance based on replication
        // factor.
        System.out.println("Plan partition distribution: "
                           + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

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
                                                                           Arrays.asList(0,
                                                                                         1,
                                                                                         2,
                                                                                         3,
                                                                                         4,
                                                                                         5,
                                                                                         6,
                                                                                         7,
                                                                                         8),
                                                                           empty,
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

    public void testClusterTransitionDeletingFirstNode() {
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        System.out.println("testClusterTransitionDeletingFirstNode() - moving Primary 0 - running...");
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 4 }, { 0, 1, 5 },
                { 2, 6 }, { 3, 7 } });

        clusterTransitionDeletingFirstNodeParition0(currentCluster, targetCluster, storeDef);

        System.out.println("testRebalanceDeletingFirstNodeFirstStepByStepTransition() - moving Primary 4 - running...");
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 4 }, { 0, 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { {}, { 0, 1, 5 },
                { 4, 2, 6 }, { 3, 7 } });
        clusterTransitionDeletingFirstNodeParition4(currentCluster, targetCluster, storeDef);

    }

    private void clusterTransitionDeletingFirstNodeParition0(Cluster currentCluster,
                                                             Cluster targetCluster,
                                                             List<StoreDefinition> storeDef) {
        OrderedClusterTransition orderedClusterTransition = createOrderedClusterTransition(currentCluster,
                                                                                           targetCluster,
                                                                                           storeDef);

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        // Current Cluster:
        // 0 - [0, 4] + [2, 3, 6, 7]
        // 1 - [1, 5] + [0, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 4, 5]
        // 3 - [3, 7] + [1, 2, 5, 6]
        //
        // Target Cluster:
        // 0 - [4] + [2, 3]
        // 1 - [0, 1, 5] + [3, 4, 6, 7]
        // 2 - [2, 6] + [0, 1, 4, 5, 7]
        // 3 - [3, 7] + [0, 1, 2, 5, 6]
        //
        // RebalancingStealInfo(1 <--- 0 partitions:[0, 6] steal master
        // partitions:[0] deleted:[6] stores:[test])
        // RebalancingStealInfo(2 <--- 0 partitions:[7] steal master
        // partitions:[] deleted:[7] stores:[test])
        // RebalancingStealInfo(3 <--- 0 partitions:[0] steal master
        // partitions:[] deleted:[0] stores:[test])
        // 
        // The only possible donor for partition 0 is Node0, any other Donor
        // will have the side effect that if failed will leave the cluster
        // without 2
        // replicas. See partition "0".

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(0);
            assertEquals("Stealer 1 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                    0,
                                                                    Arrays.asList(0, 6),
                                                                    Arrays.asList(6),
                                                                    Arrays.asList(0),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }
        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(1);
            assertEquals("Stealer 2 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    0,
                                                                    Arrays.asList(7),
                                                                    Arrays.asList(7),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(2);
            assertEquals("Stealer 3 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    0,
                                                                    Arrays.asList(0),
                                                                    Arrays.asList(0),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

    }

    private void clusterTransitionDeletingFirstNodeParition4(Cluster currentCluster,
                                                             Cluster targetCluster,
                                                             List<StoreDefinition> storeDef) {
        final OrderedClusterTransition orderedClusterTransition = createOrderedClusterTransition(currentCluster,
                                                                                                 targetCluster,
                                                                                                 storeDef);

        orderedClusterTransition.getOrderedRebalanceNodePlanList();
        List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();

        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        // Current Cluster:
        // 0 - [4] + [2, 3]
        // 1 - [0, 1, 5] + [3, 4, 6, 7]
        // 2 - [2, 6] + [0, 1, 4, 5, 7]
        // 3 - [3, 7] + [0, 1, 2, 5, 6]
        //
        // Target Cluster:
        // 0 - [] + [] -2, 3, 4
        // 1 - [0, 1, 5] + [2, 3, 4, 6, 7] +2,
        // 2 - [2, 4, 6] + [0, 1, 3, 5, 7] +"4", 3,
        // 3 - [3, 7] + [0, 1, 2, 4, 5, 6] +4
        //
        // RebalancingStealInfo(2 <--- 0 partitions:[3, 4] steal master
        // partitions:[4] deleted:[3] stores:[test])
        // RebalancingStealInfo(1 <--- 0 partitions:[2] steal master
        // partitions:[] deleted:[2] stores:[test])
        // RebalancingStealInfo(3 <--- 0 partitions:[4] steal master
        // partitions:[] deleted:[4] stores:[test])
        {
            RebalanceNodePlan nodePlan = orderedRebalanceNodePlanList.get(0);
            assertEquals("Stealer 2 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    0,
                                                                    Arrays.asList(3, 4),
                                                                    Arrays.asList(3),
                                                                    Arrays.asList(4),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = orderedRebalanceNodePlanList.get(1);
            assertEquals("Stealer 1 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                    0,
                                                                    Arrays.asList(2),
                                                                    Arrays.asList(2),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = orderedRebalanceNodePlanList.get(2);
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    0,
                                                                    Arrays.asList(4),
                                                                    Arrays.asList(4),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

    }

    public void testClusterTransitionDeletingLastNode() {
        System.out.println("testClusterTransitionDeletingLastNode() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 3, 0, 4 }, { 1, 5 },
                { 2, 6 }, { 7 } });
        clusterTransitionDeletingLastNodePartition3(currentCluster, targetCluster, storeDef);

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 3, 0, 4 }, { 1, 5 },
                { 2, 6 }, { 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 3, 0, 4 }, { 7, 1, 5 },
                { 2, 6 }, {} });
        clusterTransitionDeletingLastNodePartition7(currentCluster, targetCluster, storeDef);
    }

    private void clusterTransitionDeletingLastNodePartition3(Cluster currentCluster,
                                                             Cluster targetCluster,
                                                             List<StoreDefinition> storeDef) {
        OrderedClusterTransition orderedClusterTransition = createOrderedClusterTransition(currentCluster,
                                                                                           targetCluster,
                                                                                           storeDef);

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        // Current Cluster:
        // 0 - [0, 4] + [2, 3, 6, 7]
        // 1 - [1, 5] + [0, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 4, 5]
        // 3 - [3, 7] + [1, 2, 5, 6]
        //
        // Target Cluster:
        // 0 - [0, 3, 4] + [1, 2, 6, 7] +"3", 1
        // 1 - [1, 5] + [0, 2, 3, 4, 7] +2,
        // 2 - [2, 6] + [0, 1, 3, 4, 5] +3
        // 3 - [7] + [5, 6] -1,2,3
        //
        // RebalancingStealInfo(0 <--- 3 partitions:[1, 3] steal master
        // partitions:[3] deleted:[1] stores:[test])
        // RebalancingStealInfo(1 <--- 3 partitions:[2] steal master
        // partitions:[] deleted:[2] stores:[test])
        // RebalancingStealInfo(2 <--- 3 partitions:[3] steal master
        // partitions:[] deleted:[3] stores:[test])

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(0);
            assertEquals("Stealer 0 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                    3,
                                                                    Arrays.asList(1, 3),
                                                                    Arrays.asList(1),
                                                                    Arrays.asList(3),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(1);
            assertEquals("Stealer 1 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                    3,
                                                                    Arrays.asList(2),
                                                                    Arrays.asList(2),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(2);
            assertEquals("Stealer 2 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    3,
                                                                    Arrays.asList(3),
                                                                    Arrays.asList(3),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

    }

    private void clusterTransitionDeletingLastNodePartition7(Cluster currentCluster,
                                                             Cluster targetCluster,
                                                             List<StoreDefinition> storeDef) {
        OrderedClusterTransition orderedClusterTransition = createOrderedClusterTransition(currentCluster,
                                                                                           targetCluster,
                                                                                           storeDef);

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     orderedRebalanceNodePlanList.size());

        // Current Cluster:
        // 0 - [0, 3, 4] + [1, 2, 6, 7]
        // 1 - [1, 5] + [0, 2, 3, 4, 7]
        // 2 - [2, 6] + [0, 1, 3, 4, 5]
        // 3 - [7] + [5, 6]
        //
        // Target Cluster:
        // 0 - [0, 3, 4] + [1, 2, 5, 6, 7]
        // 1 - [1, 5, 7] + [0, 2, 3, 4, 6]
        // 2 - [2, 6] + [0, 1, 3, 4, 5, 7]
        // 3 - [] + []
        //
        // RebalancingStealInfo(1 <--- 3 partitions:[6, 7] steal master
        // partitions:[7] deleted:[6] stores:[test])
        // RebalancingStealInfo(0 <--- 3 partitions:[5] steal master
        // partitions:[] deleted:[5] stores:[test])
        // RebalancingStealInfo(2 <--- 3 partitions:[7] steal master
        // partitions:[] deleted:[7] stores:[test])
        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(0);
            assertEquals("Stealer 1 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                    3,
                                                                    Arrays.asList(6, 7),
                                                                    Arrays.asList(6),
                                                                    Arrays.asList(7),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(1);
            assertEquals("Stealer 0 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                    3,
                                                                    Arrays.asList(5),
                                                                    Arrays.asList(5),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(2);
            assertEquals("Stealer 2 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    3,
                                                                    Arrays.asList(7),
                                                                    Arrays.asList(7),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }

    }

    public void testRebalanceDeletingMidleNode() {
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
        System.out.println("testRebalanceDeletingMidleNode() running...");
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 2, 1, 5 }, {},
                { 6, 3, 7 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDef,
                                                                      true);
        System.out.println("Plan partition distribution: "
                           + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                    2,
                                                                    Arrays.asList(1, 5),
                                                                    Arrays.asList(1, 5),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 1 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                    2,
                                                                    Arrays.asList(2, 6),
                                                                    Arrays.asList(2),
                                                                    Arrays.asList(2),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    2,
                                                                    Arrays.asList(0, 4, 6),
                                                                    Arrays.asList(0, 4, 6),
                                                                    Arrays.asList(6),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
    }

    public void testRebalancePlanWithReplicationChanges() {
        System.out.println("testRebalancePlanWithReplicationChanges() running...");
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6 }, { 7, 8, 9 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 3 }, { 4, 6 },
                { 7, 8, 9 }, { 1, 5 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDefList,
                                                                      true);
        System.out.println("Plan partition distribution: "
                           + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // - Current Cluster:
        // 0 - [0, 1, 2, 3] + [7, 8, 9]
        // 1 - [4, 5, 6] + [0, 1, 2, 3]
        // 2 - [7, 8, 9] + [4, 5, 6]
        //
        // - Target Cluster:
        // 0 - [0, 2, 3] + [1, 7, 8, 9]
        // 1 - [4, 6] + [2, 3, 5]
        // 2 - [7, 8, 9] + [6]
        // 3 - [1, 5] + [0, 4]
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();

            assertEquals("Rebalancing node 1 should have 1 entries",
                         1,
                         nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(1,
                                                                    2,
                                                                    empty,
                                                                    Arrays.asList(4),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDefList),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Rebalancing node 2 should have 1 entries",
                         1,
                         nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    1,
                                                                    empty,
                                                                    Arrays.asList(0),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDefList),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Rebalancing node 3 should have 2 entries",
                         2,
                         nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    0,
                                                                    Arrays.asList(1),
                                                                    empty,
                                                                    Arrays.asList(1),
                                                                    RebalanceUtils.getStoreNames(storeDefList),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            RebalancePartitionsInfo b = new RebalancePartitionsInfo(3,
                                                                    1,
                                                                    Arrays.asList(5),
                                                                    Arrays.asList(1),
                                                                    Arrays.asList(5),
                                                                    RebalanceUtils.getStoreNames(storeDefList),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a, b));
        }

    }

    /**
     * Issue 288
     * 
     */
    public void testRebalanceAllReplicasBeingMigrated() {
        System.out.println("testRebalanceAllReplicasBeingMigrated() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 2, 3 },
                { 1, 5 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 4 }, { 2, 3 }, { 1, 5 },
                { 0 } });

        OrderedClusterTransition orderedClusterTransition = createOrderedClusterTransition(currentCluster,
                                                                                           targetCluster,
                                                                                           storeDef);

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();
        assertEquals("There should have exactly 1 rebalancing node",
                     1,
                     orderedRebalanceNodePlanList.size());

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(0);
            assertEquals("Stealer 3 should have 2 entry",
                         2,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    0,
                                                                    Arrays.asList(0, 5),
                                                                    Arrays.asList(0, 5),
                                                                    Arrays.asList(0),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            RebalancePartitionsInfo b = new RebalancePartitionsInfo(3,
                                                                    1,
                                                                    Arrays.asList(4),
                                                                    Arrays.asList(4),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a, b));
        }
    }

    public void testRebalanceDeletingNode() {
        System.out.println("testRebalanceDeletingNode() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 1, 4 }, {},
                { 2, 5, 6 }, { 3, 7 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDef,
                                                                      true);
        System.out.println("Plan partition distribution: "
                           + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                    1,
                                                                    Arrays.asList(1, 5),
                                                                    Arrays.asList(1),
                                                                    Arrays.asList(1),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 2 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    1,
                                                                    Arrays.asList(3, 5, 7),
                                                                    Arrays.asList(3, 5, 7),
                                                                    Arrays.asList(5),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    1,
                                                                    Arrays.asList(0, 4),
                                                                    Arrays.asList(0, 4),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);

            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
    }

    public void testRebalanceDeletingOnePartition() {
        System.out.println("testRebalanceDeletingOnePartition() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4, 8 }, { 1, 5, 9 },
                { 2, 6, 10 }, { 3, 7, 11 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 1, 4, 8 }, { 9 },
                { 2, 5, 6, 10 }, { 3, 7, 11 } });

        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDef,
                                                                      true);
        System.out.println("Plan partition distribution: "
                           + rebalancePlan.printPartitionDistribution());
        System.out.println("Plan: " + rebalancePlan);

        // Replication factor = 3
        //
        // - Current Cluster:
        // 0 - [0, 4, 8] + [2, 3, 6, 7, 10, 11]
        // 1 - [1, 5, 9] + [0, 3, 4, 7, 8, 11]
        // 2 - [2, 6, 10] + [0, 1, 4, 5, 8, 9]
        // 3 - [3, 7, 11] + [1, 2, 5, 6, 9, 10]
        //
        // - Target Cluster:
        // 0 - [0, 1, 4, 8] + [2, 3, 5, 6, 7, 10, 11]
        // 1 - [9] + [7, 8]
        // 2 - [2, 5, 6, 10] + [0, 1, 3, 4, 8, 9, 11]
        // 3 - [3, 7, 11] + [0, 1, 2, 4, 5, 6, 9, 10]
        //
        assertEquals("There should have exactly 3 rebalancing node",
                     3,
                     rebalancePlan.getRebalancingTaskQueue().size());

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 0 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                    1,
                                                                    Arrays.asList(1, 5),
                                                                    Arrays.asList(1),
                                                                    Arrays.asList(1),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 2 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(2,
                                                                    1,
                                                                    Arrays.asList(3, 5, 11),
                                                                    Arrays.asList(3, 5, 11),
                                                                    Arrays.asList(5),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }

        {
            RebalanceNodePlan nodePlan = rebalancePlan.getRebalancingTaskQueue().poll();
            assertEquals("Stealer 3 should have 1 entry", 1, nodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(3,
                                                                    1,
                                                                    Arrays.asList(0, 4),
                                                                    Arrays.asList(0, 4),
                                                                    empty,
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(nodePlan, Arrays.asList(a));
        }
    }

    private void checkAllRebalanceInfoPresent(RebalanceNodePlan nodePlan,
                                              List<RebalancePartitionsInfo> rebalanceInfoList) {
        for(RebalancePartitionsInfo rebalanceInfo: rebalanceInfoList) {
            boolean match = false;
            for(RebalancePartitionsInfo nodeRebalanceInfo: nodePlan.getRebalanceTaskList()) {
                if(rebalanceInfo.getDonorId() == nodeRebalanceInfo.getDonorId()) {
                    assertEquals("partitions should match",
                                 true,
                                 Utils.compareList(rebalanceInfo.getPartitionList(),
                                                   nodeRebalanceInfo.getPartitionList()));

                    assertEquals("delete partitions should match",
                                 true,
                                 Utils.compareList(rebalanceInfo.getDeletePartitionsList(),
                                                   nodeRebalanceInfo.getDeletePartitionsList()));

                    assertEquals("store list should match",
                                 true,
                                 Utils.compareList(rebalanceInfo.getUnbalancedStoreList(),
                                                   nodeRebalanceInfo.getUnbalancedStoreList()));

                    assertEquals("steal master partitions should match",
                                 true,
                                 Utils.compareList(rebalanceInfo.getStealMasterPartitions(),
                                                   nodeRebalanceInfo.getStealMasterPartitions()));
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
        System.out.println("testUpdateCluster() running...");
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    public void testRebalanceStealInfo() {
        System.out.println("testRebalanceStealInfo() running...");
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

    public void testDeleteLastNode() {

        System.out.println("testDeleteLastNode() running...");
        List<StoreDefinition> storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));

        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 3, 6, 9, 12, 15 },
                { 1, 4, 7, 10, 13, 16 }, { 2, 5, 8, 11, 14, 17 }, { 0 } });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 3, 6, 9, 12, 15 },
                { 1, 4, 7, 10, 13, 16 }, { 2, 5, 8, 11, 14, 17 }, {} });

        // Current Cluster:
        // 0 - [3, 6, 9, 12, 15] + [1, 2, 4, 5, 7, 8, 10, 11, 13, 14]
        // 1 - [1, 4, 7, 10, 13, 16] + [0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17]
        // 2 - [2, 5, 8, 11, 14, 17] + [0, 1, 3, 4, 6, 7, 9, 10, 12, 13, 15, 16]
        // 3 - [0] + [16, 17]
        //
        // Target Cluster:
        // 0 - [0, 3, 6, 9, 12, 15] + [1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17]
        // 1 - [1, 4, 7, 10, 13, 16] + [0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17]
        // 2 - [2, 5, 8, 11, 14, 17] + [0, 1, 3, 4, 6, 7, 9, 10, 12, 13, 15, 16]
        // 3 - [] + []

        OrderedClusterTransition orderedClusterTransition = createOrderedClusterTransition(currentCluster,
                                                                                           targetCluster,
                                                                                           storeDef);

        List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();
        assertEquals("There should have exactly 1 rebalancing node",
                     1,
                     orderedRebalanceNodePlanList.size());

        {
            RebalanceNodePlan rebalanceNodePlan = orderedRebalanceNodePlanList.get(0);
            assertEquals("Stealer 3 should have 1 entry",
                         1,
                         rebalanceNodePlan.getRebalanceTaskList().size());
            RebalancePartitionsInfo a = new RebalancePartitionsInfo(0,
                                                                    3,
                                                                    Arrays.asList(0, 16, 17),
                                                                    Arrays.asList(0, 16, 17),
                                                                    Arrays.asList(0),
                                                                    RebalanceUtils.getStoreNames(storeDef),
                                                                    new HashMap<String, String>(),
                                                                    new HashMap<String, String>(),
                                                                    0);
            checkAllRebalanceInfoPresent(rebalanceNodePlan, Arrays.asList(a));
        }
    }

    private OrderedClusterTransition createOrderedClusterTransition(Cluster currentCluster,
                                                                    Cluster targetCluster,
                                                                    List<StoreDefinition> storeDef) {
        RebalanceClusterPlan rebalancePlan = new RebalanceClusterPlan(currentCluster,
                                                                      targetCluster,
                                                                      storeDef,
                                                                      true);
        final OrderedClusterTransition orderedClusterTransition = new RebalanceController.OrderedClusterTransition(currentCluster,
                                                                                                                   targetCluster,
                                                                                                                   rebalancePlan);
        System.out.println("orderedClusterTransition: " + orderedClusterTransition);
        return orderedClusterTransition;
    }

    public void testGetNodeIds() {
        List<Node> nodes = Lists.newArrayList();

        // Test with empty node list
        assertEquals(RebalanceUtils.getNodeIds(nodes).size(), 0);

        // Add one node
        nodes.add(new Node(0, "localhost", 1, 2, 3, new ArrayList<Integer>()));
        assertEquals(RebalanceUtils.getNodeIds(nodes).size(), 1);
        assertEquals(RebalanceUtils.getNodeIds(nodes).get(0).intValue(), 0);
    }
}