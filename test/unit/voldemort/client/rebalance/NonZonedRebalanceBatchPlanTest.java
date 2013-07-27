/*
 * Copyright 2008-2013 LinkedIn, Inc
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tests rebalance batch plan for non-zoned cluster. These tests existed before
 * the RebalancePlan was re-written in April/May 2013. That is why these tests
 * follow a different format than those in ZonedRebalanceBatchPlanTest.
 */
public class NonZonedRebalanceBatchPlanTest {

    private static String storeDefFile = "test/common/voldemort/config/stores.xml";
    private Cluster currentCluster;
    private Cluster finalCluster;

    private List<StoreDefinition> storeDefList;
    private List<StoreDefinition> storeDefList2;
    private List<StoreDefinition> test211StoreDef;

    @Before
    public void setUp() {
        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
            storeDefList2 = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStore322Xml()));
            test211StoreDef = Lists.newArrayList(ServerTestUtils.getStoreDef("test",
                                                                             2,
                                                                             1,
                                                                             1,
                                                                             1,
                                                                             1,
                                                                             RoutingStrategyType.CONSISTENT_STRATEGY));
        } catch(FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    /**
     * Tests the scenario where-in a migration causes a drop in the number of
     * replicas
     */
    @Test
    public void testInsufficientNodes() {
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0 }, { 1 }, { 2 } });

        finalCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 1 }, { 0 }, { 2 } });

        try {
            new RebalanceBatchPlan(currentCluster, finalCluster, storeDefList);
            fail("Should have thrown an exception since the migration should result in decrease in replication factor");
        } catch(VoldemortException e) {}

    }

    /**
     * confirm that a shuffle of a cluster of size 2 for a 211 store is a no op.
     */
    @Test
    public void testShuffleNoop() {
        int numServers = 2;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);
        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 1, 2, 3 }, { 4, 5, 6, 7, 0 } });

        List<RebalanceTaskInfo> batchPlan = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                          finalCluster,
                                                                          test211StoreDef);

        assertTrue("Batch plan should be empty.", batchPlan.isEmpty());
    }

    /**
     * Expand on to an empty server.
     */
    @Test
    public void testClusterExpansion() {
        int numServers = 3;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);

        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, {} });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 1, 2, 3 }, { 4, 5, 6, 7 }, { 0 } });

        List<RebalanceTaskInfo> batchPlan = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                          finalCluster,
                                                                          test211StoreDef);
        // data should only move from node 0 to node 2 for node 2 to host
        // everything needed. no other movement should occur.
        assertEquals("There should be one move in this plan.", 1, batchPlan.size());
        assertEquals("There should be exactly 1 rebalancing nodes",
                     1,
                     getUniqueNodeCount(batchPlan, false));
        assertEquals("Stealer 2 should have 1 entry",
                     1,
                     getStealerNodePartitionTaskCount(2, batchPlan));

        // Partitions to move
        
        HashMap<String, List<Integer>> storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", Lists.newArrayList(0, 4, 5, 6, 7));

        checkAllRebalanceInfoPresent(getStealerNodePartitionTaskList(2, batchPlan),
                                     Arrays.asList(new RebalanceTaskInfo(2,
                                                                         0,
                                                                         storeToPartitionsToMove,
                                                                         currentCluster)));
    }

    /**
     * Tests the case where-in we delete all the partitions from the last node
     */
    @Test
    public void testDeleteLastNode() {
        int numServers = 4;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);

        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 3, 6, 9, 12, 15 }, { 1, 4, 7, 10, 13, 16 }, { 2, 5, 8, 11, 14, 17 }, { 0 } });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 0, 3, 6, 9, 12, 15 }, { 1, 4, 7, 10, 13, 16 }, { 2, 5, 8, 11, 14, 17 }, {} });

        List<RebalanceTaskInfo> orderedRebalanceTaskInfoList = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                                              finalCluster,
                                                                                              storeDefList2);
        assertEquals("There should have exactly 1 rebalancing node",
                     1,
                     getUniqueNodeCount(orderedRebalanceTaskInfoList, false));
        assertEquals("There should be exactly 1 rebalancing partition info",
                     1,
                     orderedRebalanceTaskInfoList.size());

        assertEquals("Stealer 0 should have 1 entry",
                     1,
                     getStealerNodePartitionTaskCount(0, orderedRebalanceTaskInfoList));

        HashMap<String, List<Integer>> storeToPartitionsToMove = Maps.newHashMap();
        storeToPartitionsToMove.put("test", Lists.newArrayList(0, 16, 17));

        checkAllRebalanceInfoPresent(getStealerNodePartitionTaskList(0,
                                                                     orderedRebalanceTaskInfoList),
                                     Arrays.asList(new RebalanceTaskInfo(0,
                                                                         3,
                                                                         storeToPartitionsToMove,
                                                                         currentCluster)));
    }

    /**
     * Tests the scenario where-in we delete the first node
     */
    @Test
    public void testDeleteFirstNode() {
        int numServers = 4;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);

        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 0, 4 },
                { 1, 5 }, { 2, 6 }, { 3, 7 } });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 4 },
                { 0, 1, 5 }, { 2, 6 }, { 3, 7 } });

        // PHASE 1 - move partition 0 off of node 0 to node 1
        List<RebalanceTaskInfo> batchPlan = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                          finalCluster,
                                                                          storeDefList2);

        assertFalse("Batch plan should not be empty.", batchPlan.isEmpty());

        // Cannot do other tests because with partition 1 already on node 1, its
        // unclear which partitions will actual move.

        // PHASE 2 - Move partition 4 off of node 0 to node 2
        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 4 },
                { 0, 1, 5 }, { 2 }, { 3, 6, 7 } });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { {},
                { 0, 1, 5 }, { 4, 2 }, { 3, 6, 7 } });

        batchPlan = ClusterTestUtils.getBatchPlan(currentCluster, finalCluster, storeDefList2);

        assertFalse("Batch plan should not be empty.", batchPlan.isEmpty());
        assertFalse("Batch plan for server 2 should not be empty.",
                    getStealerNodePartitionTaskList(2, batchPlan).isEmpty());
        boolean hasTheMove = false;
        // Confirm partition 4 is moved from server 0 to server 2
        for(RebalanceTaskInfo info: getStealerNodePartitionTaskList(2, batchPlan)) {
            assertTrue(info.getStealerId() == 2);
            if(info.getDonorId() == 0) {
                hasTheMove = true;
                assertTrue(info.getPartitionStores().size() == 1);
                assertTrue(info.getPartitionIds("test").contains(4));
            }
        }
        assertTrue(hasTheMove);
    }

    @Test
    public void testRebalanceDeletingMiddleNode() {
        int numServers = 4;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);

        // PHASE 1 - move partition 2 off of node 2 and onto node 1
        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 0, 4 },
                { 1, 5 }, { 2, 6 }, { 3, 7 } });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 0, 4 },
                { 2, 1, 5 }, { 6 }, { 3, 7 } });

        List<RebalanceTaskInfo> batchPlan = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                          finalCluster,
                                                                          storeDefList2);

        assertFalse("Batch plan should not be empty.", batchPlan.isEmpty());
        assertFalse("Batch plan for server 1 should not be empty.",
                    getStealerNodePartitionTaskList(1, batchPlan).isEmpty());
        boolean hasTheMove = false;
        // Confirm partition 2 is moved from server 2 to server 1
        for(RebalanceTaskInfo info: getStealerNodePartitionTaskList(1, batchPlan)) {
            assertTrue(info.getStealerId() == 1);
            if(info.getDonorId() == 2) {
                hasTheMove = true;
                assertTrue(info.getPartitionStores().size() == 1);
                assertTrue(info.getPartitionIds("test").contains(2));
            }
        }
        assertTrue(hasTheMove);

        // PHASE 2 - move partition 6 off of node 2 and onto node 3
        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 0, 4 },
                { 2, 1, 5 }, { 6 }, { 3, 7 } });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 0, 4 },
                { 2, 1, 5 }, {}, { 6, 3, 7 } });

        batchPlan = ClusterTestUtils.getBatchPlan(currentCluster, finalCluster, storeDefList2);

        assertFalse("Batch plan should not be empty.", batchPlan.isEmpty());

        // Cannot do other tests because with partition 7 already on node 3, its
        // unclear which partitions will actual move when partitoin 6 also moves
        // to node 3.
    }

    @Test
    public void testManyStoreClusterExpansion() {
        int numServers = 4;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);

        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 0, 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 }, {} });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] {
                { 0, 2, 3 }, { 4, 6 }, { 7, 8, 9 }, { 1, 5 } });

        List<RebalanceTaskInfo> batchPlan = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                          finalCluster,
                                                                          storeDefList);

        assertFalse("Batch plan should not be empty.", batchPlan.isEmpty());
        assertFalse("Batch plan for server 3 should not be empty.",
                getStealerNodePartitionTaskList(3, batchPlan).isEmpty());

        boolean hasTheMove = false;
        // Confirm partition 1 is moved from server 0 to server 3
        for(RebalanceTaskInfo info: getStealerNodePartitionTaskList(3, batchPlan)) {
            assertTrue(info.getStealerId() == 3);
            if(info.getDonorId() == 0) {
                hasTheMove = true;
                for(String storeName: info.getPartitionStores()) {
                    assertTrue(info.getPartitionIds(storeName).contains(1));
                }
            }
        }
        assertTrue(hasTheMove);

        hasTheMove = false;
        // Confirm partition 5 is moved from server 1 to server 3
        for(RebalanceTaskInfo info: getStealerNodePartitionTaskList(3, batchPlan)) {
            assertTrue(info.getStealerId() == 3);
            if(info.getDonorId() == 1) {
                hasTheMove = true;
                for(String storeName: info.getPartitionStores()) {
                    assertTrue(info.getPartitionIds(storeName).contains(5));
                }
            }
        }
        assertTrue(hasTheMove);
    }

    /**
     * Issue 288
     */
    @Test
    public void testRebalanceAllReplicasBeingMigrated() {
        int numServers = 4;
        int ports[] = ServerTestUtils.findFreePorts(3 * numServers);

        currentCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 0, 4 },
                { 2, 3 }, { 1, 5 }, {} });

        finalCluster = ServerTestUtils.getLocalCluster(numServers, ports, new int[][] { { 4 },
                { 2, 3 }, { 1, 5 }, { 0 } });

        List<RebalanceTaskInfo> orderedRebalanceTaskInfoList = ClusterTestUtils.getBatchPlan(currentCluster,
                                                                                                  finalCluster,
                                                                                                  storeDefList2);

        assertEquals("There should have exactly 1 rebalancing node",
                     1,
                     this.getUniqueNodeCount(orderedRebalanceTaskInfoList, false));
        assertEquals("There should have exactly 2 rebalancing partition info",
                     2,
                     orderedRebalanceTaskInfoList.size());
        assertEquals("Stealer 3 should have 2 entry",
                     2,
                     this.getStealerNodePartitionTaskCount(3, orderedRebalanceTaskInfoList));

        HashMap<String, List<Integer>> storeToPartitionsToMove1 = Maps.newHashMap();
        storeToPartitionsToMove1.put("test", Lists.newArrayList(0, 5));
        HashMap<String, List<Integer>> storeToPartitionsToMove2 = Maps.newHashMap();
        storeToPartitionsToMove2.put("test", Lists.newArrayList(4));

        checkAllRebalanceInfoPresent(this.getStealerNodePartitionTaskList(3,
                                                                          orderedRebalanceTaskInfoList),
                                     Arrays.asList(new RebalanceTaskInfo(3,
                                                                         0,
                                                                         storeToPartitionsToMove1,
                                                                         currentCluster),
                                                   new RebalanceTaskInfo(3,
                                                                         1,
                                                                         storeToPartitionsToMove2,
                                                                         currentCluster)));
    }

    private int getUniqueNodeCount(List<RebalanceTaskInfo> rebalanceInfoList,
                                   boolean isDonorBased) {
        HashSet<Integer> uniqueNodeSet = Sets.newHashSet();
        for(RebalanceTaskInfo partitionInfo: rebalanceInfoList) {
            int nodeId;
            if(isDonorBased) {
                nodeId = partitionInfo.getDonorId();
            } else {
                nodeId = partitionInfo.getStealerId();
            }
            if(!uniqueNodeSet.contains(nodeId)) {
                uniqueNodeSet.add(nodeId);
            }
        }
        return uniqueNodeSet.size();
    }

    private int getStealerNodePartitionTaskCount(int stealerId,
                                                 List<RebalanceTaskInfo> rebalanceInfoList) {
        int count = 0;

        for(RebalanceTaskInfo partitionInfo: rebalanceInfoList) {
            if(partitionInfo.getStealerId() == stealerId) {
                count++;
            }
        }
        return count;
    }

    private List<RebalanceTaskInfo> getStealerNodePartitionTaskList(int stealerId,
                                                                    List<RebalanceTaskInfo> rebalanceTaskList) {
        ArrayList<RebalanceTaskInfo> partitionList = Lists.newArrayList();

        for(RebalanceTaskInfo partitionInfo: rebalanceTaskList) {
            if(partitionInfo.getStealerId() == stealerId) {
                partitionList.add(partitionInfo);
            }
        }
        return partitionList;
    }

    private void checkAllRebalanceInfoPresent(List<RebalanceTaskInfo> toCheckRebalanceTaskInfoList,
                                              List<RebalanceTaskInfo> rebalanceTaskInfoList) {
        for(RebalanceTaskInfo rebalanceInfo: rebalanceTaskInfoList) {
            boolean match = false;
            for(RebalanceTaskInfo nodeRebalanceTaskInfo: toCheckRebalanceTaskInfoList) {
                if(rebalanceInfo.getDonorId() == nodeRebalanceTaskInfo.getDonorId()) {
                    assertEquals("Store lists should match",
                                 rebalanceInfo.getPartitionStores(),
                                 nodeRebalanceTaskInfo.getPartitionStores());

                    assertEquals("Clusters to be same",
                                 rebalanceInfo.getInitialCluster(),
                                 nodeRebalanceTaskInfo.getInitialCluster());

                    for(String storeName: rebalanceInfo.getPartitionStores()) {
                        assertEquals("add partition mapping for store " + storeName
                                     + " should be same ",
                                     rebalanceInfo.getPartitionIds(storeName),
                                     nodeRebalanceTaskInfo.getPartitionIds(storeName));
                    }
                    match = true;
                }
            }

            assertNotSame("rebalancePartition Info " + rebalanceInfo
                          + " should be present in the nodePlan "
                          + toCheckRebalanceTaskInfoList,
                          false,
                          match);
        }
    }
}
