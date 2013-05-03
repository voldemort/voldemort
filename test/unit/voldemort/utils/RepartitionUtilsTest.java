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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;

public class RepartitionUtilsTest {

    /**
     * Nodes balanced == each server has same number (within 1) of partition Ids
     * within each zone.
     * 
     * @param cluster
     * @return
     */
    public boolean verifyNodesBalancedInEachZone(Cluster cluster) {
        for(Integer zoneId: cluster.getZoneIds()) {
            int maxPartitions = Integer.MIN_VALUE;
            int minPartitions = Integer.MAX_VALUE;
            for(Integer nodeId: cluster.getNodeIdsInZone(zoneId)) {
                int numPartitions = cluster.getNodeById(nodeId).getNumberOfPartitions();
                if(numPartitions > maxPartitions)
                    maxPartitions = numPartitions;
                if(numPartitions < minPartitions)
                    minPartitions = numPartitions;
            }
            int partitionsDiff = maxPartitions - minPartitions;
            if(partitionsDiff > 1 || partitionsDiff < 0)
                return false;
        }
        return true;
    }

    /**
     * Zones balanced == each zone has same number (within 1) of partition Ids
     * 
     * @param cluster
     * @return
     */
    public boolean verifyZonesBalanced(Cluster cluster) {
        int maxPartitions = Integer.MIN_VALUE;
        int minPartitions = Integer.MAX_VALUE;
        for(Integer zoneId: cluster.getZoneIds()) {
            int numPartitions = cluster.getNumberOfPartitionsInZone(zoneId);
            if(numPartitions > maxPartitions)
                maxPartitions = numPartitions;
            if(numPartitions < minPartitions)
                minPartitions = numPartitions;
        }
        int partitionsDiff = maxPartitions - minPartitions;
        if(partitionsDiff > 1 || partitionsDiff < 0) {
            return false;
        }
        return true;
    }

    /**
     * Verify balancing of partitions among zones, and within each zone, among
     * nodes.
     * 
     * @param currentCluster
     * @param currentStores
     * @param targetCluster
     * @param targetStores
     */
    public void verifyBalanceZoneAndNode(Cluster currentCluster,
                                         List<StoreDefinition> currentStores,
                                         Cluster targetCluster,
                                         List<StoreDefinition> targetStores) {
        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));

        // Repartition to balance partition IDs among zones and among nodes
        // within zone.
        boolean disableNodeBalancing = false;
        boolean disableZoneBalancing = false;
        Cluster repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                                    currentStores,
                                                                    targetCluster,
                                                                    targetStores,
                                                                    null,
                                                                    1,
                                                                    disableNodeBalancing,
                                                                    disableZoneBalancing,
                                                                    false,
                                                                    0,
                                                                    0,
                                                                    false,
                                                                    null,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0);
        assertTrue(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertTrue(verifyZonesBalanced(repartitionedCluster));
    }

    /**
     * Verify balancing of partitions among nodes without balancing partitions
     * among zones.
     * 
     * @param currentCluster
     * @param currentStores
     * @param targetCluster
     * @param targetStores
     */
    public void verifyBalanceNodesNotZones(Cluster currentCluster,
                                           List<StoreDefinition> currentStores,
                                           Cluster targetCluster,
                                           List<StoreDefinition> targetStores) {

        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));

        // Repartition to balance partition IDs among zones and among nodes
        // within zone.
        boolean disableNodeBalancing = false;
        boolean disableZoneBalancing = true;
        Cluster repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                                    currentStores,

                                                                    targetCluster,
                                                                    targetStores,
                                                                    null,
                                                                    1,
                                                                    disableNodeBalancing,
                                                                    disableZoneBalancing,
                                                                    false,
                                                                    0,
                                                                    0,
                                                                    false,
                                                                    null,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0);
        assertTrue(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));
    }

    /**
     * Verify the "no op" path through repartition method does not change the
     * target cluster.
     * 
     * @param currentCluster
     * @param currentStores
     * @param targetCluster
     * @param targetStores
     */
    public void verifyRepartitionNoop(Cluster currentCluster,
                                      List<StoreDefinition> currentStores,
                                      Cluster targetCluster,
                                      List<StoreDefinition> targetStores) {
        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));

        // Confirm noop rebalance has no effect on target cluster
        boolean disableNodeBalancing = true;
        boolean disableZoneBalancing = true;
        Cluster repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                                    currentStores,
                                                                    targetCluster,
                                                                    targetStores,
                                                                    null,
                                                                    1,
                                                                    disableNodeBalancing,
                                                                    disableZoneBalancing,
                                                                    false,
                                                                    0,
                                                                    0,
                                                                    false,
                                                                    null,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0);
        assertTrue(repartitionedCluster.equals(targetCluster));
    }

    /**
     * Verify that random swaps improve balance.
     * 
     * @param currentCluster
     * @param currentStores
     */
    public void verifyRandomSwapsImproveBalance(Cluster currentCluster,
                                                List<StoreDefinition> currentStores) {
        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));
        PartitionBalance currentPb = new PartitionBalance(currentCluster, currentStores);

        // Disable basic balancing among zones & nodes
        boolean disableNodeBalancing = true;
        boolean disableZoneBalancing = true;
        // Do some random swaps!
        boolean enableRandomSwaps = true;
        int swapAttempts = 100;
        int swapSuccesses = 1;
        Cluster repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                                    currentStores,
                                                                    currentCluster,
                                                                    currentStores,
                                                                    null,
                                                                    1,
                                                                    disableNodeBalancing,
                                                                    disableZoneBalancing,
                                                                    enableRandomSwaps,
                                                                    swapAttempts,
                                                                    swapSuccesses,
                                                                    false,
                                                                    null,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0);
        // Confirm repartitioned cluster is still imbalanced on all fronts
        assertFalse(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));

        // Confirm overall balance has been improved
        PartitionBalance repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);
        assertTrue(repartitionedPb.getUtility() < currentPb.getUtility());
    }

    /**
     * Verify that greedy swaps improve balance.
     * 
     * @param currentCluster
     * @param currentStores
     */
    public void verifyGreedySwapsImproveBalance(Cluster currentCluster,
                                                List<StoreDefinition> currentStores) {
        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));
        PartitionBalance currentPb = new PartitionBalance(currentCluster, currentStores);

        // Greedy test 1: Do greedy for each zone in cluster
        // Disable basic balancing among zones & nodes
        boolean disableNodeBalancing = true;
        boolean disableZoneBalancing = true;
        // Do some greedy swaps!
        boolean enableGreedySwaps = true;
        // For each zone
        List<Integer> greedyZones = new ArrayList<Integer>(currentCluster.getZoneIds());
        int swapAttempts = 1;
        int swapsPerNode = 100;
        int swapsPerZone = 100;
        Cluster repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                                    currentStores,
                                                                    currentCluster,
                                                                    currentStores,
                                                                    null,
                                                                    1,
                                                                    disableNodeBalancing,
                                                                    disableZoneBalancing,
                                                                    false,
                                                                    0,
                                                                    0,
                                                                    enableGreedySwaps,
                                                                    greedyZones,
                                                                    swapAttempts,
                                                                    swapsPerNode,
                                                                    swapsPerZone,
                                                                    0);
        // Confirm repartitioned cluster is still imbalanced on all fronts
        assertFalse(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));

        // Confirm overall balance has been improved
        PartitionBalance repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);
        assertTrue(repartitionedPb.getUtility() < currentPb.getUtility());

        // Greedy test 2: Only do greedy for a single zone.
        greedyZones = new ArrayList<Integer>();
        greedyZones.add(currentCluster.getZoneIds().iterator().next());

        repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                            currentStores,
                                                            currentCluster,
                                                            currentStores,
                                                            null,
                                                            1,
                                                            disableNodeBalancing,
                                                            disableZoneBalancing,
                                                            false,
                                                            0,
                                                            0,
                                                            enableGreedySwaps,
                                                            greedyZones,
                                                            swapAttempts,
                                                            swapsPerNode,
                                                            swapsPerZone,
                                                            0);
        // Confirm repartitioned cluster is still imbalanced on all fronts
        assertFalse(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));

        // Confirm overall balance has been improved
        repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);
        assertTrue(repartitionedPb.getUtility() < currentPb.getUtility());

        // Greedy test 3: Greedy overall nodes in cluster (rather than
        // zone-by-zone)
        greedyZones = null;

        repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                            currentStores,
                                                            currentCluster,
                                                            currentStores,
                                                            null,
                                                            1,
                                                            disableNodeBalancing,
                                                            disableZoneBalancing,
                                                            false,
                                                            0,
                                                            0,
                                                            enableGreedySwaps,
                                                            greedyZones,
                                                            swapAttempts,
                                                            swapsPerNode,
                                                            swapsPerZone,
                                                            0);
        // Confirm repartitioned cluster is still imbalanced on all fronts
        assertFalse(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));

        // Confirm overall balance has been improved
        repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);
        assertTrue(repartitionedPb.getUtility() < currentPb.getUtility());
    }

    @Test
    public void testRebalance() {
        // Two zone cluster
        Cluster currentCluster = ClusterInstanceTest.getZZCluster();
        List<StoreDefinition> storeDefs = ClusterInstanceTest.getZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRandomSwapsImproveBalance(currentCluster, storeDefs);
        verifyGreedySwapsImproveBalance(currentCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterInstanceTest.getZZZCluster();
        storeDefs = ClusterInstanceTest.getZZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRandomSwapsImproveBalance(currentCluster, storeDefs);
        verifyGreedySwapsImproveBalance(currentCluster, storeDefs);
    }

    @Test
    public void testClusterExpansion() {
        // Two zone cluster
        Cluster currentCluster = ClusterInstanceTest.getZZCluster();
        Cluster targetCluster = ClusterInstanceTest.getZZClusterWithNN();
        List<StoreDefinition> storeDefs = ClusterInstanceTest.getZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, targetCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterInstanceTest.getZZZCluster();
        targetCluster = ClusterInstanceTest.getZZZClusterWithNNN();
        storeDefs = ClusterInstanceTest.getZZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, targetCluster, storeDefs);
    }

    @Test
    public void testZoneExpansion() {
        Cluster currentCluster = ClusterInstanceTest.getZZCluster();
        List<StoreDefinition> currentStoreDefs = ClusterInstanceTest.getZZStoreDefsInMemory();

        Cluster targetCluster = ClusterInstanceTest.getZZZClusterWithNNN();
        List<StoreDefinition> targetStoreDefs = ClusterInstanceTest.getZZZStoreDefsInMemory();

        verifyBalanceZoneAndNode(currentCluster, currentStoreDefs, targetCluster, targetStoreDefs);
        // verifyBalanceNodesNotZones does not make sense for zone expansion.
        verifyRepartitionNoop(currentCluster, currentStoreDefs, targetCluster, targetStoreDefs);
    }

    /**
     * Helper to decide if a cluster has contiguous runs of partitions that are
     * "too long". Remember that getting rid of contiguous runs of partitions is
     * a stochastic and so cutoff should be higher than the target run length
     * given to repartitioning algorithm.
     * 
     * @param cluster
     * @param cutoff Definition of "too long". Runs longer than the cutoff are
     *        too long.
     * @return
     */
    public boolean verifyNoncontiguousPartitions(Cluster cluster, int cutoff) {
        for(int zoneId: cluster.getZoneIds()) {
            Map<Integer, Integer> rlMap = ClusterUtils.getMapOfContiguousPartitionRunLengths(cluster,
                                                                                             zoneId);
            for(int runLength: rlMap.keySet()) {
                if(runLength > cutoff)
                    return false;
            }
        }
        return true;
    }

    /**
     * Helper method to test breaking up contiguous runs of partitions
     * 
     * @param currentCluster
     * @param currentStores
     */
    public void decontigRepartition(Cluster currentCluster, List<StoreDefinition> currentStores) {
        Cluster repartitionedCluster;

        // Repartition to balance partition IDs among zones and among nodes
        // within zone.
        int maxContigRun = 1;

        // Confirm current cluster has contiguous runs that are too long
        assertFalse(verifyNoncontiguousPartitions(currentCluster, maxContigRun + 1));

        // Do not do any repartitioning other than that necessary to get rid of
        // contiguous runs
        boolean disableNodeBalancing = true;
        repartitionedCluster = RepartitionUtils.repartition(currentCluster,
                                                            currentStores,
                                                            currentCluster,
                                                            currentStores,
                                                            null,
                                                            1,
                                                            disableNodeBalancing,
                                                            false,
                                                            false,
                                                            0,
                                                            0,
                                                            false,
                                                            null,
                                                            0,
                                                            0,
                                                            0,
                                                            maxContigRun);
        assertTrue(verifyNoncontiguousPartitions(repartitionedCluster, maxContigRun + 1));
    }

    @Test
    public void testDeContig() {
        // Two zone cluster
        Cluster currentCluster = ClusterInstanceTest.getZZCluster();
        List<StoreDefinition> storeDefs = ClusterInstanceTest.getZZStoreDefsInMemory();
        decontigRepartition(currentCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterInstanceTest.getZZZCluster();
        storeDefs = ClusterInstanceTest.getZZZStoreDefsInMemory();
        decontigRepartition(currentCluster, storeDefs);
    }

    // TODO: Switch from numberOfZones to a set of zoneIds
    // TODO: Create a ClusterTest or ClusterUtilsTest and add this test to that
    // class/file
    @Test
    public void testGetMapOfContiguousPartitionRunLengths() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 } };
        Cluster cluster = ServerTestUtils.getLocalZonedCluster(numberOfZones,
                                                               nodesPerZone,
                                                               partitionMap);
        Map<Integer, Integer> iiMap;
        // Zone 0:
        // 0, 1, 2, 6, 7, 8, 12, 14, 15, 16, 17
        // -------oo-------oo--oo-------------- !Wraps around!

        // => {14,7}, {6,3}, {12,1}
        iiMap = ClusterUtils.getMapOfContiguousPartitions(cluster, 0);
        assertTrue(iiMap.containsKey(6));
        assertTrue(iiMap.get(6) == 3);
        assertTrue(iiMap.containsKey(12));
        assertTrue(iiMap.get(12) == 1);
        assertTrue(iiMap.containsKey(14));
        assertTrue(iiMap.get(14) == 7);

        // => {3,1}, {1,1}, {7,1}
        iiMap = ClusterUtils.getMapOfContiguousPartitionRunLengths(cluster, 0);
        assertTrue(iiMap.containsKey(1));
        assertTrue(iiMap.get(1) == 1);
        assertTrue(iiMap.containsKey(3));
        assertTrue(iiMap.get(3) == 1);
        assertTrue(iiMap.containsKey(7));
        assertTrue(iiMap.get(7) == 1);

        // Zone 1:
        // 3, 4, 5, 9, 10, 11, 13
        // -------oo---------oo--

        // => {3,3}, {9,3}, {13,1}
        iiMap = ClusterUtils.getMapOfContiguousPartitions(cluster, 1);
        assertTrue(iiMap.containsKey(3));
        assertTrue(iiMap.get(3) == 3);
        assertTrue(iiMap.containsKey(9));
        assertTrue(iiMap.get(9) == 3);
        assertTrue(iiMap.containsKey(13));
        assertTrue(iiMap.get(13) == 1);

        // => {1,1}, {3,2}
        iiMap = ClusterUtils.getMapOfContiguousPartitionRunLengths(cluster, 1);
        assertTrue(iiMap.containsKey(1));
        assertTrue(iiMap.get(1) == 1);
        assertTrue(iiMap.containsKey(3));
        assertTrue(iiMap.get(3) == 2);
    }

    @Test
    public void testRemoveItemsToSplitListEvenly() {
        // input of size 5
        List<Integer> input = new ArrayList<Integer>();
        System.out.println("Input of size 5");
        for(int i = 0; i < 5; ++i) {
            input.add(i);
        }

        List<Integer> output = RepartitionUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(1), new Integer(3));
        System.out.println("1 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("2 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("3 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("4 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 0);
        System.out.println("5 : " + output);

        // input of size 10
        input.clear();
        System.out.println("Input of size 10");
        for(int i = 0; i < 10; ++i) {
            input.add(i);
        }

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(4), new Integer(9));
        System.out.println("1 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(2), new Integer(8));
        System.out.println("2 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("3 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("4 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("5 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("6 : " + output);

        // input of size 20
        input.clear();
        System.out.println("Input of size 20");
        for(int i = 0; i < 20; ++i) {
            input.add(i);
        }

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 10);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(9), new Integer(19));
        System.out.println("1 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 6);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(5), new Integer(17));
        System.out.println("2 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(4), new Integer(17));
        System.out.println("3 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 4);
        assertEquals(output.get(0), new Integer(4));
        assertEquals(output.get(3), new Integer(16));
        System.out.println("4 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(5));
        assertEquals(output.get(2), new Integer(15));
        System.out.println("5 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("6 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 7);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("7 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 9);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("9 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 10);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("10 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 11);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("11 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 19);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("19 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 20);
        assertEquals(output.size(), 0);
        System.out.println("20 : " + output);
    }

    @Test
    public void testPeanutButterList() {

        List<Integer> pbList;

        pbList = RepartitionUtils.peanutButterList(4, 4);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(1));
        assertEquals(pbList.get(1), new Integer(1));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(1));

        pbList = RepartitionUtils.peanutButterList(4, 6);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(2));
        assertEquals(pbList.get(1), new Integer(2));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(1));

        pbList = RepartitionUtils.peanutButterList(4, 3);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(1));
        assertEquals(pbList.get(1), new Integer(1));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(0));

        pbList = RepartitionUtils.peanutButterList(4, 0);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(0));
        assertEquals(pbList.get(1), new Integer(0));
        assertEquals(pbList.get(2), new Integer(0));
        assertEquals(pbList.get(3), new Integer(0));

        boolean caught = false;
        try {
            pbList = RepartitionUtils.peanutButterList(0, 10);
        } catch(IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            pbList = RepartitionUtils.peanutButterList(4, -5);
        } catch(IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);
    }

}
