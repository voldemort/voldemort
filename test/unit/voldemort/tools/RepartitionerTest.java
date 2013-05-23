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

package voldemort.tools;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.ClusterUtils;

public class RepartitionerTest {

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
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
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
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
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
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
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
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
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
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
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

        repartitionedCluster = Repartitioner.repartition(currentCluster,
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

        repartitionedCluster = Repartitioner.repartition(currentCluster,
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
    public void testShuffle() {
        // Two zone cluster
        Cluster currentCluster = ClusterTestUtils.getZZCluster();
        List<StoreDefinition> storeDefs = ClusterTestUtils.getZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRandomSwapsImproveBalance(currentCluster, storeDefs);
        verifyGreedySwapsImproveBalance(currentCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterTestUtils.getZZZCluster();
        storeDefs = ClusterTestUtils.getZZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, currentCluster, storeDefs);
        verifyRandomSwapsImproveBalance(currentCluster, storeDefs);
        verifyGreedySwapsImproveBalance(currentCluster, storeDefs);
    }

    @Test
    public void testClusterExpansion() {
        // Two zone cluster
        Cluster currentCluster = ClusterTestUtils.getZZCluster();
        Cluster targetCluster = ClusterTestUtils.getZZClusterWithNN();
        List<StoreDefinition> storeDefs = ClusterTestUtils.getZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, targetCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterTestUtils.getZZZCluster();
        targetCluster = ClusterTestUtils.getZZZClusterWithNNN();
        storeDefs = ClusterTestUtils.getZZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, targetCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, targetCluster, storeDefs);
    }

    @Test
    public void testZoneExpansion() {
        Cluster currentCluster = ClusterTestUtils.getZZECluster();
        List<StoreDefinition> currentStoreDefs = ClusterTestUtils.getZZZStoreDefsInMemory();

        Cluster targetCluster = ClusterTestUtils.getZZZClusterWithNNN();
        List<StoreDefinition> targetStoreDefs = ClusterTestUtils.getZZZStoreDefsInMemory();

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
        repartitionedCluster = Repartitioner.repartition(currentCluster,
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
        Cluster currentCluster = ClusterTestUtils.getZZCluster();
        List<StoreDefinition> storeDefs = ClusterTestUtils.getZZStoreDefsInMemory();
        decontigRepartition(currentCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterTestUtils.getZZZCluster();
        storeDefs = ClusterTestUtils.getZZZStoreDefsInMemory();
        decontigRepartition(currentCluster, storeDefs);
    }

}
