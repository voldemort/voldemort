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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.PartitionBalanceUtils;

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
     * @param interimCluster
     * @param finalStores
     */
    public void verifyBalanceZoneAndNode(Cluster currentCluster,
                                         List<StoreDefinition> currentStores,
                                         Cluster interimCluster,
                                         List<StoreDefinition> finalStores) {
        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));

        // Repartition to balance partition IDs among zones and among nodes
        // within zone.
        boolean disableNodeBalancing = false;
        boolean disableZoneBalancing = false;
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
                                                                 currentStores,
                                                                 interimCluster,
                                                                 finalStores,
                                                                 null,
                                                                 1,
                                                                 disableNodeBalancing,
                                                                 disableZoneBalancing,
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
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
     * @param interimCluster
     * @param finalStores
     */
    public void verifyBalanceNodesNotZones(Cluster currentCluster,
                                           List<StoreDefinition> currentStores,
                                           Cluster interimCluster,
                                           List<StoreDefinition> finalStores) {

        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));

        // Repartition to balance partition IDs among zones and among nodes
        // within zone.
        boolean disableNodeBalancing = false;
        boolean disableZoneBalancing = true;
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
                                                                 currentStores,

                                                                 interimCluster,
                                                                 finalStores,
                                                                 null,
                                                                 1,
                                                                 disableNodeBalancing,
                                                                 disableZoneBalancing,
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 0);
        assertTrue(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));
    }

    /**
     * Verify the "no op" path through repartition method does not change the
     * interim cluster.
     * 
     * @param currentCluster
     * @param currentStores
     * @param interimCluster
     * @param finalStores
     */
    public void verifyRepartitionNoop(Cluster currentCluster,
                                      List<StoreDefinition> currentStores,
                                      Cluster interimCluster,
                                      List<StoreDefinition> finalStores) {
        // Confirm current cluster is imbalanced on all fronts:
        assertFalse(verifyNodesBalancedInEachZone(currentCluster));
        assertFalse(verifyZonesBalanced(currentCluster));

        // Confirm noop rebalance has no effect on interim cluster
        boolean disableNodeBalancing = true;
        boolean disableZoneBalancing = true;
        Cluster repartitionedCluster = Repartitioner.repartition(currentCluster,
                                                                 currentStores,
                                                                 interimCluster,
                                                                 finalStores,
                                                                 null,
                                                                 1,
                                                                 disableNodeBalancing,
                                                                 disableZoneBalancing,
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 0);
        assertTrue(repartitionedCluster.equals(interimCluster));
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
                                                                 Collections.<Integer> emptyList(),
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 0);
        // Confirm repartitioned cluster is still imbalanced on all fronts
        assertFalse(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));

        // Confirm overall balance has been improved
        PartitionBalance repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);
        assertTrue(repartitionedPb.getUtility() < currentPb.getUtility());
    }

    /**
     * Verify that random swaps within improve balance
     * 
     * @param currentCluster
     * @param currentStores
     * @param swapZoneIds
     */
    public void verifyRandomSwapsWithinZoneOnlyShufflesParitionsInThatZone(Cluster currentCluster,
                                                          List<StoreDefinition> currentStores,
                                                          List<Integer> swapZoneIds) {

        PartitionBalance currentPb = new PartitionBalance(currentCluster, currentStores);
        // Disable basic balancing among zones & nodes
        boolean disableNodeBalancing = true;
        boolean disableZoneBalancing = true;
        // Do some random swaps within zone
        boolean enableRandomSwaps = true;
        int swapAttempts = 100;
        int swapSuccesses = 10;
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
                                                                 swapZoneIds,
                                                                 false,
                                                                 0,
                                                                 0,
                                                                 0,
                                                                 Collections.<Integer> emptyList(),
                                                                 0);
        PartitionBalance repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);

        Set<Integer> allNodeIds = repartitionedCluster.getNodeIds();
        Set<Integer> swapNodeIds = null;
        for (Integer swapZoneId: swapZoneIds) {
            swapNodeIds = repartitionedCluster.getNodeIdsInZone(swapZoneId);
        }
        // Remove nodes that we don't want to verify the partition count on
        allNodeIds.removeAll(swapNodeIds);
        for (Integer remainingNodeId : allNodeIds) {
            Set<Integer> beforeRepartition = new HashSet<Integer>(currentCluster
                                                                  .getNodeById(remainingNodeId)
                                                                  .getPartitionIds());
            Set<Integer> afterRepartition = new HashSet<Integer>(currentCluster
                                                                 .getNodeById(remainingNodeId)
                                                                 .getPartitionIds());
            assertTrue(beforeRepartition.equals(afterRepartition));
            assertTrue(repartitionedPb.getUtility() <= currentPb.getUtility());
        }

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
                                                                 null,
                                                                 enableGreedySwaps,
                                                                 swapAttempts,
                                                                 swapsPerNode,
                                                                 swapsPerZone,
                                                                 greedyZones,
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
                                                         null,
                                                         enableGreedySwaps,
                                                         swapAttempts,
                                                         swapsPerNode,
                                                         swapsPerZone,
                                                         greedyZones,
                                                         0);
        // Confirm repartitioned cluster is still imbalanced on all fronts
        assertFalse(verifyNodesBalancedInEachZone(repartitionedCluster));
        assertFalse(verifyZonesBalanced(repartitionedCluster));

        // Confirm overall balance has been improved
        repartitionedPb = new PartitionBalance(repartitionedCluster, currentStores);
        assertTrue(repartitionedPb.getUtility() < currentPb.getUtility());

        // Greedy test 3: Greedy overall nodes in cluster (rather than
        // zone-by-zone)
        greedyZones = Collections.<Integer> emptyList();

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
                                                         null,
                                                         enableGreedySwaps,
                                                         swapAttempts,
                                                         swapsPerNode,
                                                         swapsPerZone,
                                                         greedyZones,
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
    public void testShuffleWithinZone() {
        // Two zone cluster
        Cluster currentCluster = ClusterTestUtils.getZZCluster();
        List<StoreDefinition> storeDefs = ClusterTestUtils.getZZStoreDefsInMemory();
        List<Integer> swapZoneIds = new ArrayList<Integer>();
        // Only shuffle within zone 1
        swapZoneIds.add(1);
        verifyRandomSwapsWithinZoneOnlyShufflesParitionsInThatZone(currentCluster, storeDefs,
                swapZoneIds);
       
        // Three zone cluster
        currentCluster = ClusterTestUtils.getZZZCluster();
        storeDefs = ClusterTestUtils.getZZZStoreDefsInMemory();
        // Shuffle only within zone 2
        swapZoneIds.clear();
        swapZoneIds.add(2);
        verifyRandomSwapsWithinZoneOnlyShufflesParitionsInThatZone(currentCluster, storeDefs,
                swapZoneIds);
        // Shuffle only within zone 1, 2
        swapZoneIds.clear();
        swapZoneIds.add(1);
        swapZoneIds.add(2);
        verifyRandomSwapsWithinZoneOnlyShufflesParitionsInThatZone(currentCluster, storeDefs,
                swapZoneIds);
        // Shuffle only within zone 0, 2
        swapZoneIds.clear();
        swapZoneIds.add(0);
        swapZoneIds.add(2);
        verifyRandomSwapsWithinZoneOnlyShufflesParitionsInThatZone(currentCluster, storeDefs,
                swapZoneIds);
    }

    @Test
    public void testClusterExpansion() {
        // Two zone cluster
        Cluster currentCluster = ClusterTestUtils.getZZCluster();
        Cluster interimCluster = ClusterTestUtils.getZZClusterWithNN();
        List<StoreDefinition> storeDefs = ClusterTestUtils.getZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, interimCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, interimCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, interimCluster, storeDefs);

        // Three zone cluster
        currentCluster = ClusterTestUtils.getZZZCluster();
        interimCluster = ClusterTestUtils.getZZZClusterWithNNN();
        storeDefs = ClusterTestUtils.getZZZStoreDefsInMemory();
        verifyBalanceZoneAndNode(currentCluster, storeDefs, interimCluster, storeDefs);
        verifyBalanceNodesNotZones(currentCluster, storeDefs, interimCluster, storeDefs);
        verifyRepartitionNoop(currentCluster, storeDefs, interimCluster, storeDefs);
    }

    @Test
    public void testZoneExpansionAsRepartitionerCLI() {
        Cluster currentCluster = ClusterTestUtils.getZZCluster();
        List<StoreDefinition> currentStoreDefs = ClusterTestUtils.getZZStoreDefsInMemory();

        Cluster interimCluster = ClusterTestUtils.getZZZClusterWithNNN();
        List<StoreDefinition> finalStoreDefs = ClusterTestUtils.getZZZStoreDefsInMemory();

        verifyBalanceZoneAndNode(currentCluster, currentStoreDefs, interimCluster, finalStoreDefs);
        // verifyBalanceNodesNotZones does not make sense for zone expansion.
        verifyRepartitionNoop(currentCluster, currentStoreDefs, interimCluster, finalStoreDefs);
    }

    @Test
    public void testZoneExpansionAsRebalanceControllerCLI() {
        Cluster currentCluster = ClusterTestUtils.getZZECluster();
        List<StoreDefinition> currentStoreDefs = ClusterTestUtils.getZZZStoreDefsInMemory();

        Cluster interimCluster = ClusterTestUtils.getZZZClusterWithNNN();
        List<StoreDefinition> finalStoreDefs = ClusterTestUtils.getZZZStoreDefsInMemory();

        verifyBalanceZoneAndNode(currentCluster, currentStoreDefs, interimCluster, finalStoreDefs);
        // verifyBalanceNodesNotZones does not make sense for zone expansion.
        verifyRepartitionNoop(currentCluster, currentStoreDefs, interimCluster, finalStoreDefs);
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
            Map<Integer, Integer> rlMap = PartitionBalanceUtils.getMapOfContiguousPartitionRunLengths(cluster,
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
                                                         null,
                                                         false,
                                                         0,
                                                         0,
                                                         0,
                                                         Collections.<Integer> emptyList(),
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
