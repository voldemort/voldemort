/*
 * Copyright 2012-2013 LinkedIn, Inc
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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.MoveMap;

public class RebalancePlanTest {

    static Cluster zzCurrent;
    static Cluster zzShuffle;
    static Cluster zzClusterExpansion;
    static List<StoreDefinition> zzStores;

    static Cluster zzzCurrent;
    static Cluster zzzShuffle;
    static Cluster zzzClusterExpansion;
    static Cluster zzzZoneExpansion;
    static List<StoreDefinition> zzzStores;

    @BeforeClass
    public static void setup() {
        zzCurrent = ClusterTestUtils.getZZCluster();
        zzShuffle = ClusterTestUtils.getZZClusterWithSwappedPartitions();
        zzClusterExpansion = ClusterTestUtils.getZZClusterWithPP();
        zzStores = ClusterTestUtils.getZZStoreDefsBDB();

        zzzCurrent = ClusterTestUtils.getZZZCluster();

        zzzShuffle = ClusterTestUtils.getZZZClusterWithSwappedPartitions();
        zzzClusterExpansion = ClusterTestUtils.getZZZClusterWithPPP();
        zzzZoneExpansion = ClusterTestUtils.getZZEClusterXXP();
        zzzStores = ClusterTestUtils.getZZZStoreDefsBDB();
    }

    @Test
    public void testNoop() {
        RebalancePlan rebalancePlan;

        // Two zones
        rebalancePlan = ClusterTestUtils.makePlan(zzCurrent, zzStores, zzCurrent, zzStores);
        assertEquals(rebalancePlan.getPlan().size(), 0);
        assertEquals(rebalancePlan.getPrimariesMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        // Three zones
        rebalancePlan = ClusterTestUtils.makePlan(zzzCurrent, zzzStores, zzzCurrent, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 0);
        assertEquals(rebalancePlan.getPrimariesMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);
    }

    @Test
    public void testShuffle() {
        RebalancePlan rebalancePlan;

        // Two zones
        rebalancePlan = ClusterTestUtils.makePlan(zzCurrent, zzStores, zzShuffle, zzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        MoveMap zoneMoves = rebalancePlan.getZoneMoveMap();
        assertTrue(zoneMoves.get(0, 0) > 0);
        assertTrue(zoneMoves.get(0, 1) == 0);
        assertTrue(zoneMoves.get(1, 0) == 0);
        assertTrue(zoneMoves.get(1, 1) > 0);

        // Three zones
        rebalancePlan = ClusterTestUtils.makePlan(zzzCurrent, zzzStores, zzzShuffle, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        zoneMoves = rebalancePlan.getZoneMoveMap();
        assertTrue(zoneMoves.get(0, 0) > 0);
        assertTrue(zoneMoves.get(0, 1) == 0);
        assertTrue(zoneMoves.get(0, 2) == 0);
        assertTrue(zoneMoves.get(1, 0) == 0);
        assertTrue(zoneMoves.get(1, 1) > 0);
        assertTrue(zoneMoves.get(1, 2) == 0);
        assertTrue(zoneMoves.get(2, 0) == 0);
        assertTrue(zoneMoves.get(2, 1) == 0);
        assertTrue(zoneMoves.get(2, 2) > 0);
    }

    @Test
    public void testClusterExpansion() {
        RebalancePlan rebalancePlan;

        // Two zones
        rebalancePlan = ClusterTestUtils.makePlan(zzCurrent, zzStores, zzClusterExpansion, zzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        MoveMap zoneMoves = rebalancePlan.getZoneMoveMap();
        assertTrue(zoneMoves.get(0, 0) > 0);
        assertTrue(zoneMoves.get(0, 1) == 0);
        assertTrue(zoneMoves.get(1, 0) == 0);
        assertTrue(zoneMoves.get(1, 1) > 0);

        // Three zones
        rebalancePlan = ClusterTestUtils.makePlan(zzzCurrent, zzzStores, zzzClusterExpansion, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        zoneMoves = rebalancePlan.getZoneMoveMap();
        assertTrue(zoneMoves.get(0, 0) > 0);
        assertTrue(zoneMoves.get(0, 1) == 0);
        assertTrue(zoneMoves.get(0, 2) == 0);
        assertTrue(zoneMoves.get(1, 0) == 0);
        assertTrue(zoneMoves.get(1, 1) > 0);
        assertTrue(zoneMoves.get(1, 2) == 0);
        assertTrue(zoneMoves.get(2, 0) == 0);
        assertTrue(zoneMoves.get(2, 1) == 0);
        assertTrue(zoneMoves.get(2, 2) > 0);
    }

    @Test
    public void testZoneExpansion() {
        RebalancePlan rebalancePlan;

        rebalancePlan = ClusterTestUtils.makePlan(zzCurrent, zzStores, zzzZoneExpansion, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMovedXZone() > 0);

        // zone id 2 is the new zone.
        MoveMap zoneMoves = rebalancePlan.getZoneMoveMap();
        assertTrue(zoneMoves.get(0, 0) > 0);
        assertTrue(zoneMoves.get(0, 1) == 0);
        assertTrue(zoneMoves.get(0, 2) > 0);
        assertTrue(zoneMoves.get(1, 0) == 0);
        assertTrue(zoneMoves.get(1, 1) > 0);
        assertTrue(zoneMoves.get(1, 2) > 0);
        assertTrue(zoneMoves.get(2, 0) == 0);
        assertTrue(zoneMoves.get(2, 1) == 0);
        assertTrue(zoneMoves.get(2, 2) == 0);
    }
}
