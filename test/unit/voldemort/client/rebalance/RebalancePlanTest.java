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

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.ClusterInstanceTest;

public class RebalancePlanTest {

    static Cluster zzCurrent;
    static Cluster zzRebalance;
    static Cluster zzClusterExpansion;
    static List<StoreDefinition> zzStores;

    static Cluster zzzCurrent;
    static Cluster zzzRebalance;
    static Cluster zzzClusterExpansion;
    static Cluster zzzZoneExpansion;
    static List<StoreDefinition> zzzStores;

    @BeforeClass
    public static void setup() {
        zzCurrent = ClusterInstanceTest.getZZCluster();
        zzRebalance = ClusterInstanceTest.getZZClusterWithSwappedPartitions();
        zzClusterExpansion = ClusterInstanceTest.getZZClusterWithPP();
        zzStores = ClusterInstanceTest.getZZStoreDefsBDB();

        zzzCurrent = ClusterInstanceTest.getZZZCluster();

        zzzRebalance = ClusterInstanceTest.getZZZClusterWithSwappedPartitions();
        zzzClusterExpansion = ClusterInstanceTest.getZZZClusterWithPPP();
        zzzZoneExpansion = ClusterInstanceTest.getZZEClusterXXP();
        zzzStores = ClusterInstanceTest.getZZZStoreDefsBDB();
    }

    RebalancePlan makePlan(Cluster cCluster,
                           List<StoreDefinition> cStores,
                           Cluster fCluster,
                           List<StoreDefinition> fStores) {
        // Defaults for plans
        boolean stealerBased = true;
        int batchSize = 1000;
        String outputDir = null;

        return new RebalancePlan(cCluster,
                                 cStores,
                                 fCluster,
                                 fStores,
                                 stealerBased,
                                 batchSize,
                                 outputDir);
    }

    @Test
    public void testNoop() {
        RebalancePlan rebalancePlan;

        // Two zones
        rebalancePlan = makePlan(zzCurrent, zzStores, zzCurrent, zzStores);
        assertEquals(rebalancePlan.getPlan().size(), 0);
        assertEquals(rebalancePlan.getPrimariesMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        // Three zones
        rebalancePlan = makePlan(zzzCurrent, zzzStores, zzzCurrent, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 0);
        assertEquals(rebalancePlan.getPrimariesMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMoved(), 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);
    }

    @Test
    public void testRebalance() {
        RebalancePlan rebalancePlan;

        // Two zones
        rebalancePlan = makePlan(zzCurrent, zzStores, zzRebalance, zzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        // Three zones
        rebalancePlan = makePlan(zzzCurrent, zzzStores, zzzRebalance, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);
    }

    @Test
    public void testClusterExpansion() {
        RebalancePlan rebalancePlan;

        // Two zones
        rebalancePlan = makePlan(zzCurrent, zzStores, zzClusterExpansion, zzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);

        // Three zones
        rebalancePlan = makePlan(zzzCurrent, zzzStores, zzzClusterExpansion, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);
    }

    @Test
    public void testZoneExpansion() {
        RebalancePlan rebalancePlan;

        rebalancePlan = makePlan(zzCurrent, zzStores, zzzZoneExpansion, zzzStores);
        assertEquals(rebalancePlan.getPlan().size(), 1);
        assertTrue(rebalancePlan.getPrimariesMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMoved() > 0);
        assertTrue(rebalancePlan.getPartitionStoresMovedXZone() > 0);
    }
}
