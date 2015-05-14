package voldemort.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.rebalance.RebalancePlan;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.StoreDefinitionUtils;

import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class ZoneCLipperTest {

    private Cluster initialCluster;
    private int dropZoneId;
    private List<StoreDefinition> storeDefs;

    public ZoneCLipperTest(Cluster initialCluster, int dropZoneId, List<StoreDefinition> storeDefs) {
        this.initialCluster = initialCluster;
        this.dropZoneId = dropZoneId;
        this.storeDefs = storeDefs;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { VoldemortTestConstants.getNineNodeClusterWith3Zones(), 0,
                        ClusterTestUtils.getZZZStoreDefsBDB() },
                { VoldemortTestConstants.getNineNodeClusterWith3Zones(), 1,
                        ClusterTestUtils.getZZZStoreDefsBDB() },
                { VoldemortTestConstants.getNineNodeClusterWith3Zones(), 2,
                        ClusterTestUtils.getZZZStoreDefsBDB() },
                { VoldemortTestConstants.getSixNodeClusterWith3Zones(), 2,
                        ClusterTestUtils.getZZZ211StoreDefs("bdb") },
                // Test with 2 zones cluster
                { VoldemortTestConstants.getEightNodeClusterWithZones(), 0,
                        ClusterTestUtils.getZZ211StoreDefs("bdb") },
                // Test with 2 zones cluster Different store Def
                { VoldemortTestConstants.getEightNodeClusterWithZones(), 0,
                        ClusterTestUtils.getZZ322StoreDefs("bdb") }, });
    }

    @Test
    public void testDropZoneIdWorks() {

        // Create a list of current partition ids. We will use this set to
        // compare partitions ids with the interim cluster
        Set<Integer> originalPartitions = new HashSet<Integer>();
        for(Integer zoneId: initialCluster.getZoneIds()) {
            originalPartitions.addAll(initialCluster.getPartitionIdsInZone(zoneId));
        }

        // Get an intermediate cluster where partitions that belong to the zone
        // that is being dropped have been moved to the existing zones
        Cluster interimCluster = RebalanceUtils.vacateZone(initialCluster, dropZoneId);

        // Make sure that the intermediate cluster should have same number of
        // partitions
        RebalanceUtils.validateClusterPartitionCounts(initialCluster, interimCluster);

        // Make sure that the intermediate cluster should have same number of
        // nodes
        RebalanceUtils.validateClusterNodeCounts(initialCluster, interimCluster);

        // Make sure that the intermediate cluster doesn't have any partitons in
        // the dropped zone
        assertTrue("Zone being dropped has partitions. ZoneClipper didn't work properly",
                   interimCluster.getPartitionIdsInZone(dropZoneId).isEmpty());

        // Make sure that the nodes being dropped don't host any partitions
        for(Integer nodeId: interimCluster.getNodeIdsInZone(dropZoneId)) {
            assertTrue("Nodes in the zone being dropped don't have empty partitions list",
                       interimCluster.getNodeById(nodeId).getPartitionIds().isEmpty());
        }

        Set<Integer> finalPartitions = new HashSet<Integer>();
        for(Integer zoneId: interimCluster.getZoneIds()) {
            finalPartitions.addAll(interimCluster.getPartitionIdsInZone(zoneId));
        }

        // Compare to original partition ids list
        assertTrue("Original and interm partition ids don't match",
                   originalPartitions.equals(finalPartitions));

        // Make sure that there is no data movement
        RebalancePlan rebalancePlan = ClusterTestUtils.makePlan(initialCluster,
                                                                storeDefs,
                                                                interimCluster,
                                                                storeDefs);
        // Make sure we have a plan
        assertEquals(rebalancePlan.getPlan().size(), 1);

        // Make sure there is no cross zones between zones in the plan
        assertEquals(rebalancePlan.getPartitionStoresMovedXZone(), 0);
        // Make sure there is no data movement between nodes
        assertEquals(rebalancePlan.getPartitionStoresMoved(), 0);
        for(Integer nodeId: interimCluster.getNodeIds()) {
            Set<Integer> remainingNodes = Sets.symmetricDifference(interimCluster.getNodeIds(),
                                                                   Sets.newHashSet(nodeId));
            for(Integer otherNodeId: remainingNodes) {
                assertTrue("Something went wrong as there is data movement between nodes",
                           rebalancePlan.getNodeMoveMap().get(nodeId, otherNodeId) == 0);
            }
        }

        // Also get the adjusted store definitions, with the zone dropped
        Cluster finalCluster = RebalanceUtils.dropZone(interimCluster, dropZoneId);
        List<StoreDefinition> clippedStoreDefs = RebalanceUtils.dropZone(storeDefs, dropZoneId);

        for(StoreDefinition storeDef: clippedStoreDefs) {
            StoreDefinition orgStoreDef = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                          storeDef.getName());
            assertFalse("Clipped storedef has replication for dropped zone",
                        storeDef.getZoneReplicationFactor().containsKey(dropZoneId));
            assertEquals("Clipped storedef has incorrect number of zones",
                         initialCluster.getZones().size() - 1,
                         storeDef.getZoneReplicationFactor().size());
            assertEquals("Clipped storedef has incorrect total repfactor",
                         orgStoreDef.getReplicationFactor()
                                 - orgStoreDef.getZoneReplicationFactor().get(dropZoneId),
                         storeDef.getReplicationFactor());
        }

        // Confirm that we would not route to any of the dropped nodes for any
        // store.
        Set<Integer> dropNodes = interimCluster.getNodeIdsInZone(dropZoneId);
        for(StoreDefinition storeDef: clippedStoreDefs) {
            StoreRoutingPlan routingPlan = new StoreRoutingPlan(finalCluster, storeDef);
            Node[] partitionToNode = finalCluster.getPartitionIdToNodeArray();
            for(int p = 0; p < partitionToNode.length; p++) {
                List<Integer> replicaNodes = routingPlan.getReplicationNodeList(p);
                assertFalse("Should not be routing to any dropped nodes",
                            replicaNodes.removeAll(dropNodes));
            }
        }
    }
}
