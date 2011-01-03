package voldemort.client.rebalance;

import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalanceClusterPlanTest {

    private final int NUM_NODES = 4;
    private final int NUM_ZONES = 2;
    private Cluster consistentRoutingCluster;
    private Cluster consistentRoutingClusterModified;
    private Cluster zoneRoutingCluster;
    private Cluster zoneRoutingClusterModified;
    private HashMap<Integer, Integer> zoneReplicationFactors;

    @Before
    public void setUp() {
        List<Node> nodes = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            nodes.add(new Node(i, "node" + i, 100, 200, 300, Lists.newArrayList(i, i + NUM_NODES)));
        }
        consistentRoutingCluster = new Cluster("consistent1", nodes);

        List<Node> nodes2 = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            List<Integer> partitions = Lists.newArrayList();
            if(i == 0)
                partitions.add(i + NUM_NODES);
            else {
                partitions.add(i);
                partitions.add(i + NUM_NODES);
            }

            if(i == NUM_NODES - 1)
                partitions.add(0);
            nodes2.add(new Node(i, "node" + i, 100, 200, 300, partitions));
        }
        consistentRoutingClusterModified = new Cluster("consistent2", nodes2);

        List<Zone> zones = ServerTestUtils.getZones(NUM_ZONES);

        List<Node> nodes3 = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {

            if(i < NUM_NODES / 2)
                nodes3.add(new Node(i,
                                    "node" + i,
                                    100,
                                    200,
                                    300,
                                    0,
                                    Lists.newArrayList(i, i + NUM_NODES)));
            else
                nodes3.add(new Node(i,
                                    "node" + i,
                                    100,
                                    200,
                                    300,
                                    1,
                                    Lists.newArrayList(i, i + NUM_NODES)));
        }
        zoneRoutingCluster = new Cluster("zone1", nodes3, zones);

        List<Node> nodes4 = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            List<Integer> partitions = Lists.newArrayList();
            if(i == 0)
                partitions.add(i + NUM_NODES);
            else {
                partitions.add(i);
                partitions.add(i + NUM_NODES);
            }

            if(i == NUM_NODES - 1)
                partitions.add(0);

            if(i < NUM_NODES / 2)
                nodes4.add(new Node(i, "node" + i, 100, 200, 300, 0, partitions));
            else
                nodes4.add(new Node(i, "node" + i, 100, 200, 300, 1, partitions));
        }
        zoneRoutingClusterModified = new Cluster("zone1", nodes4, zones);

        zoneReplicationFactors = Maps.newHashMap();
    }

    /**
     * Tests Consistent Routing -> Consistent Routing with no replication factor
     * change
     */
    @Test
    public void testConsistentToConsistent() {
        StoreDefinition storeDefRepFactor1 = ServerTestUtils.getStoreDef("consistent_store",
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         RoutingStrategyType.CONSISTENT_STRATEGY);
        RebalanceClusterPlan plan = new RebalanceClusterPlan(consistentRoutingCluster,
                                                             consistentRoutingClusterModified,
                                                             Lists.newArrayList(storeDefRepFactor1),
                                                             false,
                                                             null);

        HashMap<Integer, RebalanceNodePlan> nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));

        StoreDefinition storeDefRepFactor2 = ServerTestUtils.getStoreDef("consistent_store",
                                                                         2,
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         RoutingStrategyType.CONSISTENT_STRATEGY);
        plan = new RebalanceClusterPlan(consistentRoutingCluster,
                                        consistentRoutingClusterModified,
                                        Lists.newArrayList(storeDefRepFactor2),
                                        false,
                                        null);

        nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));

    }

    /**
     * Tests Consistent Routing -> Consistent Routing with replication factor
     * change
     */
    @Test
    public void testConsistentToConsistentWithRepFactorChange() {
        StoreDefinition beforeStoreDef = ServerTestUtils.getStoreDef("consistent_store",
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        StoreDefinition afterStoreDef = ServerTestUtils.getStoreDef("consistent_store",
                                                                    2,
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    RoutingStrategyType.CONSISTENT_STRATEGY);

        RebalanceClusterPlan plan = new RebalanceClusterPlan(consistentRoutingCluster,
                                                             consistentRoutingClusterModified,
                                                             Lists.newArrayList(beforeStoreDef),
                                                             Lists.newArrayList(afterStoreDef),
                                                             false,
                                                             null);

        HashMap<Integer, RebalanceNodePlan> nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 4);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(3));

        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0, 4));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getDonorId(), 1);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(1, 5));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(1).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(2, 6));

        beforeStoreDef = ServerTestUtils.getStoreDef("consistent_store",
                                                     2,
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef = ServerTestUtils.getStoreDef("consistent_store",
                                                    3,
                                                    1,
                                                    1,
                                                    1,
                                                    1,
                                                    RoutingStrategyType.CONSISTENT_STRATEGY);

        plan = new RebalanceClusterPlan(consistentRoutingCluster,
                                        consistentRoutingClusterModified,
                                        Lists.newArrayList(beforeStoreDef),
                                        Lists.newArrayList(afterStoreDef),
                                        false,
                                        null);

        nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 4);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(2));

        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(6));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(3, 7));

        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0, 4));
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(1).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(1).getDonorId(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(1, 5));
    }

    /**
     * Tests Zone Routing -> Zone Routing with no replication factor change
     */
    @Test
    public void testZoneToZone() {
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        StoreDefinition storeDefRepFactor1 = ServerTestUtils.getStoreDef("zone_store",
                                                                         2,
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         0,
                                                                         0,
                                                                         zoneReplicationFactors,
                                                                         HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                         RoutingStrategyType.ZONE_STRATEGY);
        RebalanceClusterPlan plan = new RebalanceClusterPlan(zoneRoutingCluster,
                                                             zoneRoutingClusterModified,
                                                             Lists.newArrayList(storeDefRepFactor1),
                                                             false,
                                                             null);

        HashMap<Integer, RebalanceNodePlan> nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().size(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(6));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(2).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(2).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));

        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        StoreDefinition storeDefRepFactor2 = ServerTestUtils.getStoreDef("zone_store",
                                                                         4,
                                                                         1,
                                                                         1,
                                                                         1,
                                                                         0,
                                                                         0,
                                                                         zoneReplicationFactors,
                                                                         HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                         RoutingStrategyType.ZONE_STRATEGY);
        plan = new RebalanceClusterPlan(zoneRoutingCluster,
                                        zoneRoutingClusterModified,
                                        Lists.newArrayList(storeDefRepFactor2),
                                        false,
                                        null);

        nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 2);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().size(), 3);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getDonorId(), 1);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(5));
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(1).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(6));
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(2).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(2).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));
    }

    /**
     * Tests Consistent Routing -> Zone Routing with no replication factor
     * change
     */
    @Test
    public void testConsistentToZoneWithRepFactorChange() {
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        StoreDefinition beforeStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        StoreDefinition afterStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                                    2,
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    0,
                                                                    0,
                                                                    zoneReplicationFactors,
                                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                    RoutingStrategyType.ZONE_STRATEGY);

        RebalanceClusterPlan plan = new RebalanceClusterPlan(consistentRoutingCluster,
                                                             zoneRoutingClusterModified,
                                                             Lists.newArrayList(beforeStoreDef),
                                                             Lists.newArrayList(afterStoreDef),
                                                             false,
                                                             null);

        HashMap<Integer, RebalanceNodePlan> nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 4);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(2));
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(1).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(3));

        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().size(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(6));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(2).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(2).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(4));
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(1).getDonorId(), 1);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(1, 5));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0));

        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        beforeStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                     2,
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                    4,
                                                    1,
                                                    1,
                                                    1,
                                                    0,
                                                    0,
                                                    zoneReplicationFactors,
                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                    RoutingStrategyType.ZONE_STRATEGY);

        plan = new RebalanceClusterPlan(consistentRoutingCluster,
                                        zoneRoutingClusterModified,
                                        Lists.newArrayList(beforeStoreDef),
                                        Lists.newArrayList(afterStoreDef),
                                        false,
                                        null);

        nodePlan = plan.getRebalancingTaskQueuePerNode();
        Assert.assertEquals(nodePlan.size(), 4);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().size(), 3);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getDonorId(), 1);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(1, 5));
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(1).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(2, 6));
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(2).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(0).getRebalanceTaskList().get(2).getPartitionList(),
                            Lists.newArrayList(7));

        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getDonorId(), 2);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(2, 6));
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(1).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(3, 7));

        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0, 4));
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(1).getDonorId(), 3);
        Assert.assertEquals(nodePlan.get(2).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(3, 7));

        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().size(), 2);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getDonorId(), 0);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(0).getPartitionList(),
                            Lists.newArrayList(0, 4));
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(1).getDonorId(), 1);
        Assert.assertEquals(nodePlan.get(3).getRebalanceTaskList().get(1).getPartitionList(),
                            Lists.newArrayList(1, 5));
    }

}
