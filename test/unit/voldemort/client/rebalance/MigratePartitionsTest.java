package voldemort.client.rebalance;

import java.util.HashMap;
import java.util.LinkedList;
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

public class MigratePartitionsTest {

    private final int NUM_NODES = 4;
    private final int NUM_ZONES = 2;

    private Cluster consistentRoutingCluster;
    private Cluster zoneRoutingClusterModified;
    private StoreDefinition beforeStoreDef;
    private StoreDefinition afterStoreDef;

    @Before
    public void setUp() {
        List<Node> nodes = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            nodes.add(new Node(i, "node" + i, 100, 200, 300, Lists.newArrayList(i, i + NUM_NODES)));
        }
        consistentRoutingCluster = new Cluster("consistent", nodes);

        List<Zone> zones = Lists.newArrayList();
        for(int i = 0; i < NUM_ZONES; i++) {
            LinkedList<Integer> proximityList = Lists.newLinkedList();
            int zoneId = i + 1;
            for(int j = 0; j < NUM_ZONES; j++) {
                proximityList.add(zoneId % NUM_ZONES);
                zoneId++;
            }
            zones.add(new Zone(i, proximityList));
        }

        nodes = Lists.newArrayList();
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
                nodes.add(new Node(i, "node" + i, 100, 200, 300, 0, partitions));
            else
                nodes.add(new Node(i, "node" + i, 100, 200, 300, 1, partitions));
        }
        zoneRoutingClusterModified = new Cluster("zone", nodes, zones);

        HashMap<Integer, Integer> zoneReplicationFactors = Maps.newHashMap();

        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        beforeStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                    2,
                                                    1,
                                                    1,
                                                    1,
                                                    0,
                                                    0,
                                                    zoneReplicationFactors,
                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                    RoutingStrategyType.ZONE_STRATEGY);

    }

    /**
     * To test whether we are generating the correct donor node plans from the
     * stealer node plans generated from RebalanceClusterPlan
     * 
     */
    @Test
    public void testDonorNodePlanGeneration() {

        // Stealer node - 0
        MigratePartitions tool = new MigratePartitions(consistentRoutingCluster,
                                                       zoneRoutingClusterModified,
                                                       Lists.newArrayList(beforeStoreDef),
                                                       Lists.newArrayList(afterStoreDef),
                                                       null,
                                                       null,
                                                       Lists.newArrayList(0));
        HashMap<Integer, List<RebalancePartitionsInfo>> donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 2);
        Assert.assertEquals(donorNodePlans.get(2).size(), 1);
        Assert.assertEquals(donorNodePlans.get(3).size(), 1);

        // Stealer node - 0, 1
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     null,
                                     null,
                                     Lists.newArrayList(0, 1));
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 3);
        Assert.assertEquals(donorNodePlans.get(0).size(), 1);
        Assert.assertEquals(donorNodePlans.get(2).size(), 2);
        Assert.assertEquals(donorNodePlans.get(3).size(), 2);

        // Stealer node - 0, 1, -1
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     null,
                                     null,
                                     Lists.newArrayList(0, 1, -1));
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 3);
        Assert.assertEquals(donorNodePlans.get(0).size(), 1);
        Assert.assertEquals(donorNodePlans.get(2).size(), 2);
        Assert.assertEquals(donorNodePlans.get(3).size(), 2);

        // Stealer node - 0, 1, 2
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     null,
                                     null,
                                     Lists.newArrayList(0, 1, 2));
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 4);
        Assert.assertEquals(donorNodePlans.get(0).size(), 2);
        Assert.assertEquals(donorNodePlans.get(1).size(), 1);
        Assert.assertEquals(donorNodePlans.get(2).size(), 2);
        Assert.assertEquals(donorNodePlans.get(3).size(), 2);

        // Stealer node - 0, 1, 2, 3
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     null,
                                     null,
                                     Lists.newArrayList(0, 1, 2, 3));
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 4);
        Assert.assertEquals(donorNodePlans.get(0).size(), 3);
        Assert.assertEquals(donorNodePlans.get(1).size(), 1);
        Assert.assertEquals(donorNodePlans.get(2).size(), 2);
        Assert.assertEquals(donorNodePlans.get(3).size(), 2);

        // Stealer node - null
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     null,
                                     null,
                                     null);
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 4);
        Assert.assertEquals(donorNodePlans.get(0).size(), 3);
        Assert.assertEquals(donorNodePlans.get(1).size(), 1);
        Assert.assertEquals(donorNodePlans.get(2).size(), 2);
        Assert.assertEquals(donorNodePlans.get(3).size(), 2);
    }

}
