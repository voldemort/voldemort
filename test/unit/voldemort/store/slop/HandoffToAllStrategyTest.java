package voldemort.store.slop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.List;

import static org.junit.Assert.*;

public class HandoffToAllStrategyTest {

    @Test
    public void testRouteHint() {
        Cluster cluster = VoldemortTestConstants.getNineNodeCluster();
        HintedHandoffStrategy handoffStrategy = new HandoffToAllStrategy(cluster, false, 0);
        for(Node origin: cluster.getNodes()) {
            List<Node> nodes = handoffStrategy.routeHint(origin);
            assertTrue("hint preflist is correctly sized", nodes.size() == cluster.getNumberOfNodes() - 1);
            assertFalse("hint preflist doesn't include self", nodes.contains(origin));
        }
    }

    @Test
    public void testRouteHintWithZones() {
        Cluster cluster = VoldemortTestConstants.getEightNodeClusterWithZones();
        HintedHandoffStrategy handoffStrategy0 = new HandoffToAllStrategy(cluster, true, 0);
        HintedHandoffStrategy handoffStrategy1 = new HandoffToAllStrategy(cluster, true, 1);

        List<Node> zone0Nodes = Lists.newArrayList();
        List<Node> zone1Nodes = Lists.newArrayList();
        for(int nodeId: ImmutableList.of(0, 1, 2, 3))
            zone0Nodes.add(cluster.getNodeById(nodeId));
        for(int nodeId: ImmutableList.of(4, 5, 6, 7))
            zone1Nodes.add(cluster.getNodeById(nodeId));

        for(Node origin: zone0Nodes) {
            List<Node> nodes = handoffStrategy0.routeHint(origin);
            assertFalse("hint preflist doesn't include self", nodes.contains(origin));
            for(Node node: nodes)
                assertFalse("local hints not routed remotely", zone1Nodes.contains(node));
            nodes = handoffStrategy1.routeHint(origin);
            assertFalse("hint preflist doesn't include self", nodes.contains(origin));
            for(Node node: nodes)
                assertTrue("remote hints routed locally", zone1Nodes.contains(node));
        }

        for(Node origin: zone1Nodes) {
            List<Node> nodes = handoffStrategy1.routeHint(origin);
            assertFalse("hint preflist doesn't include self", nodes.contains(origin));
            for(Node node: nodes)
                assertFalse("local hints not routed remotely", zone0Nodes.contains(node));
            nodes = handoffStrategy0.routeHint(origin);
            assertFalse("hint preflist doesn't include self", nodes.contains(origin));
            for(Node node: nodes)
                assertTrue("remote hints routed locally", zone0Nodes.contains(node));
        }
    }
}
