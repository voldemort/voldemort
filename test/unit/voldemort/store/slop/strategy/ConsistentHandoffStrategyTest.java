package voldemort.store.slop.strategy;

import org.junit.Test;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ConsistentHandoffStrategyTest {

    @Test
    public void testRouteHint() {
        Node node0 = new Node(0,
                              "host",
                              1,
                              1,
                              1,
                              Arrays.asList(0));
        Node node1 = new Node(1,
                              "host",
                              1,
                              1,
                              1,
                              Arrays.asList(1));
        Node node2 = new Node(2,
                              "host",
                              1,
                              1,
                              1,
                              Arrays.asList(2));
        Cluster cluster = new Cluster("cluster",
                                      Arrays.asList(node0, node1, node2));
        ConsistentHandoffStrategy strategy = new ConsistentHandoffStrategy(cluster,
                                                                           2,
                                                                           false,
                                                                           0);
        List<Node> hintedToNodes = strategy.routeHint(node0);
        for(Node hintedToNode: hintedToNodes) {
            assertTrue("hints shouldn't be routed to original node",
                       hintedToNode.getId() != node0.getId());
        }
    }
}
