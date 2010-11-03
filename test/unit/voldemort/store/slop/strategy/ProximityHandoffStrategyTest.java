package voldemort.store.slop.strategy;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;

public class ProximityHandoffStrategyTest {

    @Test
    public void testTwoZones() {
        Cluster cluster = VoldemortTestConstants.getEightNodeClusterWithZones();
        ProximityHandoffStrategy handoffStrategy = new ProximityHandoffStrategy(cluster);

        List<Node> zone0Nodes = Lists.newArrayList();
        List<Node> zone1Nodes = Lists.newArrayList();
        for(Node node: cluster.getNodes()) {
            if(node.getZoneId() == 0) {
                zone0Nodes.add(node);
            } else if(node.getZoneId() == 1) {
                zone1Nodes.add(node);
            }
        }

        // Try with node from zone 0
        for(Node zone0Node: zone0Nodes) {
            List<Node> prefList = handoffStrategy.routeHint(zone0Node);
            for(int i = 0; i < prefList.size(); i++) {
                if(i < zone0Nodes.size()) {
                    assertEquals(prefList.get(i).getZoneId(), 0);
                } else {
                    assertEquals(prefList.get(i).getZoneId(), 1);
                }
            }
        }

        // Try with node from zone 1
        for(Node zone1Node: zone1Nodes) {
            List<Node> prefList = handoffStrategy.routeHint(zone1Node);
            for(int i = 0; i < prefList.size(); i++) {
                if(i < zone1Nodes.size()) {
                    assertEquals(prefList.get(i).getZoneId(), 1);
                } else {
                    assertEquals(prefList.get(i).getZoneId(), 0);
                }
            }
        }
    }

}
