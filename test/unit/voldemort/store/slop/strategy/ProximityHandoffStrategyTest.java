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
        List<Node> zone0Nodes = Lists.newArrayList();
        List<Node> zone1Nodes = Lists.newArrayList();
        for(Node node: cluster.getNodes()) {
            if(node.getZoneId() == 0) {
                zone0Nodes.add(node);
            } else if(node.getZoneId() == 1) {
                zone1Nodes.add(node);
            }
        }

        ProximityHandoffStrategy handoffStrategy = new ProximityHandoffStrategy(cluster, 0);

        for(Node node: cluster.getNodes()) {
            List<Node> prefList = handoffStrategy.routeHint(node);
            for(int i = 0; i < prefList.size(); i++) {
                if(node.getZoneId() == 0) {
                    if(i < zone0Nodes.size() - 1) {
                        assertEquals(prefList.get(i).getZoneId(), 0);
                    } else {
                        assertEquals(prefList.get(i).getZoneId(), 1);
                    }
                } else {
                    if(i < zone0Nodes.size()) {
                        assertEquals(prefList.get(i).getZoneId(), 0);
                    } else {
                        assertEquals(prefList.get(i).getZoneId(), 1);
                    }
                }

            }
        }

        handoffStrategy = new ProximityHandoffStrategy(cluster, 1);

        for(Node node: cluster.getNodes()) {
            List<Node> prefList = handoffStrategy.routeHint(node);
            for(int i = 0; i < prefList.size(); i++) {
                if(node.getZoneId() == 0) {
                    if(i < zone0Nodes.size()) {
                        assertEquals(prefList.get(i).getZoneId(), 1);
                    } else {
                        assertEquals(prefList.get(i).getZoneId(), 0);
                    }
                } else {
                    if(i < zone0Nodes.size() - 1) {
                        assertEquals(prefList.get(i).getZoneId(), 1);
                    } else {
                        assertEquals(prefList.get(i).getZoneId(), 0);
                    }
                }

            }
        }

    }

}
