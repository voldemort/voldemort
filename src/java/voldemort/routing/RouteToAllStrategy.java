package voldemort.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import voldemort.cluster.Node;

/**
 * A routing strategy which just routes each request to all the nodes given
 * 
 * @author jay
 * 
 */
public class RouteToAllStrategy implements RoutingStrategy {

    private Collection<Node> nodes;

    public RouteToAllStrategy(Collection<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Node> routeRequest(byte[] key) {
        return new ArrayList<Node>(nodes);
    }

    public Set<Node> getNodes() {
        return new HashSet<Node>(nodes);
    }

}
