package voldemort.routing;

import java.util.Collection;

import voldemort.cluster.Node;

public class RouteToAllLocalPrefStrategy extends RouteToAllStrategy {

    public RouteToAllLocalPrefStrategy(Collection<Node> nodes) {
        super(nodes);
    }

    @Override
    public String getType() {
        return RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY;
    }
}
