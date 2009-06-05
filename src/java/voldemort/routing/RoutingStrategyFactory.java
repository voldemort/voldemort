package voldemort.routing;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;

/**
 * A factory that gets the appropriate {@link RoutingStrategy} for a given
 * {@link RoutingStrategyType}.
 * 
 * @author bbansal
 * 
 */
public class RoutingStrategyFactory {

    private final Cluster cluster;

    public RoutingStrategyFactory(Cluster cluster) {
        this.cluster = cluster;
    }

    public RoutingStrategy getRoutingStrategy(StoreDefinition storeDef) {
        if(RoutingStrategyType.CONSISTENT_STRATEGY.equals(storeDef.getRoutingStrategyType())) {
            return new ConsistentRoutingStrategy(cluster.getNodes(),
                                                 storeDef.getReplicationFactor());
        } else if(RoutingStrategyType.TO_ALL_STRATEGY.equals(storeDef.getRoutingStrategyType())) {
            return new RouteToAllStrategy(cluster.getNodes());
        } else {
            throw new VoldemortException("RoutingStrategyType:" + storeDef.getRoutingStrategyType()
                                         + " not handled by " + this.getClass());
        }
    }
}
