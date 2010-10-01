package voldemort.store.metadata;

import voldemort.routing.RoutingStrategy;

public interface MetadataStoreListener {

    void updateRoutingStrategy(RoutingStrategy routingStrategyMap);
}
