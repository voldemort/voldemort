package voldemort.store.metadata;

import voldemort.routing.RoutingStrategy;
import voldemort.store.StoreDefinition;

public interface MetadataStoreListener {

    void updateRoutingStrategy(RoutingStrategy routingStrategyMap);

    void updateStoreDefinition(StoreDefinition storeDef);
}
