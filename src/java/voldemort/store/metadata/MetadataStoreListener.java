package voldemort.store.metadata;

import java.util.Map;

import voldemort.routing.RoutingStrategy;

public interface MetadataStoreListener {

    void updateRoutingStrategy(Map<String, RoutingStrategy> routingStrategyMap);
}
