package voldemort.store.metadata;

import java.util.List;

import voldemort.routing.RoutingStrategy;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;

public interface MetadataStoreListener {

    void updateRoutingStrategy(RoutingStrategy routingStrategyMap);

    void updateStoreDefinition(StoreDefinition storeDef);

    void updateMetadataKeys(List<Pair<String, Object>> metadataKeyValueList);
}
