package voldemort.store.routed;

import java.util.Map;

import voldemort.routing.RoutingStrategy;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

public interface RoutableStore extends Store<ByteArray, byte[]> {

    public void updateRoutingStrategy(RoutingStrategy routingStrategy);

    public Map<Integer, Store<ByteArray, byte[]>> getInnerStores();

}
