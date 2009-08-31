package voldemort;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.nodeavailabilitydetector.NodeAvailabilityDetector;
import voldemort.cluster.nodeavailabilitydetector.NodeAvailabilityDetectorUtils;
import voldemort.store.Store;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * A simple test store client factory that returns stores from a predetermined
 * list.
 * 
 * If the factory is supplied with N stores then the first N calls to
 * getRawStore will return the stores in succession. After that all calls will
 * return the Nth store.
 * 
 * This is intended to help simultate things like MetadataExceptions which may
 * require having an out-of-date Store.
 * 
 * @author jay
 * 
 */
public class StaticStoreClientFactory implements StoreClientFactory {

    private final AtomicInteger current;
    private final List<Store<?, ?>> stores;
    private final NodeAvailabilityDetector nodeAvailabilityDetector;

    public StaticStoreClientFactory(Store<?, ?>... stores) {
        if(stores.length < 1)
            throw new IllegalArgumentException("Must provide at least one store.");
        this.stores = Arrays.asList(stores);
        current = new AtomicInteger(0);
        nodeAvailabilityDetector = NodeAvailabilityDetectorUtils.create(new ClientConfig());
    }

    @SuppressWarnings("unchecked")
    public <K, V> Store<K, V> getRawStore(String storeName,
                                          InconsistencyResolver<Versioned<V>> resolver) {
        return (Store<K, V>) stores.get(Math.max(current.getAndIncrement(), stores.size() - 1));
    }

    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        return new DefaultStoreClient(storeName, null, this, 3);
    }

    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {
        return new DefaultStoreClient(storeName, resolver, this, 3);
    }

    public void close() {
        for(Store<?, ?> store: stores)
            store.close();
    }

    public NodeAvailabilityDetector getNodeAvailabilityDetector() {
        return nodeAvailabilityDetector;
    }

}
