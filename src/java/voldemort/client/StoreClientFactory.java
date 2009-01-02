package voldemort.client;

import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * An abstraction the represents a connection to a Voldemort cluster and can be
 * used to create StoreClients to interact with individual stores
 * 
 * @author jay
 * 
 */
public interface StoreClientFactory {

    public <K, V> StoreClient<K, V> getStoreClient(String storeName);

    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> inconsistencyResolver);

}