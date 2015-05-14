package voldemort.rest.coordinator.config;

import java.util.Properties;

/**
 * This interface allows a class to register to store config changes and be notified when they happen.
 */
public interface StoreClientConfigServiceListener {

    /**
     * When a new store is added or an existing store is updated, this function will be called to notify implementers
     * of this interface.
     *
     * @param storeName The name of the store that was added or updated
     * @param storeClientProps The non-default configurations of that store
     */
    public void onStoreConfigAddOrUpdate(String storeName, Properties storeClientProps);

    /**
     * When a store is delete, this function will be called to notify implementers of this interface.
     * @param storeName The name of the store that was deleted
     */
    public void onStoreConfigDelete(String storeName);

}
