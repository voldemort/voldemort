package voldemort.store.rebalancing;

import java.util.HashMap;
import java.util.Map;

import voldemort.store.metadata.MetadataStore;

public class ProxyMetadataListenerManager {

    static Map<String, ProxyMetadataListener> storeToListeners = new HashMap<String, ProxyMetadataListener>();

    static public void addListener(String storeName, MetadataStore metadata) {
        ProxyMetadataListener listener = new ProxyMetadataListener(storeName);
        metadata.addMetadataStoreListener(storeName, listener);
        storeToListeners.put(storeName, listener);
    }

    static public ProxyMetadataListener getListenerForStore(String storeName) {
        return storeToListeners.get(storeName);
    }
}
