package voldemort.store.rebalancing;

import java.util.HashMap;
import java.util.Map;

import voldemort.store.metadata.MetadataStore;

public class ProxyPutMetadataListenerManager {

    static Map<String, ProxyPutMetadataListener> storeToListeners = new HashMap<String, ProxyPutMetadataListener>();

    static public void addListener(String storeName, MetadataStore metadata) {
        ProxyPutMetadataListener listener = new ProxyPutMetadataListener(storeName);
        metadata.addMetadataStoreListener(storeName, listener);
        storeToListeners.put(storeName, listener);
    }

    static public ProxyPutMetadataListener getListenerForStore(String storeName) {
        return storeToListeners.get(storeName);
    }
}
