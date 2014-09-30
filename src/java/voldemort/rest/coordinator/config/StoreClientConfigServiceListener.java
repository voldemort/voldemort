package voldemort.rest.coordinator.config;

import java.util.Properties;

public interface StoreClientConfigServiceListener {

    public void onStoreConfigAddOrUpdate(String storeName, Properties storeClientProps);

    public void onStoreConfigDelte(String storeName);

}
