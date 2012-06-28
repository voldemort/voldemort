package voldemort.client;

import java.util.concurrent.ConcurrentHashMap;

import voldemort.store.system.SystemStoreConstants;

/*
 * A repository that creates and maintains all the system stores in one place.
 * The purpose is to act as a source of truth for all the system stores, since
 * they can be recreated dynamically (in case cluster.xml changes).
 */

public class SystemStoreRepository {

    private ConcurrentHashMap<String, SystemStore> sysStoreMap;

    public SystemStoreRepository() {
        sysStoreMap = new ConcurrentHashMap<String, SystemStore>();
    }

    public void createSystemStores(ClientConfig config, String clusterXml) {
        for(SystemStoreConstants.SystemStoreName storeName: SystemStoreConstants.SystemStoreName.values()) {
            SystemStore<String, Long> sysVersionStore = new SystemStore<String, Long>(storeName.name(),
                                                                                      config.getBootstrapUrls(),
                                                                                      config.getClientZoneId(),
                                                                                      clusterXml);
            this.sysStoreMap.put(storeName.name(), sysVersionStore);
        }
    }

    public SystemStore<String, Long> getVersionStore() {
        String name = SystemStoreConstants.SystemStoreName.voldsys$_metadata_version.name();
        SystemStore<String, Long> sysVersionStore = sysStoreMap.get(name);
        return sysVersionStore;
    }

    public SystemStore<String, ClientInfo> getClientRegistryStore() {
        String name = SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name();
        SystemStore<String, ClientInfo> sysRegistryStore = sysStoreMap.get(name);
        return sysRegistryStore;
    }
}
