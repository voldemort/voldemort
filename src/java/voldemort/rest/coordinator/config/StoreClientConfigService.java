package voldemort.rest.coordinator.config;

import java.util.List;

/**
 * Singleton containing APIs for interacting reading and writing client store configs
 */
public abstract class StoreClientConfigService {
    private static StoreClientConfigService singleton = null;
    protected CoordinatorConfig coordinatorConfig;

    protected StoreClientConfigService(CoordinatorConfig coordinatorConfig) {
        this.coordinatorConfig = coordinatorConfig;
    }

    public static synchronized void initialize(CoordinatorConfig coordinatorConfig) {
        if (singleton == null) {
            switch(coordinatorConfig.getFatClientConfigSource()){
                case FILE:
                    singleton = new FileBasedStoreClientConfigService(coordinatorConfig);
                    break;
                case ZOOKEEPER:
                    throw new UnsupportedOperationException("Zookeeper-based configs are not implemented yet!");
            }
        } else {
            // Redundant init... No big deal?
        }
    }

    private static void checkInit() {
        if (singleton == null) {
            throw new IllegalStateException("StoreClientConfigService was accessed before being initialized!");
        }
    }

    public static String getAllConfigs() {
        checkInit();
        return singleton.getAllConfigsImpl();
    }
    protected abstract String getAllConfigsImpl();

    public static String getSpecificConfigs(List<String> storeNames) {
        checkInit();
        return singleton.getSpecificConfigsImpl(storeNames);
    }
    protected abstract String getSpecificConfigsImpl(List<String> storeNames);


}
