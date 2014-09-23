package voldemort.rest.coordinator.config;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Singleton containing APIs for interacting reading and writing client store configs
 */
public abstract class StoreClientConfigService {
    private static StoreClientConfigService singleton = null;
    protected CoordinatorConfig coordinatorConfig;

    // Keys for passing messages back to the admin client
    public static final String SUCCESS_MESSAGE_PARAM_KEY = "success_message";
    public static final String WARNING_MESSAGE_PARAM_KEY = "warning_message";
    public static final String ERROR_MESSAGE_PARAM_KEY = "error_message";

    // Success strings
    public static final String STORE_CREATED_SUCCESS = "A new config for this store was successfully created.";
    public static final String STORE_UPDATED_SUCCESS = "The config for this store was successfully updated.";
    public static final String STORE_DELETED_SUCCESS = "The config for this store was successfully deleted.";

    // Warning strings
    public static final String STORE_UNCHANGED_WARNING =
            "The config for this store was not changed, since it was already in the requested state.";
    public static final String STORE_ALREADY_DOES_NOT_EXIST_WARNING = "This store already does not exist.";

    // Error strings
    public static final String STORE_NOT_FOUND_ERROR = "This store is not currently defined in the Coordinator config.";
    public static final String INVALID_CONFIG_PARAMS_ERROR =
            "The config for this store was not changed, since it included invalid configuration parameters:";
    public static final String INVALID_PARAM_KEY_ERROR = "is not a valid config key";
    public static final String PARAM_VALUE_MUST_BE_INTEGER_ERROR = "must be an integer";
    public static final String PARAM_VALUE_MUST_BE_POSITIVE_ERROR = "must be positive";

    protected static final Properties STORE_NOT_FOUND_PROPS = new Properties();
    protected static final Properties STORE_UNCHANGED_PROPS = new Properties();
    protected static final Properties STORE_CREATED_PROPS = new Properties();
    protected static final Properties STORE_UPDATED_PROPS = new Properties();
    protected static final Properties STORE_DELETED_PROPS = new Properties();
    protected static final Properties STORE_ALREADY_DOES_NOT_EXIST_PROPS = new Properties();
    static {
        STORE_CREATED_PROPS.put(SUCCESS_MESSAGE_PARAM_KEY, STORE_CREATED_SUCCESS);
        STORE_UPDATED_PROPS.put(SUCCESS_MESSAGE_PARAM_KEY, STORE_UPDATED_SUCCESS);
        STORE_DELETED_PROPS.put(SUCCESS_MESSAGE_PARAM_KEY, STORE_DELETED_SUCCESS);

        STORE_UNCHANGED_PROPS.put(WARNING_MESSAGE_PARAM_KEY, STORE_UNCHANGED_WARNING);
        STORE_ALREADY_DOES_NOT_EXIST_PROPS.put(WARNING_MESSAGE_PARAM_KEY, STORE_ALREADY_DOES_NOT_EXIST_WARNING);

        STORE_NOT_FOUND_PROPS.put(ERROR_MESSAGE_PARAM_KEY, STORE_NOT_FOUND_ERROR);
    }

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
        return ClientConfigUtil.writeMultipleClientConfigAvro(singleton.getAllConfigsImpl());
    }
    public static Map<String, Properties> getAllConfigsMap() {
        checkInit();
        return singleton.getAllConfigsImpl();
    }
    protected abstract Map<String, Properties> getAllConfigsImpl();

    public static String getSpecificConfigs(List<String> storeNames) {
        checkInit();
        return ClientConfigUtil.writeMultipleClientConfigAvro(singleton.getSpecificConfigsImpl(storeNames));
    }
    protected abstract Map<String, Properties> getSpecificConfigsImpl(List<String> storeNames);

    public static String putConfigs(Map<String, Properties> configs) {
        checkInit();
        return ClientConfigUtil.writeMultipleClientConfigAvro(singleton.putConfigsImpl(configs));
    }
    protected abstract Map<String, Properties> putConfigsImpl(Map<String, Properties> configs);

    public static String deleteSpecificConfigs(List<String> storeNames) {
        checkInit();
        return ClientConfigUtil.writeMultipleClientConfigAvro(singleton.deleteSpecificConfigsImpl(storeNames));
    }
    protected abstract Map<String, Properties> deleteSpecificConfigsImpl(List<String> storeNames);

}
