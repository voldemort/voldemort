package voldemort.rest.coordinator.config;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import voldemort.common.service.ServiceType;

/**
 * Singleton containing APIs for interacting reading and writing client store
 * configs
 */
public abstract class StoreClientConfigService {

    protected CoordinatorConfig coordinatorConfig = null;
    protected final Map<String, StoreClientConfigServiceListener> storeClientConfigListeners;

    // Keys for passing messages back to the admin client
    public static final String SUCCESS_MESSAGE_PARAM_KEY = "success_message";
    public static final String WARNING_MESSAGE_PARAM_KEY = "warning_message";
    public static final String ERROR_MESSAGE_PARAM_KEY = "error_message";

    // Success strings
    public static final String STORE_CREATED_SUCCESS = "A new config for this store was successfully created.";
    public static final String STORE_UPDATED_SUCCESS = "The config for this store was successfully updated.";
    public static final String STORE_DELETED_SUCCESS = "The config for this store was successfully deleted.";

    // Warning strings
    public static final String STORE_UNCHANGED_WARNING = "The config for this store was not changed, since it was already in the requested state.";
    public static final String STORE_ALREADY_DOES_NOT_EXIST_WARNING = "This store already does not exist.";

    // Error strings
    public static final String STORE_NOT_FOUND_ERROR = "This store is not currently defined in the Coordinator config.";
    public static final String INVALID_CONFIG_PARAMS_ERROR = "The config for this store was not changed, since it included invalid configuration parameters:";
    public static final String INVALID_PARAM_KEY_ERROR = "is not a valid config key";
    public static final String PARAM_VALUE_MUST_BE_INTEGER_ERROR = "must be an integer";
    public static final String PARAM_VALUE_MUST_BE_POSITIVE_ERROR = "must be positive";
    public static final String BOOTSTRAP_OPERATION_FAILED_ERROR = "Failed to bootstrap. Check if Voldemort serves this store.";

    protected static final Properties STORE_NOT_DELETED_PROPS = new Properties();
    protected static final Properties STORE_NOT_UPDATED_PROPS = new Properties();
    protected static final Properties STORE_NOT_CREATED_PROPS = new Properties();
    protected static final Properties STORE_NOT_SERVED_BY_VOLDEMORT_PROPS = new Properties();
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
        STORE_ALREADY_DOES_NOT_EXIST_PROPS.put(WARNING_MESSAGE_PARAM_KEY,
                                               STORE_ALREADY_DOES_NOT_EXIST_WARNING);

        STORE_NOT_FOUND_PROPS.put(ERROR_MESSAGE_PARAM_KEY, STORE_NOT_FOUND_ERROR);
        STORE_NOT_SERVED_BY_VOLDEMORT_PROPS.put(ERROR_MESSAGE_PARAM_KEY,
                                                BOOTSTRAP_OPERATION_FAILED_ERROR);
    }

    protected StoreClientConfigService(CoordinatorConfig coordinatorConfig) {
        this.coordinatorConfig = coordinatorConfig;
        storeClientConfigListeners = new ConcurrentHashMap<String, StoreClientConfigServiceListener>();
    }

    public String getAllConfigs() {
        return ClientConfigUtil.writeMultipleClientConfigAvro(this.getAllConfigsMap());
    }

    public abstract Map<String, Properties> getAllConfigsMap();

    public String getSpecificConfigs(List<String> storeNames) {
        return ClientConfigUtil.writeMultipleClientConfigAvro(this.getSpecificConfigsMap(storeNames));
    }

    protected abstract Map<String, Properties> getSpecificConfigsMap(List<String> storeNames);

    public String putConfigs(Map<String, Properties> configs) {
        return ClientConfigUtil.writeMultipleClientConfigAvro(this.putConfigsMap(configs));
    }

    protected abstract Map<String, Properties> putConfigsMap(Map<String, Properties> configs);

    public String deleteSpecificConfigs(List<String> storeNames) {
        return ClientConfigUtil.writeMultipleClientConfigAvro(this.deleteSpecificConfigsMap(storeNames));
    }

    protected abstract Map<String, Properties> deleteSpecificConfigsMap(List<String> storeNames);

    public void registerListener(ServiceType serviceType,
                                 StoreClientConfigServiceListener storeClientConfigServiceListener) {
        // Assumes a single service can only have one listener any time and is
        // registered on startup
        this.storeClientConfigListeners.put(serviceType.getDisplayName(),
                                            storeClientConfigServiceListener);

    }

}
