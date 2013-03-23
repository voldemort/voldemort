package voldemort.coordinator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import voldemort.utils.ConfigurationException;
import voldemort.utils.Props;
import voldemort.utils.Utils;

public class CoordinatorConfig {

    private volatile List<String> bootstrapURLs = null;
    private volatile String fatClientConfigPath = null;
    private volatile int fatClientWrapperMaxPoolSize = 20;
    private volatile int fatClientWrapperCorePoolSize = 20;
    private volatile int fatClientWrapperKeepAliveInSecs = 60;
    private volatile int metadataCheckIntervalInMs = 5000;

    /* Propery names for propery-based configuration */
    public static final String BOOTSTRAP_URLS_PROPERTY = "bootstrap_urls";
    public static final String FAT_CLIENTS_CONFIG_FILE_PATH_PROPERTY = "fat_clients_config_file_path";
    public static final String FAT_CLIENT_WRAPPER_MAX_POOL_SIZE_PROPERTY = "fat_client_wrapper_max_pool_size";
    public static final String FAT_CLIENT_WRAPPER_CORE_POOL_SIZE_PROPERTY = "fat_client_wrapper_core_pool_size";
    public static final String FAT_CLIENT_WRAPPER_POOL_KEEPALIVE_IN_SECS = "fat_client_wrapper_pool_keepalive_in_secs";
    public static final String METADATA_CHECK_INTERVAL_IN_MS = "metadata_check_interval_in_ms";

    /**
     * Instantiate the coordinator config using a properties file
     * 
     * @param propertyFile Properties file
     */
    public CoordinatorConfig(File propertyFile) {
        Properties properties = new Properties();
        InputStream input = null;
        try {
            input = new BufferedInputStream(new FileInputStream(propertyFile.getAbsolutePath()));
            properties.load(input);
        } catch(IOException e) {
            throw new ConfigurationException(e);
        } finally {
            IOUtils.closeQuietly(input);
        }
        setProperties(properties);
    }

    /**
     * Initiate the coordinator config from a set of properties. This is useful
     * for wiring from Spring or for externalizing client properties to a
     * properties file
     * 
     * @param properties The properties to use
     */
    public CoordinatorConfig(Properties properties) {
        setProperties(properties);
    }

    private void setProperties(Properties properties) {
        Props props = new Props(properties);
        if(props.containsKey(BOOTSTRAP_URLS_PROPERTY)) {
            setBootstrapURLs(props.getList(BOOTSTRAP_URLS_PROPERTY));
        }

        if(props.containsKey(FAT_CLIENTS_CONFIG_FILE_PATH_PROPERTY)) {
            setFatClientConfigPath(props.getString(FAT_CLIENTS_CONFIG_FILE_PATH_PROPERTY));
        }

        if(props.containsKey(FAT_CLIENT_WRAPPER_CORE_POOL_SIZE_PROPERTY)) {
            setFatClientWrapperCorePoolSize(props.getInt(FAT_CLIENT_WRAPPER_CORE_POOL_SIZE_PROPERTY,
                                                         this.fatClientWrapperCorePoolSize));
        }

        if(props.containsKey(FAT_CLIENT_WRAPPER_MAX_POOL_SIZE_PROPERTY)) {
            setFatClientWrapperMaxPoolSize(props.getInt(FAT_CLIENT_WRAPPER_MAX_POOL_SIZE_PROPERTY,
                                                        this.fatClientWrapperMaxPoolSize));
        }

        if(props.containsKey(FAT_CLIENT_WRAPPER_POOL_KEEPALIVE_IN_SECS)) {
            setFatClientWrapperKeepAliveInSecs(props.getInt(FAT_CLIENT_WRAPPER_POOL_KEEPALIVE_IN_SECS,
                                                            this.fatClientWrapperKeepAliveInSecs));
        }

        if(props.containsKey(METADATA_CHECK_INTERVAL_IN_MS)) {
            setMetadataCheckIntervalInMs(props.getInt(METADATA_CHECK_INTERVAL_IN_MS,
                                                      this.metadataCheckIntervalInMs));
        }
    }

    public String[] getBootstrapURLs() {
        if(this.bootstrapURLs == null)
            throw new IllegalStateException("No bootstrap urls have been set.");
        return this.bootstrapURLs.toArray(new String[this.bootstrapURLs.size()]);
    }

    public CoordinatorConfig setBootstrapURLs(List<String> bootstrapUrls) {
        this.bootstrapURLs = Utils.notNull(bootstrapUrls);
        if(this.bootstrapURLs.size() <= 0)
            throw new IllegalArgumentException("Must provide at least one bootstrap URL.");
        return this;
    }

    public String getFatClientConfigPath() {
        return fatClientConfigPath;
    }

    public void setFatClientConfigPath(String fatClientConfigPath) {
        this.fatClientConfigPath = fatClientConfigPath;
    }

    public int getFatClientWrapperMaxPoolSize() {
        return fatClientWrapperMaxPoolSize;
    }

    public void setFatClientWrapperMaxPoolSize(int fatClientWrapperMaxPoolSize) {
        this.fatClientWrapperMaxPoolSize = fatClientWrapperMaxPoolSize;
    }

    public int getFatClientWrapperCorePoolSize() {
        return fatClientWrapperCorePoolSize;
    }

    public void setFatClientWrapperCorePoolSize(int fatClientWrapperCorePoolSize) {
        this.fatClientWrapperCorePoolSize = fatClientWrapperCorePoolSize;
    }

    public int getFatClientWrapperKeepAliveInSecs() {
        return fatClientWrapperKeepAliveInSecs;
    }

    public void setFatClientWrapperKeepAliveInSecs(int fatClientWrapperKeepAliveInSecs) {
        this.fatClientWrapperKeepAliveInSecs = fatClientWrapperKeepAliveInSecs;
    }

    public int getMetadataCheckIntervalInMs() {
        return metadataCheckIntervalInMs;
    }

    public void setMetadataCheckIntervalInMs(int metadataCheckIntervalInMs) {
        this.metadataCheckIntervalInMs = metadataCheckIntervalInMs;
    }

}
