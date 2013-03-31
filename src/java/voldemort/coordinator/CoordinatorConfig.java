/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
    private volatile int nettyServerPort = 8080;

    /* Propery names for propery-based configuration */
    public static final String BOOTSTRAP_URLS_PROPERTY = "bootstrap_urls";
    public static final String FAT_CLIENTS_CONFIG_FILE_PATH_PROPERTY = "fat_clients_config_file_path";
    public static final String FAT_CLIENT_WRAPPER_MAX_POOL_SIZE_PROPERTY = "fat_client_wrapper_max_pool_size";
    public static final String FAT_CLIENT_WRAPPER_CORE_POOL_SIZE_PROPERTY = "fat_client_wrapper_core_pool_size";
    public static final String FAT_CLIENT_WRAPPER_POOL_KEEPALIVE_IN_SECS = "fat_client_wrapper_pool_keepalive_in_secs";
    public static final String METADATA_CHECK_INTERVAL_IN_MS = "metadata_check_interval_in_ms";
    public static final String NETTY_SERVER_PORT = "netty_server_port";

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

    /**
     * Dummy constructor for testing purposes
     */
    public CoordinatorConfig() {}

    /**
     * Set the values using the specified Properties object
     * 
     * @param properties Properties object containing specific property values
     *        for the Coordinator config
     */
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

        if(props.containsKey(NETTY_SERVER_PORT)) {
            setMetadataCheckIntervalInMs(props.getInt(NETTY_SERVER_PORT, this.nettyServerPort));
        }
    }

    public String[] getBootstrapURLs() {
        if(this.bootstrapURLs == null)
            throw new IllegalStateException("No bootstrap urls have been set.");
        return this.bootstrapURLs.toArray(new String[this.bootstrapURLs.size()]);
    }

    /**
     * Sets the bootstrap URLs used by the different Fat clients inside the
     * Coordinator
     * 
     * @param bootstrapUrls list of bootstrap URLs defining which cluster to
     *        connect to
     * @return
     */
    public CoordinatorConfig setBootstrapURLs(List<String> bootstrapUrls) {
        this.bootstrapURLs = Utils.notNull(bootstrapUrls);
        if(this.bootstrapURLs.size() <= 0)
            throw new IllegalArgumentException("Must provide at least one bootstrap URL.");
        return this;
    }

    public String getFatClientConfigPath() {
        return fatClientConfigPath;
    }

    /**
     * Defines individual config for each of the fat clients managed by the
     * Coordinator
     * 
     * @param fatClientConfigPath The path of the file containing the fat client
     *        config in Avro format
     */
    public void setFatClientConfigPath(String fatClientConfigPath) {
        this.fatClientConfigPath = fatClientConfigPath;
    }

    public int getFatClientWrapperMaxPoolSize() {
        return fatClientWrapperMaxPoolSize;
    }

    /**
     * @param fatClientWrapperMaxPoolSize Defines the Maximum pool size for the
     *        thread pool used in the Fat client wrapper
     */
    public void setFatClientWrapperMaxPoolSize(int fatClientWrapperMaxPoolSize) {
        this.fatClientWrapperMaxPoolSize = fatClientWrapperMaxPoolSize;
    }

    public int getFatClientWrapperCorePoolSize() {
        return fatClientWrapperCorePoolSize;
    }

    /**
     * @param fatClientWrapperMaxPoolSize Defines the Core pool size for the
     *        thread pool used in the Fat client wrapper
     */
    public void setFatClientWrapperCorePoolSize(int fatClientWrapperCorePoolSize) {
        this.fatClientWrapperCorePoolSize = fatClientWrapperCorePoolSize;
    }

    public int getFatClientWrapperKeepAliveInSecs() {
        return fatClientWrapperKeepAliveInSecs;
    }

    /**
     * @param fatClientWrapperKeepAliveInSecs Defines the Keep alive period in
     *        seconds for the thread pool used in the Fat client wrapper
     */
    public void setFatClientWrapperKeepAliveInSecs(int fatClientWrapperKeepAliveInSecs) {
        this.fatClientWrapperKeepAliveInSecs = fatClientWrapperKeepAliveInSecs;
    }

    public int getMetadataCheckIntervalInMs() {
        return metadataCheckIntervalInMs;
    }

    /**
     * @param metadataCheckIntervalInMs Defines the frequency with which to
     *        check for updates in the cluster metadata (Eg: cluster.xml and
     *        stores.xml)
     */
    public void setMetadataCheckIntervalInMs(int metadataCheckIntervalInMs) {
        this.metadataCheckIntervalInMs = metadataCheckIntervalInMs;
    }

    public int getServerPort() {
        return nettyServerPort;
    }

    /**
     * @param serverPort Defines the port to use while bootstrapping the Netty
     *        server
     */
    public void setServerPort(int serverPort) {
        this.nettyServerPort = serverPort;
    }

}
