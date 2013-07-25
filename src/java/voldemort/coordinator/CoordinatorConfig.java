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
    private volatile int metadataCheckIntervalInMs = 5000;
    private volatile int nettyServerPort = 8080;
    private volatile int nettyServerBacklog = 1000;
    private volatile int coordinatorCoreThreads = 100;
    private volatile int coordinatorMaxThreads = 200;
    private volatile int numCoordinatorQueuedRequests = 1000;

    /* Propery names for propery-based configuration */
    public static final String BOOTSTRAP_URLS_PROPERTY = "bootstrap_urls";
    public static final String FAT_CLIENTS_CONFIG_FILE_PATH_PROPERTY = "fat_clients_config_file_path";
    public static final String METADATA_CHECK_INTERVAL_IN_MS = "metadata_check_interval_in_ms";
    public static final String NETTY_SERVER_PORT = "netty_server_port";
    public static final String NETTY_SERVER_BACKLOG = "netty_server_backlog";
    public static final String COORDINATOR_CORE_THREADS = "num_coordinator_core_threads";
    public static final String COORDINATOR_MAX_THREADS = "num_coordinator_max_threads";
    public static final String COORDINATOR_QUEUED_REQUESTS = "num_coordinator_queued_requests";

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

        if(props.containsKey(METADATA_CHECK_INTERVAL_IN_MS)) {
            setMetadataCheckIntervalInMs(props.getInt(METADATA_CHECK_INTERVAL_IN_MS,
                                                      this.metadataCheckIntervalInMs));
        }

        if(props.containsKey(NETTY_SERVER_PORT)) {
            setMetadataCheckIntervalInMs(props.getInt(NETTY_SERVER_PORT, this.nettyServerPort));
        }

        if(props.containsKey(NETTY_SERVER_BACKLOG)) {
            setMetadataCheckIntervalInMs(props.getInt(NETTY_SERVER_BACKLOG, this.nettyServerBacklog));
        }

        if(props.containsKey(COORDINATOR_CORE_THREADS)) {
            setCoordinatorCoreThreads(props.getInt(COORDINATOR_CORE_THREADS,
                                                   this.coordinatorCoreThreads));
        }

        if(props.containsKey(COORDINATOR_MAX_THREADS)) {
            setCoordinatorMaxThreads(props.getInt(COORDINATOR_MAX_THREADS,
                                                  this.coordinatorMaxThreads));
        }

        if(props.containsKey(COORDINATOR_QUEUED_REQUESTS)) {
            setCoordinatorQueuedRequestsSize(props.getInt(COORDINATOR_QUEUED_REQUESTS,
                                                          this.numCoordinatorQueuedRequests));
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

    public int getNettyServerBacklog() {
        return nettyServerBacklog;
    }

    /**
     * @param nettyServerBacklog Defines the netty server backlog value
     * 
     */
    public void setNettyServerBacklog(int nettyServerBacklog) {
        this.nettyServerBacklog = nettyServerBacklog;
    }

    public int getCoordinatorCoreThreads() {
        return coordinatorCoreThreads;
    }

    /**
     * @param coordinatorCoreThreads Specifies the # core request executor
     *        threads
     */
    public void setCoordinatorCoreThreads(int coordinatorCoreThreads) {
        this.coordinatorCoreThreads = coordinatorCoreThreads;
    }

    public int getCoordinatorMaxThreads() {
        return coordinatorMaxThreads;
    }

    /**
     * @param coordinatorMaxThreads Specifies the # max request executor threads
     */
    public void setCoordinatorMaxThreads(int coordinatorMaxThreads) {
        this.coordinatorMaxThreads = coordinatorMaxThreads;
    }

    public int getCoordinatorQueuedRequestsSize() {
        return numCoordinatorQueuedRequests;
    }

    /**
     * @param coordinatorQueuedRequestsSize Defines the max # requests that can
     *        be queued for processing
     */
    public void setCoordinatorQueuedRequestsSize(int coordinatorQueuedRequestsSize) {
        this.numCoordinatorQueuedRequests = coordinatorQueuedRequestsSize;
    }

}
