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

package voldemort.rest.coordinator;

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
    private volatile int serverPort = 8080;

    private volatile int nettyServerBacklog = 1000;
    private volatile int coordinatorCoreThreads = 100;
    private volatile int coordinatorMaxThreads = 200;
    private volatile int numCoordinatorQueuedRequests = 1000;

    private volatile int httpMessageDecoderMaxInitialLength = 4096;
    private volatile int httpMessageDecoderMaxHeaderSize = 8192;
    private volatile int httpMessageDecoderMaxChunkSize = 8192;

    // Coordinator Admin Service related
    private volatile boolean enableAdminService = true;
    public volatile int adminPort = 9090;
    private volatile int adminServiceBacklog = 1000;
    private volatile int adminServiceCoreThreads = 100;
    private volatile int adminServiceMaxThreads = 200;
    private volatile int adminServiceQueuedRequests = 1000;

    /* Propery names for propery-based configuration */
    public static final String BOOTSTRAP_URLS_PROPERTY = "bootstrap_urls";
    public static final String FAT_CLIENTS_CONFIG_FILE_PATH_PROPERTY = "fat_clients_config_file_path";
    public static final String METADATA_CHECK_INTERVAL_IN_MS = "metadata_check_interval_in_ms";
    public static final String NETTY_SERVER_PORT = "netty_server_port";
    public static final String NETTY_SERVER_BACKLOG = "netty_server_backlog";
    public static final String COORDINATOR_CORE_THREADS = "num_coordinator_core_threads";
    public static final String COORDINATOR_MAX_THREADS = "num_coordinator_max_threads";
    public static final String COORDINATOR_QUEUED_REQUESTS = "num_coordinator_queued_requests";
    public static final String HTTP_MESSAGE_DECODER_MAX_INITIAL_LINE_LENGTH = "http_message_decoder_max_initial_length";
    public static final String HTTP_MESSAGE_DECODER_MAX_HEADER_SIZE = "http_message_decoder_max_header_size";
    public static final String HTTP_MESSAGE_DECODER_MAX_CHUNK_SIZE = "http_message_decoder_max_chunk_size";

    public static final String ADMIN_ENABLE = "admin_enable";
    public static final String ADMIN_PORT = "admin_port";

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
            setServerPort(props.getInt(NETTY_SERVER_PORT, this.serverPort));
        }

        if(props.containsKey(NETTY_SERVER_BACKLOG)) {
            setNettyServerBacklog(props.getInt(NETTY_SERVER_BACKLOG, this.nettyServerBacklog));
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

        if(props.containsKey(HTTP_MESSAGE_DECODER_MAX_INITIAL_LINE_LENGTH)) {
            setHttpMessageDecoderMaxInitialLength(props.getInt(HTTP_MESSAGE_DECODER_MAX_INITIAL_LINE_LENGTH,
                                                               this.httpMessageDecoderMaxInitialLength));
        }

        if(props.containsKey(HTTP_MESSAGE_DECODER_MAX_HEADER_SIZE)) {
            setHttpMessageDecoderMaxHeaderSize(props.getInt(HTTP_MESSAGE_DECODER_MAX_HEADER_SIZE,
                                                            this.httpMessageDecoderMaxHeaderSize));
        }

        if(props.containsKey(HTTP_MESSAGE_DECODER_MAX_CHUNK_SIZE)) {
            setHttpMessageDecoderMaxChunkSize(props.getInt(HTTP_MESSAGE_DECODER_MAX_CHUNK_SIZE,
                                                           this.httpMessageDecoderMaxChunkSize));
        }

        if(props.containsKey(ADMIN_ENABLE)) {
            setAdminServiceEnabled(props.getBoolean(ADMIN_ENABLE, this.enableAdminService));
        }

        if(props.containsKey(ADMIN_PORT)) {
            setServerPort(props.getInt(ADMIN_PORT, this.adminPort));
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
     * @return modified CoordinatorConfig
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
    public CoordinatorConfig setFatClientConfigPath(String fatClientConfigPath) {
        this.fatClientConfigPath = fatClientConfigPath;
        return this;
    }

    public int getMetadataCheckIntervalInMs() {
        return metadataCheckIntervalInMs;
    }

    /**
     * @param metadataCheckIntervalInMs Defines the frequency with which to
     *        check for updates in the cluster metadata (Eg: cluster.xml and
     *        stores.xml)
     */
    public CoordinatorConfig setMetadataCheckIntervalInMs(int metadataCheckIntervalInMs) {
        this.metadataCheckIntervalInMs = metadataCheckIntervalInMs;
        return this;
    }

    public int getServerPort() {
        return serverPort;
    }

    /**
     * @param serverPort Defines the port to use while bootstrapping the Netty
     *        server
     */
    public CoordinatorConfig setServerPort(int serverPort) {
        this.serverPort = serverPort;
        return this;
    }

    public int getNettyServerBacklog() {
        return nettyServerBacklog;
    }

    /**
     * @param nettyServerBacklog Defines the netty server backlog value
     * 
     */
    public CoordinatorConfig setNettyServerBacklog(int nettyServerBacklog) {
        this.nettyServerBacklog = nettyServerBacklog;
        return this;
    }

    public int getCoordinatorCoreThreads() {
        return coordinatorCoreThreads;
    }

    /**
     * @param coordinatorCoreThreads Specifies the # core request executor
     *        threads
     */
    public CoordinatorConfig setCoordinatorCoreThreads(int coordinatorCoreThreads) {
        this.coordinatorCoreThreads = coordinatorCoreThreads;
        return this;
    }

    public int getCoordinatorMaxThreads() {
        return coordinatorMaxThreads;
    }

    /**
     * @param coordinatorMaxThreads Specifies the # max request executor threads
     */
    public CoordinatorConfig setCoordinatorMaxThreads(int coordinatorMaxThreads) {
        this.coordinatorMaxThreads = coordinatorMaxThreads;
        return this;
    }

    public int getCoordinatorQueuedRequestsSize() {
        return numCoordinatorQueuedRequests;
    }

    /**
     * @param coordinatorQueuedRequestsSize Defines the max # requests that can
     *        be queued for processing
     */
    public CoordinatorConfig setCoordinatorQueuedRequestsSize(int coordinatorQueuedRequestsSize) {
        this.numCoordinatorQueuedRequests = coordinatorQueuedRequestsSize;
        return this;
    }

    /**
     * @param httpMessageDecoderMaxInitialLength Defines the maximum length of
     *        the initial line. If the length of the initial line exceeds this
     *        value, a TooLongFrameException will be raised.
     */

    public CoordinatorConfig setHttpMessageDecoderMaxInitialLength(int httpMessageDecoderMaxInitialLength) {
        this.httpMessageDecoderMaxInitialLength = httpMessageDecoderMaxInitialLength;
        return this;
    }

    public int getHttpMessageDecoderMaxInitialLength() {
        return httpMessageDecoderMaxInitialLength;
    }

    /**
     * @param httpMessageDecoderMaxHeaderSize Defines maximum length of all
     *        headers. If the sum of the length of each header exceeds this
     *        value, a TooLongFrameException will be raised.
     */

    public CoordinatorConfig setHttpMessageDecoderMaxHeaderSize(int httpMessageDecoderMaxHeaderSize) {
        this.httpMessageDecoderMaxHeaderSize = httpMessageDecoderMaxHeaderSize;
        return this;
    }

    public int getHttpMessageDecoderMaxHeaderSize() {
        return httpMessageDecoderMaxHeaderSize;
    }

    /**
     * @param httpMessageDecoderMaxChunkSize Defines the maximum length of the
     *        content or each chunk. If the content length (or the length of
     *        each chunk) exceeds this value, the content or chunk will be split
     *        into multiple HttpChunks whose length is maxChunkSize at maximum.
     */

    public CoordinatorConfig setHttpMessageDecoderMaxChunkSize(int httpMessageDecoderMaxChunkSize) {
        this.httpMessageDecoderMaxChunkSize = httpMessageDecoderMaxChunkSize;
        return this;
    }

    public int getHttpMessageDecoderMaxChunkSize() {
        return httpMessageDecoderMaxChunkSize;
    }

    /**
     * Determine whether the admin service has been enabled to perform
     * maintenance operations on the coordinator
     * 
     * Default : true
     */
    public void setAdminServiceEnabled(boolean enableAdminService) {
        this.enableAdminService = enableAdminService;
    }

    public boolean isAdminServiceEnabled() {
        return enableAdminService;
    }

    public int getAdminPort() {
        return adminPort;
    }

    /**
     * @param serverPort Defines the port to use while bootstrapping the Netty
     *        server
     */
    public CoordinatorConfig setAdminPort(int adminPort) {
        this.adminPort = adminPort;
        return this;
    }

    public int getAdminServiceBacklog() {
        return adminServiceBacklog;
    }

    public void setAdminServiceBacklog(int adminServiceBacklog) {
        this.adminServiceBacklog = adminServiceBacklog;
    }

    public int getAdminServiceCoreThreads() {
        return adminServiceCoreThreads;
    }

    public void setAdminServiceCoreThreads(int adminServiceCoreThreads) {
        this.adminServiceCoreThreads = adminServiceCoreThreads;
    }

    public int getAdminServiceMaxThreads() {
        return adminServiceMaxThreads;
    }

    public void setAdminServiceMaxThreads(int adminServiceMaxThreads) {
        this.adminServiceMaxThreads = adminServiceMaxThreads;
    }

    public int getAdminServiceQueuedRequests() {
        return adminServiceQueuedRequests;
    }

    public void setAdminServiceQueuedRequests(int adminServiceQueuedRequests) {
        this.adminServiceQueuedRequests = adminServiceQueuedRequests;
    }

}