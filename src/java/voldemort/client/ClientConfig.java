/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.ThresholdFailureDetector;
import voldemort.common.VoldemortOpCode;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
import voldemort.utils.ConfigurationException;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

/**
 * A configuration object that holds configuration parameters for the client.
 * 
 * 
 */
public class ClientConfig {

    private volatile int maxConnectionsPerNode = 50;
    private volatile long connectionTimeoutMs = 500;
    private volatile long socketTimeoutMs = 5000;
    private volatile boolean socketKeepAlive = false;
    private volatile int selectors = 8;
    private volatile long routingTimeoutMs = 5000;
    private volatile TimeoutConfig timeoutConfig = new TimeoutConfig(routingTimeoutMs, false);
    private volatile int socketBufferSize = 64 * 1024;
    private volatile SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private volatile List<String> bootstrapUrls = null;
    private volatile RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT_V1;
    private volatile RoutingTier routingTier = RoutingTier.CLIENT;
    private volatile boolean enableLazy = true;

    private volatile boolean enablePipelineRoutedStore = true;
    private volatile int clientZoneId = Zone.DEFAULT_ZONE_ID;

    /*
     * The following are only used with a non pipe line routed, i.e non NIO
     * based client
     */
    @Deprecated
    private volatile int maxTotalConnections = 500;
    @Deprecated
    private volatile int maxThreads = 5;
    @Deprecated
    private volatile int maxQueuedRequests = 50;
    @Deprecated
    private volatile long threadIdleMs = 100000;

    private volatile boolean useDefaultClient = true;

    private volatile String failureDetectorImplementation = FailureDetectorConfig.DEFAULT_IMPLEMENTATION_CLASS_NAME;
    private volatile long failureDetectorBannagePeriod = FailureDetectorConfig.DEFAULT_BANNAGE_PERIOD;
    private volatile int failureDetectorThreshold = FailureDetectorConfig.DEFAULT_THRESHOLD;
    private volatile int failureDetectorThresholdCountMinimum = FailureDetectorConfig.DEFAULT_THRESHOLD_COUNT_MINIMUM;
    private volatile long failureDetectorThresholdIntervalMs = FailureDetectorConfig.DEFAULT_THRESHOLD_INTERVAL;
    private volatile long failureDetectorAsyncRecoveryIntervalMs = FailureDetectorConfig.DEFAULT_ASYNC_RECOVERY_INTERVAL;
    private volatile List<String> failureDetectorCatastrophicErrorTypes = FailureDetectorConfig.DEFAULT_CATASTROPHIC_ERROR_TYPES;
    private long failureDetectorRequestLengthThreshold = socketTimeoutMs;

    private volatile int maxBootstrapRetries = 2;
    private volatile String clientContextName = "";

    /* 5 second check interval, in ms */
    private volatile long asyncCheckMetadataIntervalInMs = 5000;
    /* 12 hr refresh internval, in seconds */
    private volatile int clientRegistryRefreshIntervalInSecs = 3600 * 12;
    private volatile int asyncJobThreadPoolSize = 2;

    /* SystemStore client config */
    private volatile int sysMaxConnectionsPerNode = 2;
    private volatile int sysRoutingTimeoutMs = 5000;
    private volatile int sysSocketTimeoutMs = 5000;
    private volatile int sysConnectionTimeoutMs = 1500;
    private volatile boolean sysEnableJmx = false;
    private volatile boolean sysEnablePipelineRoutedStore = true;

    /* Voldemort client component config */
    private volatile boolean enableJmx = true;
    private volatile boolean enableCompressionLayer = true;
    private volatile boolean enableSerializationLayer = true;
    private volatile boolean enableInconsistencyResolvingLayer = true;

    public ClientConfig() {}

    /* Propery names for propery-based configuration */

    public static final String MAX_CONNECTIONS_PER_NODE_PROPERTY = "max_connections";
    public static final String MAX_TOTAL_CONNECTIONS_PROPERTY = "max_total_connections";
    public static final String MAX_THREADS_PROPERTY = "max_threads";
    public static final String MAX_QUEUED_REQUESTS_PROPERTY = "max_queued_requests";
    public static final String THREAD_IDLE_MS_PROPERTY = "thread_idle_ms";
    public static final String CONNECTION_TIMEOUT_MS_PROPERTY = "connection_timeout_ms";
    public static final String SOCKET_TIMEOUT_MS_PROPERTY = "socket_timeout_ms";
    public static final String SOCKET_KEEPALIVE_PROPERTY = "socket_keepalive";
    public static final String SELECTORS_PROPERTY = "selectors";
    public static final String ROUTING_TIMEOUT_MS_PROPERTY = "routing_timeout_ms";
    public static final String GETALL_ROUTING_TIMEOUT_MS_PROPERTY = "getall_routing_timeout_ms";
    public static final String PUT_ROUTING_TIMEOUT_MS_PROPERTY = "put_routing_timeout_ms";
    public static final String GET_ROUTING_TIMEOUT_MS_PROPERTY = "get_routing_timeout_ms";
    public static final String GET_VERSIONS_ROUTING_TIMEOUT_MS_PROPERTY = "getversions_routing_timeout_ms";
    public static final String DELETE_ROUTING_TIMEOUT_MS_PROPERTY = "delete_routing_timeout_ms";
    public static final String ALLOW_PARTIAL_GETALLS_PROPERTY = "allow_partial_getalls";
    public static final String NODE_BANNAGE_MS_PROPERTY = "node_bannage_ms";
    public static final String SOCKET_BUFFER_SIZE_PROPERTY = "socket_buffer_size";
    public static final String SERIALIZER_FACTORY_CLASS_PROPERTY = "serializer_factory_class";
    public static final String BOOTSTRAP_URLS_PROPERTY = "bootstrap_urls";
    public static final String REQUEST_FORMAT_PROPERTY = "request_format";
    public static final String ENABLE_JMX_PROPERTY = "enable_jmx";
    public static final String ENABLE_PIPELINE_ROUTED_STORE_PROPERTY = "enable_pipeline_routed_store";
    public static final String ENABLE_HINTED_HANDOFF_PROPERTY = "enable_hinted_handoff";
    public static final String ENABLE_LAZY_PROPERTY = "enable-lazy";
    public static final String CLIENT_ZONE_ID = "client_zone_id";
    public static final String FAILUREDETECTOR_IMPLEMENTATION_PROPERTY = "failuredetector_implementation";
    public static final String FAILUREDETECTOR_BANNAGE_PERIOD_PROPERTY = "failuredetector_bannage_period";
    public static final String FAILUREDETECTOR_THRESHOLD_PROPERTY = "failuredetector_threshold";
    public static final String FAILUREDETECTOR_THRESHOLD_INTERVAL_PROPERTY = "failuredetector_threshold_interval";
    public static final String FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM_PROPERTY = "failuredetector_threshold_countminimum";
    public static final String FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL_PROPERTY = "failuredetector_asyncscan_interval";
    public static final String FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES_PROPERTY = "failuredetector_catastrophic_error_types";
    public static final String FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD_PROPERTY = "failuredetector_request_length_threshold";
    public static final String MAX_BOOTSTRAP_RETRIES = "max_bootstrap_retries";
    public static final String CLIENT_CONTEXT_NAME = "voldemort_client_context_name";
    public static final String ASYNC_CHECK_METADATA_INTERVAL = "check_metadata_interval_ms";
    public static final String USE_DEFAULT_CLIENT = "use_default_client";
    public static final String CLIENT_REGISTRY_REFRESH_INTERVAL = "client_registry_refresh_interval_seconds";
    public static final String ASYNC_JOB_THREAD_POOL_SIZE = "async_job_thread_pool_size";
    public static final String SYS_MAX_CONNECTIONS_PER_NODE = "sys_max_connections_per_node";
    public static final String SYS_ROUTING_TIMEOUT_MS = "sys_routing_timeout_ms";
    public static final String SYS_CONNECTION_TIMEOUT_MS = "sys_connection_timeout_ms";
    public static final String SYS_SOCKET_TIMEOUT_MS = "sys_socket_timeout_ms";
    public static final String SYS_ENABLE_JMX = "sys_enable_jmx";
    public static final String SYS_ENABLE_PIPELINE_ROUTED_STORE = "sys_enable_pipeline_routed_store";
    public static final String ENABLE_COMPRESSION_LAYER = "enable_compression_layer";
    public static final String ENABLE_SERIALIZATION_LAYER = "enable_serialization_layer";
    public static final String ENABLE_INCONSISTENCY_RESOLVING_LAYER = "enable_inconsistency_resolving_layer";

    /**
     * Instantiate the client config using a properties file
     * 
     * @param propertyFile Properties file
     */
    public ClientConfig(File propertyFile) {
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
     * Initiate the client config from a set of properties. This is useful for
     * wiring from Spring or for externalizing client properties to a properties
     * file
     * 
     * @param properties The properties to use
     */
    public ClientConfig(Properties properties) {
        setProperties(properties);
    }

    private void setProperties(Properties properties) {
        Props props = new Props(properties);
        if(props.containsKey(MAX_CONNECTIONS_PER_NODE_PROPERTY))
            this.setMaxConnectionsPerNode(props.getInt(MAX_CONNECTIONS_PER_NODE_PROPERTY));

        if(props.containsKey(MAX_TOTAL_CONNECTIONS_PROPERTY))
            this.setMaxTotalConnections(props.getInt(MAX_TOTAL_CONNECTIONS_PROPERTY));

        if(props.containsKey(MAX_THREADS_PROPERTY))
            this.setMaxThreads(props.getInt(MAX_THREADS_PROPERTY));

        if(props.containsKey(MAX_QUEUED_REQUESTS_PROPERTY))
            this.setMaxQueuedRequests(props.getInt(MAX_QUEUED_REQUESTS_PROPERTY));

        if(props.containsKey(THREAD_IDLE_MS_PROPERTY))
            this.setThreadIdleTime(props.getLong(THREAD_IDLE_MS_PROPERTY), TimeUnit.MILLISECONDS);

        if(props.containsKey(CONNECTION_TIMEOUT_MS_PROPERTY))
            this.setConnectionTimeout(props.getInt(CONNECTION_TIMEOUT_MS_PROPERTY),
                                      TimeUnit.MILLISECONDS);

        if(props.containsKey(SOCKET_TIMEOUT_MS_PROPERTY))
            this.setSocketTimeout(props.getInt(SOCKET_TIMEOUT_MS_PROPERTY), TimeUnit.MILLISECONDS);

        if(props.containsKey(SOCKET_KEEPALIVE_PROPERTY))
            this.setSocketKeepAlive(props.getBoolean(SOCKET_KEEPALIVE_PROPERTY));

        if(props.containsKey(SELECTORS_PROPERTY))
            this.setSelectors(props.getInt(SELECTORS_PROPERTY));

        if(props.containsKey(ROUTING_TIMEOUT_MS_PROPERTY))
            this.setRoutingTimeout(props.getInt(ROUTING_TIMEOUT_MS_PROPERTY), TimeUnit.MILLISECONDS);

        // By default, make all the timeouts equal to routing timeout
        timeoutConfig = new TimeoutConfig(routingTimeoutMs, false);

        if(props.containsKey(GETALL_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE,
                                              props.getInt(GETALL_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(GET_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_OP_CODE,
                                              props.getInt(GET_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(PUT_ROUTING_TIMEOUT_MS_PROPERTY)) {
            long putTimeoutMs = props.getInt(PUT_ROUTING_TIMEOUT_MS_PROPERTY);
            timeoutConfig.setOperationTimeout(VoldemortOpCode.PUT_OP_CODE, putTimeoutMs);
            // By default, use the same thing for getVersions() also
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE, putTimeoutMs);
        }

        // of course, if someone overrides it, we will respect that
        if(props.containsKey(GET_VERSIONS_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE,
                                              props.getInt(GET_VERSIONS_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(DELETE_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.DELETE_OP_CODE,
                                              props.getInt(DELETE_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(ALLOW_PARTIAL_GETALLS_PROPERTY))
            timeoutConfig.setPartialGetAllAllowed(props.getBoolean(ALLOW_PARTIAL_GETALLS_PROPERTY));

        if(props.containsKey(SOCKET_BUFFER_SIZE_PROPERTY))
            this.setSocketBufferSize(props.getInt(SOCKET_BUFFER_SIZE_PROPERTY));

        if(props.containsKey(SERIALIZER_FACTORY_CLASS_PROPERTY)) {
            Class<?> factoryClass = ReflectUtils.loadClass(props.getString(SERIALIZER_FACTORY_CLASS_PROPERTY));
            SerializerFactory factory = (SerializerFactory) ReflectUtils.callConstructor(factoryClass,
                                                                                         new Object[] {});
            this.setSerializerFactory(factory);
        }

        if(props.containsKey(BOOTSTRAP_URLS_PROPERTY))
            this.setBootstrapUrls(props.getList(BOOTSTRAP_URLS_PROPERTY));

        if(props.containsKey(REQUEST_FORMAT_PROPERTY))
            this.setRequestFormatType(RequestFormatType.fromCode(props.getString(REQUEST_FORMAT_PROPERTY)));

        if(props.containsKey(ENABLE_JMX_PROPERTY))
            this.setEnableJmx(props.getBoolean(ENABLE_JMX_PROPERTY));

        if(props.containsKey(ENABLE_LAZY_PROPERTY))
            this.setEnableLazy(props.getBoolean(ENABLE_LAZY_PROPERTY));

        if(props.containsKey(ENABLE_PIPELINE_ROUTED_STORE_PROPERTY))
            this.setEnablePipelineRoutedStore(props.getBoolean(ENABLE_PIPELINE_ROUTED_STORE_PROPERTY));

        if(props.containsKey(CLIENT_ZONE_ID))
            this.setClientZoneId(props.getInt(CLIENT_ZONE_ID));

        if(props.containsKey(USE_DEFAULT_CLIENT))
            this.enableDefaultClient(props.getBoolean(USE_DEFAULT_CLIENT));

        if(props.containsKey(FAILUREDETECTOR_IMPLEMENTATION_PROPERTY))
            this.setFailureDetectorImplementation(props.getString(FAILUREDETECTOR_IMPLEMENTATION_PROPERTY));

        // We're changing the property from "node_bannage_ms" to
        // "failuredetector_bannage_period" so if we have the old one, migrate
        // it over.
        if(props.containsKey(NODE_BANNAGE_MS_PROPERTY)
           && !props.containsKey(FAILUREDETECTOR_BANNAGE_PERIOD_PROPERTY)) {
            props.put(FAILUREDETECTOR_BANNAGE_PERIOD_PROPERTY, props.get(NODE_BANNAGE_MS_PROPERTY));
        }

        if(props.containsKey(FAILUREDETECTOR_BANNAGE_PERIOD_PROPERTY))
            this.setFailureDetectorBannagePeriod(props.getLong(FAILUREDETECTOR_BANNAGE_PERIOD_PROPERTY));

        if(props.containsKey(FAILUREDETECTOR_THRESHOLD_PROPERTY))
            this.setFailureDetectorThreshold(props.getInt(FAILUREDETECTOR_THRESHOLD_PROPERTY));

        if(props.containsKey(FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM_PROPERTY))
            this.setFailureDetectorThresholdCountMinimum(props.getInt(FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM_PROPERTY));

        if(props.containsKey(FAILUREDETECTOR_THRESHOLD_INTERVAL_PROPERTY))
            this.setFailureDetectorThresholdInterval(props.getLong(FAILUREDETECTOR_THRESHOLD_INTERVAL_PROPERTY));

        if(props.containsKey(FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL_PROPERTY))
            this.setFailureDetectorAsyncRecoveryInterval(props.getLong(FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL_PROPERTY));

        if(props.containsKey(FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES_PROPERTY))
            this.setFailureDetectorCatastrophicErrorTypes(props.getList(FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES_PROPERTY));

        if(props.containsKey(FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD_PROPERTY))
            this.setFailureDetectorRequestLengthThreshold(props.getLong(FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD_PROPERTY));
        else
            this.setFailureDetectorRequestLengthThreshold(getSocketTimeout(TimeUnit.MILLISECONDS));

        if(props.containsKey(MAX_BOOTSTRAP_RETRIES))
            this.setMaxBootstrapRetries(props.getInt(MAX_BOOTSTRAP_RETRIES));

        if(props.containsKey(CLIENT_CONTEXT_NAME)) {
            this.setClientContextName(props.getString(CLIENT_CONTEXT_NAME));
        }

        if(props.containsKey(ASYNC_CHECK_METADATA_INTERVAL)) {
            this.setAsyncMetadataRefreshInMs(props.getLong(ASYNC_CHECK_METADATA_INTERVAL));
        }

        if(props.containsKey(CLIENT_REGISTRY_REFRESH_INTERVAL)) {
            this.setClientRegistryUpdateIntervalInSecs(props.getInt(CLIENT_REGISTRY_REFRESH_INTERVAL));
        }

        if(props.containsKey(ASYNC_JOB_THREAD_POOL_SIZE)) {
            this.setAsyncJobThreadPoolSize(props.getInt(ASYNC_JOB_THREAD_POOL_SIZE));
        }

        /* Check for system store paramaters if any */
        if(props.containsKey(SYS_MAX_CONNECTIONS_PER_NODE)) {
            this.setSysMaxConnectionsPerNode(props.getInt(SYS_MAX_CONNECTIONS_PER_NODE));
        }

        if(props.containsKey(SYS_ROUTING_TIMEOUT_MS)) {
            this.setSysRoutingTimeout(props.getInt(SYS_ROUTING_TIMEOUT_MS));
        }

        if(props.containsKey(SYS_SOCKET_TIMEOUT_MS)) {
            this.setSysSocketTimeout(props.getInt(SYS_SOCKET_TIMEOUT_MS));
        }

        if(props.containsKey(SYS_CONNECTION_TIMEOUT_MS)) {
            this.setSysConnectionTimeout(props.getInt(SYS_CONNECTION_TIMEOUT_MS));
        }

        if(props.containsKey(SYS_ENABLE_JMX)) {
            this.setSysEnableJmx(props.getBoolean(SYS_ENABLE_JMX));
        }

        if(props.containsKey(SYS_ENABLE_PIPELINE_ROUTED_STORE)) {
            this.setSysEnablePipelineRoutedStore(props.getBoolean(SYS_ENABLE_PIPELINE_ROUTED_STORE));
        }

        if(props.containsKey(ENABLE_COMPRESSION_LAYER)) {
            this.setEnableCompressionLayer(props.getBoolean(ENABLE_COMPRESSION_LAYER));
        }

        if(props.containsKey(ENABLE_SERIALIZATION_LAYER)) {
            this.setEnableSerializationLayer(props.getBoolean(ENABLE_SERIALIZATION_LAYER));
        }

        if(props.containsKey(ENABLE_INCONSISTENCY_RESOLVING_LAYER)) {
            this.setEnableInconsistencyResolvingLayer(props.getBoolean(ENABLE_INCONSISTENCY_RESOLVING_LAYER));
        }

    }

    /**
     * Sets the maximum number of connections a system store client will create
     * to a voldemort server
     * 
     * @param maxConnectionsPerNode
     * @return
     */
    private ClientConfig setSysMaxConnectionsPerNode(int maxConnectionsPerNode) {
        if(maxConnectionsPerNode <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.sysMaxConnectionsPerNode = maxConnectionsPerNode;
        return this;
    }

    public int getSysMaxConnectionsPerNode() {
        return this.sysMaxConnectionsPerNode;
    }

    /**
     * Sets the routing layer timeout for the system store client.
     * 
     * @param sysRoutingTimeout
     * @return
     */
    private ClientConfig setSysRoutingTimeout(int sysRoutingTimeoutMs) {
        if(sysRoutingTimeoutMs <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.sysRoutingTimeoutMs = sysRoutingTimeoutMs;
        return this;
    }

    public int getSysRoutingTimeout() {
        return this.sysRoutingTimeoutMs;
    }

    /**
     * Sets the socket timeout (at the java.net layer) for the system store
     * client
     * 
     * @param sysSocketTimeout
     * @return
     */
    private ClientConfig setSysSocketTimeout(int sysSocketTimeoutMs) {
        if(sysSocketTimeoutMs <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.sysSocketTimeoutMs = sysSocketTimeoutMs;
        return this;
    }

    public int getSysSocketTimeout() {
        return this.sysSocketTimeoutMs;
    }

    /**
     * Amount of time in ms spent trying to establish a connection to voldemort
     * servers, from the system store client
     * 
     * @param sysConnectionTimeoutMs
     * @return
     */
    private ClientConfig setSysConnectionTimeout(int sysConnectionTimeoutMs) {
        if(sysConnectionTimeoutMs <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.sysConnectionTimeoutMs = sysConnectionTimeoutMs;
        return this;
    }

    public int getSysConnectionTimeout() {
        return this.sysConnectionTimeoutMs;
    }

    /**
     * Whether or not JMX monitoring is enabled for the system store
     * 
     * @param sysEnableJmx
     * @return
     */
    public ClientConfig setSysEnableJmx(boolean sysEnableJmx) {
        this.sysEnableJmx = sysEnableJmx;
        return this;
    }

    public boolean getSysEnableJmx() {
        return this.sysEnableJmx;
    }

    /**
     * Should pipleline store be used by the system store client?
     * 
     * @param sysEnablePipelineRoutedStore
     * @return
     */
    public ClientConfig setSysEnablePipelineRoutedStore(boolean sysEnablePipelineRoutedStore) {
        this.sysEnablePipelineRoutedStore = sysEnablePipelineRoutedStore;
        return this;
    }

    public boolean getSysEnablePipelineRoutedStore() {
        return this.sysEnablePipelineRoutedStore;
    }

    public int getMaxConnectionsPerNode() {
        return maxConnectionsPerNode;
    }

    /**
     * Set the maximum number of connections allowed to each voldemort node.
     * Play with this value to determine how many connections are enough for
     * your workload. Without high enough connections, you may not be able to
     * throw enough traffic at the servers
     * 
     * @param maxConnectionsPerNode The maximum number of connections
     */
    public ClientConfig setMaxConnectionsPerNode(int maxConnectionsPerNode) {
        if(maxConnectionsPerNode <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.maxConnectionsPerNode = maxConnectionsPerNode;
        return this;
    }

    public int getSocketTimeout(TimeUnit unit) {
        return toInt(unit.convert(socketTimeoutMs, TimeUnit.MILLISECONDS));
    }

    /**
     * Set the SO_TIMEOUT for the socket if using HTTP or socket based network
     * communication. This is the maximum amount of time the socket will block
     * waiting for network activity.
     * 
     * @param socketTimeout The socket timeout
     * @param unit The time unit of the timeout value
     */
    public ClientConfig setSocketTimeout(int socketTimeout, TimeUnit unit) {
        this.socketTimeoutMs = unit.toMillis(socketTimeout);
        return this;
    }

    public boolean getSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     * Enabled/disable SO_KEEPALIVE on the connection created with the voldemort
     * servers
     * 
     * @param socketKeepAlive
     * @return
     */
    public ClientConfig setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public int getSelectors() {
        return selectors;
    }

    /**
     * Number of NIO selector threads to use, to handle communication with the
     * server.Typically, this is same as the number of cores in the client
     * machine
     * 
     * @param selectors
     * @return
     */
    public ClientConfig setSelectors(int selectors) {
        this.selectors = selectors;
        return this;
    }

    public int getRoutingTimeout(TimeUnit unit) {
        return toInt(unit.convert(routingTimeoutMs, TimeUnit.MILLISECONDS));
    }

    /**
     * Set the timeout for all blocking operations to complete on all nodes. The
     * number of blocking operations can be configured using the preferred-reads
     * and preferred-writes configuration for the store.
     * 
     * See {@link #GETALL_ROUTING_TIMEOUT_MS_PROPERTY},
     * {@link #GET_ROUTING_TIMEOUT_MS_PROPERTY},
     * {@link #PUT_ROUTING_TIMEOUT_MS_PROPERTY},
     * {@link #DELETE_ROUTING_TIMEOUT_MS_PROPERTY} to override timeouts for
     * specific operations
     * 
     * @param routingTimeout The timeout for all operations to complete.
     * @param unit The time unit of the timeout value
     */
    public ClientConfig setRoutingTimeout(int routingTimeout, TimeUnit unit) {
        this.routingTimeoutMs = unit.toMillis(routingTimeout);
        return this;
    }

    /**
     * Set the timeout configuration for the voldemort operations
     * 
     * @param tConfig
     * @return
     */
    public ClientConfig setTimeoutConfig(TimeoutConfig tConfig) {
        this.timeoutConfig = tConfig;
        return this;
    }

    /**
     * Get the timeouts for voldemort operations
     * 
     * @return
     */
    public TimeoutConfig getTimeoutConfig() {
        return timeoutConfig;
    }

    /**
     * @deprecated Use {@link #getFailureDetectorBannagePeriod()} instead
     */
    @Deprecated
    public int getNodeBannagePeriod(TimeUnit unit) {
        return toInt(unit.convert(failureDetectorBannagePeriod, TimeUnit.MILLISECONDS));
    }

    /**
     * The period of time to ban a node that gives an error on an operation.
     * 
     * @param nodeBannagePeriod The period of time to ban the node
     * @param unit The time unit of the given value
     * 
     * @deprecated Use {@link #setFailureDetectorBannagePeriod(long)} instead
     */
    @Deprecated
    public ClientConfig setNodeBannagePeriod(int nodeBannagePeriod, TimeUnit unit) {
        this.failureDetectorBannagePeriod = unit.toMillis(nodeBannagePeriod);
        return this;
    }

    public int getConnectionTimeout(TimeUnit unit) {
        return (int) Math.min(unit.convert(connectionTimeoutMs, TimeUnit.MILLISECONDS),
                              Integer.MAX_VALUE);
    }

    /**
     * Set the maximum allowable time to block waiting for a free connection
     * 
     * @param connectionTimeout The connection timeout
     * @param unit The time unit of the given value
     */
    public ClientConfig setConnectionTimeout(int connectionTimeout, TimeUnit unit) {
        this.connectionTimeoutMs = unit.toMillis(connectionTimeout);
        return this;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /**
     * Set the size of the socket buffer to use for both socket reads and socket
     * writes
     * 
     * @param socketBufferSize The size of the socket buffer in bytes
     */
    public ClientConfig setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
        return this;
    }

    public SerializerFactory getSerializerFactory() {
        return serializerFactory;
    }

    /**
     * Set the SerializerFactory used to serialize and deserialize values
     */
    public ClientConfig setSerializerFactory(SerializerFactory serializerFactory) {
        this.serializerFactory = Utils.notNull(serializerFactory);
        return this;
    }

    public String[] getBootstrapUrls() {
        if(this.bootstrapUrls == null)
            throw new IllegalStateException("No bootstrap urls have been set.");
        return this.bootstrapUrls.toArray(new String[this.bootstrapUrls.size()]);
    }

    /**
     * Set the bootstrap urls from which to attempt a connection
     * 
     * @param bootstrapUrls The urls to bootstrap from
     */
    public ClientConfig setBootstrapUrls(List<String> bootstrapUrls) {
        this.bootstrapUrls = Utils.notNull(bootstrapUrls);
        if(this.bootstrapUrls.size() <= 0)
            throw new IllegalArgumentException("Must provide at least one bootstrap URL.");
        return this;
    }

    /**
     * Set the bootstrap urls from which to attempt a connection
     * 
     * @param bootstrapUrls The urls to bootstrap from
     */
    public ClientConfig setBootstrapUrls(String... bootstrapUrls) {
        this.bootstrapUrls = Arrays.asList(Utils.notNull(bootstrapUrls));
        if(this.bootstrapUrls.size() <= 0)
            throw new IllegalArgumentException("Must provide at least one bootstrap URL.");
        return this;
    }

    public RequestFormatType getRequestFormatType() {
        return requestFormatType;
    }

    /**
     * Set the request format type used for network communications (for example
     * protocol buffers)
     * 
     * @param requestFormatType The type of the network protocol
     */
    public ClientConfig setRequestFormatType(RequestFormatType requestFormatType) {
        this.requestFormatType = Utils.notNull(requestFormatType);
        return this;
    }

    public RoutingTier getRoutingTier() {
        return routingTier;
    }

    /**
     * Set the tier at which routing occurs. Client-side routing occurs on the
     * client, and server-side routing on the server.
     * 
     * NOTE : Server side routing is not used, as yet. The java client only
     * supports client-side routing.
     * 
     * @param routingTier The routing tier to use for routing requests
     */
    public ClientConfig setRoutingTier(RoutingTier routingTier) {
        this.routingTier = Utils.notNull(routingTier);
        return this;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * Set the maximum number of client threads
     * 
     * @param maxThreads The maximum number of client threads
     */
    public ClientConfig setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
        return this;
    }

    @Deprecated
    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    @Deprecated
    public int getThreadIdleTime(TimeUnit unit) {
        return toInt(unit.convert(threadIdleMs, TimeUnit.MILLISECONDS));
    }

    /**
     * The amount of time to keep an idle client thread alive
     * 
     * @param threadIdleTime
     */
    @Deprecated
    public ClientConfig setThreadIdleTime(long threadIdleTime, TimeUnit unit) {
        this.threadIdleMs = unit.toMillis(threadIdleTime);
        return this;
    }

    public int getMaxQueuedRequests() {
        return maxQueuedRequests;
    }

    /**
     * Set the maximum number of queued node operations before client actions
     * will be blocked
     * 
     * @param maxQueuedRequests The maximum number of queued requests
     */
    public ClientConfig setMaxQueuedRequests(int maxQueuedRequests) {
        this.maxQueuedRequests = maxQueuedRequests;
        return this;
    }

    /**
     * Set the maximum number of connections allowed to all voldemort nodes.
     * Note: This has no effect when using NIO based pipeline routing
     * 
     * @param maxTotalConnections The maximum total number of connections
     */
    @Deprecated
    public ClientConfig setMaxTotalConnections(int maxTotalConnections) {
        if(maxTotalConnections <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.maxTotalConnections = maxTotalConnections;
        return this;
    }

    public int toInt(long l) {
        return (int) Math.min(l, Integer.MAX_VALUE);
    }

    public boolean isJmxEnabled() {
        return this.enableJmx;
    }

    /**
     * Enable JMX monitoring of the clients?
     * 
     * @param enableJmx If true JMX monitoring of the clients will be enabled
     */
    public ClientConfig setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
        return this;
    }

    public boolean isLazyEnabled() {
        return this.enableLazy;
    }

    /**
     * Enable lazy initialization of clients?
     * 
     * @param enableLazy If true clients will be lazily initialized
     */
    public ClientConfig setEnableLazy(boolean enableLazy) {
        this.enableLazy = enableLazy;
        return this;
    }

    /**
     * Sets the zone the client belongs to. This is very important in zoned
     * configurations since the client always has an "affinity" towards the
     * servers in its zone.
     * 
     * Default : zone 0
     * 
     * @param clientZoneId
     * @return
     */
    public ClientConfig setClientZoneId(int clientZoneId) {
        this.clientZoneId = clientZoneId;
        return this;
    }

    public int getClientZoneId() {
        return this.clientZoneId;
    }

    /**
     * Whether or not a {@link ZenStoreClient} is created as opposed to a
     * {@link DefaultStoreClient}
     * 
     * true = DefaultStoreClient and false = ZenStoreClient
     * 
     * @param enableDefault
     * @return
     */
    public ClientConfig enableDefaultClient(boolean enableDefault) {
        this.useDefaultClient = enableDefault;
        return this;
    }

    public boolean isDefaultClientEnabled() {
        return this.useDefaultClient;
    }

    public boolean isPipelineRoutedStoreEnabled() {
        return enablePipelineRoutedStore;
    }

    /**
     * Whether or not to use the Pipeline routing which is much more resource
     * efficient by employing Java NIO to handle communication with the server
     * 
     * @param enablePipelineRoutedStore
     * @return
     */
    public ClientConfig setEnablePipelineRoutedStore(boolean enablePipelineRoutedStore) {
        this.enablePipelineRoutedStore = enablePipelineRoutedStore;
        return this;
    }

    public String getFailureDetectorImplementation() {
        return failureDetectorImplementation;
    }

    /**
     * FailureDetector to use. Its highly recommended to use
     * {@link ThresholdFailureDetector} as opposed to using
     * {@link BannagePeriodFailureDetector}
     * 
     * @param failureDetectorImplementation
     * @return
     */
    public ClientConfig setFailureDetectorImplementation(String failureDetectorImplementation) {
        this.failureDetectorImplementation = failureDetectorImplementation;
        return this;
    }

    public long getFailureDetectorBannagePeriod() {
        return failureDetectorBannagePeriod;
    }

    public ClientConfig setFailureDetectorBannagePeriod(long failureDetectorBannagePeriod) {
        this.failureDetectorBannagePeriod = failureDetectorBannagePeriod;
        return this;
    }

    public int getFailureDetectorThreshold() {
        return failureDetectorThreshold;
    }

    /**
     * Set the percentage of exceptions that tolerated in a given failure
     * detector window. If the client experiences more exceptions than this
     * threshold, it will mark the erring server down
     * 
     * @param failureDetectorThreshold
     * @return
     */
    public ClientConfig setFailureDetectorThreshold(int failureDetectorThreshold) {
        this.failureDetectorThreshold = failureDetectorThreshold;
        return this;
    }

    public int getFailureDetectorThresholdCountMinimum() {
        return failureDetectorThresholdCountMinimum;
    }

    /**
     * Sets the minimum number of failures (exceptions/slow responses) in a
     * given failure detector window, for a server to be marked down. Guards
     * against a very small number of exceptions tripping the Failure detector
     * due to low activity
     * 
     * @param failureDetectorThresholdCountMinimum
     * @return
     */
    public ClientConfig setFailureDetectorThresholdCountMinimum(int failureDetectorThresholdCountMinimum) {
        this.failureDetectorThresholdCountMinimum = failureDetectorThresholdCountMinimum;
        return this;
    }

    public long getFailureDetectorThresholdInterval() {
        return failureDetectorThresholdIntervalMs;
    }

    /**
     * Time window in ms, over which the failure detector accounts the failures
     * and successes
     * 
     * @param failureDetectorThresholdIntervalMs
     * @return
     */
    public ClientConfig setFailureDetectorThresholdInterval(long failureDetectorThresholdIntervalMs) {
        this.failureDetectorThresholdIntervalMs = failureDetectorThresholdIntervalMs;
        return this;
    }

    public long getFailureDetectorAsyncRecoveryInterval() {
        return failureDetectorAsyncRecoveryIntervalMs;
    }

    /**
     * Number of milliseconds, to try to check if a marked down server has come
     * back up again
     * 
     * @param failureDetectorAsyncRecoveryInterval
     * @return
     */
    public ClientConfig setFailureDetectorAsyncRecoveryInterval(long failureDetectorAsyncRecoveryInterval) {
        this.failureDetectorAsyncRecoveryIntervalMs = failureDetectorAsyncRecoveryInterval;
        return this;
    }

    public List<String> getFailureDetectorCatastrophicErrorTypes() {
        return failureDetectorCatastrophicErrorTypes;
    }

    /**
     * Sets the exception types that should be treated as catastrophic,by the
     * failure detector, resulting in the server being immediately considered
     * down. Input string list should be populated by something like
     * ConnectException.class.getName()
     * 
     * @param failureDetectorCatastrophicErrorTypes
     * @return
     */
    public ClientConfig setFailureDetectorCatastrophicErrorTypes(List<String> failureDetectorCatastrophicErrorTypes) {
        this.failureDetectorCatastrophicErrorTypes = failureDetectorCatastrophicErrorTypes;
        return this;
    }

    public long getFailureDetectorRequestLengthThreshold() {
        return failureDetectorRequestLengthThreshold;
    }

    /**
     * Sets the maximum amount of time a request is allowed to take, to be not
     * considered as a "slow" request and count against the server, in terms of
     * failure detection
     * 
     * @param failureDetectorRequestLengthThreshold
     * @return
     */
    public ClientConfig setFailureDetectorRequestLengthThreshold(long failureDetectorRequestLengthThreshold) {
        this.failureDetectorRequestLengthThreshold = failureDetectorRequestLengthThreshold;
        return this;
    }

    public int getMaxBootstrapRetries() {
        return maxBootstrapRetries;
    }

    /**
     * If we are unable to bootstrap, how many times should we re-try?
     * 
     * @param maxBootstrapRetries Maximum times to retry bootstrapping (must be
     *        >= 1)
     * @throws IllegalArgumentException If maxBootstrapRetries < 1
     */
    public ClientConfig setMaxBootstrapRetries(int maxBootstrapRetries) {
        if(maxBootstrapRetries < 1)
            throw new IllegalArgumentException("maxBootstrapRetries should be >= 1");

        this.maxBootstrapRetries = maxBootstrapRetries;
        return this;
    }

    public String getClientContextName() {
        return clientContextName;
    }

    /**
     * Set the client context name
     * 
     * @param clientContextName The name of client context
     */
    public ClientConfig setClientContextName(String clientContextName) {
        this.clientContextName = clientContextName;
        return this;
    }

    public long getAsyncMetadataRefreshInMs() {
        return asyncCheckMetadataIntervalInMs;
    }

    /**
     * Set the interval on which client checks for metadata change on servers
     * 
     * @param asyncCheckMetadataIntervalMs The metadata change interval
     */
    public ClientConfig setAsyncMetadataRefreshInMs(long asyncCheckMetadataIntervalMs) {

        this.asyncCheckMetadataIntervalInMs = asyncCheckMetadataIntervalMs;
        return this;
    }

    public int getClientRegistryUpdateIntervalInSecs() {
        return this.clientRegistryRefreshIntervalInSecs;
    }

    /**
     * Set the interval on which client refreshes its corresponding entry of the
     * client registry on the servers
     * 
     * @param clientRegistryRefreshIntervalInSecs The refresh interval in
     *        seconds
     */
    public ClientConfig setClientRegistryUpdateIntervalInSecs(int clientRegistryRefrshIntervalInSecs) {
        this.clientRegistryRefreshIntervalInSecs = clientRegistryRefrshIntervalInSecs;
        return this;
    }

    public int getAsyncJobThreadPoolSize() {
        return asyncJobThreadPoolSize;
    }

    /**
     * Set the # of threads for the async. job thread pool
     * 
     * @param asyncJobThreadPoolSize The max # of threads in the async job
     */
    public ClientConfig setAsyncJobThreadPoolSize(int asyncJobThreadPoolSize) {
        this.asyncJobThreadPoolSize = asyncJobThreadPoolSize;
        return this;
    }

    public boolean isEnableCompressionLayer() {
        return enableCompressionLayer;
    }

    public ClientConfig setEnableCompressionLayer(boolean enableCompressionLayer) {
        this.enableCompressionLayer = enableCompressionLayer;
        return this;
    }

    public boolean isEnableSerializationLayer() {
        return enableSerializationLayer;
    }

    public ClientConfig setEnableSerializationLayer(boolean enableSerializationLayer) {
        this.enableSerializationLayer = enableSerializationLayer;
        return this;
    }

    public boolean isEnableInconsistencyResolvingLayer() {
        return enableInconsistencyResolvingLayer;
    }

    public ClientConfig setEnableInconsistencyResolvingLayer(boolean enableInconsistencyResolvingLayer) {
        this.enableInconsistencyResolvingLayer = enableInconsistencyResolvingLayer;
        return this;
    }

    public String toString() {
        StringBuilder clientConfigInfo = new StringBuilder();
        clientConfigInfo.append("Max connections per node: " + this.maxConnectionsPerNode + "\n");
        clientConfigInfo.append("Connection timeout : " + this.connectionTimeoutMs + "\n");
        clientConfigInfo.append("Socket timeout : " + this.socketTimeoutMs + "\n");
        clientConfigInfo.append("Routing timeout : " + this.routingTimeoutMs + "\n");
        return clientConfigInfo.toString();
    }
}
