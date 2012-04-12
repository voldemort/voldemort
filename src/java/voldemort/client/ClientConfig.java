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
import voldemort.cluster.failuredetector.FailureDetectorConfig;
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
    private volatile int maxTotalConnections = 500;
    private volatile int maxThreads = 5;
    private volatile int maxQueuedRequests = 50;
    private volatile long threadIdleMs = 100000;
    private volatile long connectionTimeoutMs = 500;
    private volatile long socketTimeoutMs = 5000;
    private volatile boolean socketKeepAlive = false;
    private volatile int selectors = 8;
    private volatile long routingTimeoutMs = 15000;
    private volatile int socketBufferSize = 64 * 1024;
    private volatile SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private volatile List<String> bootstrapUrls = null;
    private volatile RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT_V1;
    private volatile RoutingTier routingTier = RoutingTier.CLIENT;
    private volatile boolean enableJmx = true;
    private volatile boolean enableLazy = true;

    private volatile boolean enablePipelineRoutedStore = true;
    private volatile int clientZoneId = Zone.DEFAULT_ZONE_ID;

    private volatile String failureDetectorImplementation = FailureDetectorConfig.DEFAULT_IMPLEMENTATION_CLASS_NAME;
    private volatile long failureDetectorBannagePeriod = FailureDetectorConfig.DEFAULT_BANNAGE_PERIOD;
    private volatile int failureDetectorThreshold = FailureDetectorConfig.DEFAULT_THRESHOLD;
    private volatile int failureDetectorThresholdCountMinimum = FailureDetectorConfig.DEFAULT_THRESHOLD_COUNT_MINIMUM;
    private volatile long failureDetectorThresholdInterval = FailureDetectorConfig.DEFAULT_THRESHOLD_INTERVAL;
    private volatile long failureDetectorAsyncRecoveryInterval = FailureDetectorConfig.DEFAULT_ASYNC_RECOVERY_INTERVAL;
    private volatile List<String> failureDetectorCatastrophicErrorTypes = FailureDetectorConfig.DEFAULT_CATASTROPHIC_ERROR_TYPES;
    private long failureDetectorRequestLengthThreshold = socketTimeoutMs;

    private volatile int maxBootstrapRetries = 2;

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
    }

    public int getMaxConnectionsPerNode() {
        return maxConnectionsPerNode;
    }

    /**
     * Set the maximum number of connection allowed to each voldemort node
     * 
     * @param maxConnectionsPerNode The maximum number of connections
     */
    public ClientConfig setMaxConnectionsPerNode(int maxConnectionsPerNode) {
        if(maxConnectionsPerNode <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.maxConnectionsPerNode = maxConnectionsPerNode;
        return this;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    /**
     * Set the maximum number of connections allowed to all voldemort nodes
     * 
     * @param maxTotalConnections The maximum total number of connections
     */
    public ClientConfig setMaxTotalConnections(int maxTotalConnections) {
        if(maxTotalConnections <= 0)
            throw new IllegalArgumentException("Value must be greater than zero.");
        this.maxTotalConnections = maxTotalConnections;
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

    public ClientConfig setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public int getSelectors() {
        return selectors;
    }

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
     * @param routingTimeout The timeout for all operations to complete.
     * @param unit The time unit of the timeout value
     */
    public ClientConfig setRoutingTimeout(int routingTimeout, TimeUnit unit) {
        this.routingTimeoutMs = unit.toMillis(routingTimeout);
        return this;
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

    public int getThreadIdleTime(TimeUnit unit) {
        return toInt(unit.convert(threadIdleMs, TimeUnit.MILLISECONDS));
    }

    /**
     * The amount of time to keep an idle client thread alive
     * 
     * @param threadIdleTime
     */
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
     * client, and server-side routing on the server. This is not yet used, as
     * the java client only supports client-side routing.
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

    public ClientConfig setClientZoneId(int clientZoneId) {
        this.clientZoneId = clientZoneId;
        return this;
    }

    public int getClientZoneId() {
        return this.clientZoneId;
    }

    public boolean isPipelineRoutedStoreEnabled() {
        return enablePipelineRoutedStore;
    }

    public ClientConfig setEnablePipelineRoutedStore(boolean enablePipelineRoutedStore) {
        this.enablePipelineRoutedStore = enablePipelineRoutedStore;
        return this;
    }

    public String getFailureDetectorImplementation() {
        return failureDetectorImplementation;
    }

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

    public ClientConfig setFailureDetectorThreshold(int failureDetectorThreshold) {
        this.failureDetectorThreshold = failureDetectorThreshold;
        return this;
    }

    public int getFailureDetectorThresholdCountMinimum() {
        return failureDetectorThresholdCountMinimum;
    }

    public ClientConfig setFailureDetectorThresholdCountMinimum(int failureDetectorThresholdCountMinimum) {
        this.failureDetectorThresholdCountMinimum = failureDetectorThresholdCountMinimum;
        return this;
    }

    public long getFailureDetectorThresholdInterval() {
        return failureDetectorThresholdInterval;
    }

    public ClientConfig setFailureDetectorThresholdInterval(long failureDetectorThresholdInterval) {
        this.failureDetectorThresholdInterval = failureDetectorThresholdInterval;
        return this;
    }

    public long getFailureDetectorAsyncRecoveryInterval() {
        return failureDetectorAsyncRecoveryInterval;
    }

    public ClientConfig setFailureDetectorAsyncRecoveryInterval(long failureDetectorAsyncRecoveryInterval) {
        this.failureDetectorAsyncRecoveryInterval = failureDetectorAsyncRecoveryInterval;
        return this;
    }

    public List<String> getFailureDetectorCatastrophicErrorTypes() {
        return failureDetectorCatastrophicErrorTypes;
    }

    public ClientConfig setFailureDetectorCatastrophicErrorTypes(List<String> failureDetectorCatastrophicErrorTypes) {
        this.failureDetectorCatastrophicErrorTypes = failureDetectorCatastrophicErrorTypes;
        return this;
    }

    public long getFailureDetectorRequestLengthThreshold() {
        return failureDetectorRequestLengthThreshold;
    }

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

}
