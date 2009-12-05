/*
 * Copyright 2008-2009 LinkedIn, Inc
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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

/**
 * A configuration object that holds configuration parameters for the client.
 * 
 * @author jay
 * 
 */
public class ClientConfig {

    private volatile int maxConnectionsPerNode = 6;
    private volatile int maxTotalConnections = 500;
    private volatile int maxThreads = 5;
    private volatile int maxQueuedRequests = 50;
    private volatile long threadIdleMs = 100000;
    private volatile long connectionTimeoutMs = 500;
    private volatile long socketTimeoutMs = 5000;
    private volatile long routingTimeoutMs = 15000;
    private volatile long nodeBannageMs = 30000;
    private volatile int socketBufferSize = 64 * 1024;
    private volatile SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private volatile List<String> bootstrapUrls = null;
    private volatile RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT_V1;
    private volatile RoutingTier routingTier = RoutingTier.CLIENT;
    private volatile boolean enableJmx = true;
    private String failureDetector = BannagePeriodFailureDetector.class.getName();
    private volatile int maxBootstrapRetries = 1;

    public ClientConfig() {}

    /* Propery names for propery-based configuration */

    public static final String MAX_CONNECTIONS_PER_NODE_PROPERTY = "max_connections";
    public static final String MAX_TOTAL_CONNECTIONS_PROPERTY = "max_total_connections";
    public static final String MAX_THREADS_PROPERTY = "max_threads";
    public static final String MAX_QUEUED_REQUESTS_PROPERTY = "max_queued_requests";
    public static final String THREAD_IDLE_MS_PROPERTY = "thread_idle_ms";
    public static final String CONNECTION_TIMEOUT_MS_PROPERTY = "connection_timeout_ms";
    public static final String SOCKET_TIMEOUT_MS_PROPERTY = "socket_timeout_ms";
    public static final String ROUTING_TIMEOUT_MS_PROPERTY = "routing_timeout_ms";
    public static final String NODE_BANNAGE_MS_PROPERTY = "node_bannage_ms";
    public static final String SOCKET_BUFFER_SIZE_PROPERTY = "socket_buffer_size";
    public static final String SERIALIZER_FACTORY_CLASS_PROPERTY = "serializer_factory_class";
    public static final String BOOTSTRAP_URLS_PROPERTY = "bootstrap_urls";
    public static final String REQUEST_FORMAT_PROPERTY = "request_format";
    public static final String ENABLE_JMX_PROPERTY = "enable_jmx";
    public static final String FAILURE_DETECTOR_PROPERTY = "failure_detector";
    public static final String MAX_BOOTSTRAP_RETRIES = "max_bootstrap_retries";

    /**
     * Initiate the client config from a set of properties. This is useful for
     * wiring from Spring or for externalizing client properties to a properties
     * file
     * 
     * @param properties The properties to use
     */
    public ClientConfig(Properties properties) {
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

        if(props.containsKey(ROUTING_TIMEOUT_MS_PROPERTY))
            this.setRoutingTimeout(props.getInt(ROUTING_TIMEOUT_MS_PROPERTY), TimeUnit.MILLISECONDS);

        if(props.containsKey(NODE_BANNAGE_MS_PROPERTY))
            this.setNodeBannagePeriod(props.getInt(NODE_BANNAGE_MS_PROPERTY), TimeUnit.MILLISECONDS);

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

        if(props.containsKey(FAILURE_DETECTOR_PROPERTY))
            this.setFailureDetector(props.getString(FAILURE_DETECTOR_PROPERTY));

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
        if(maxConnectionsPerNode <= 0)
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

    public int getNodeBannagePeriod(TimeUnit unit) {
        return toInt(unit.convert(nodeBannageMs, TimeUnit.MILLISECONDS));
    }

    /**
     * The period of time to ban a node that gives an error on an operation.
     * 
     * @param nodeBannagePeriod The period of time to ban the node
     * @param unit The time unit of the given value
     */
    public ClientConfig setNodeBannagePeriod(int nodeBannagePeriod, TimeUnit unit) {
        this.nodeBannageMs = unit.toMillis(nodeBannagePeriod);
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

    public String getFailureDetector() {
        return failureDetector;
    }

    public ClientConfig setFailureDetector(String failureDetector) {
        this.failureDetector = failureDetector;
        return this;
    }

    public int getMaxBootstrapRetries() {
        return maxBootstrapRetries;
    }

    public ClientConfig setMaxBootstrapRetries(int maxBootstrapRetries) {
        if(maxBootstrapRetries < 1)
            throw new IllegalArgumentException("maxBootstrapRetries should be >= 1");

        this.maxBootstrapRetries = maxBootstrapRetries;
        return this;
    }

}
