package voldemort.client;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import voldemort.client.protocol.RequestFormatType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
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
    private volatile int maxQueuedRequests = 500;
    private volatile long threadIdleMs = 100000;
    private volatile long connectionTimeoutMs = 500;
    private volatile long socketTimeoutMs = 5000;
    private volatile long routingTimeoutMs = 15000;
    private volatile long defaultNodeBannageMs = 30000;
    private volatile int socketBufferSize = 64 * 1024;
    private volatile SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private volatile List<String> bootstrapUrls = null;
    private volatile RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT;
    private volatile RoutingTier routingTier = RoutingTier.CLIENT;
    private volatile boolean enableJmx = true;

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
        return toInt(unit.convert(defaultNodeBannageMs, TimeUnit.MILLISECONDS));
    }

    /**
     * The period of time to ban a node that gives an error on an operation.
     * 
     * @param nodeBannagePeriod The period of time to ban the node
     * @param unit The time unit of the given value
     */
    public ClientConfig setNodeBannagePeriod(int nodeBannagePeriod, TimeUnit unit) {
        this.defaultNodeBannageMs = unit.toMillis(nodeBannagePeriod);
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
     * client, and server-side routing on the server.
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
}
