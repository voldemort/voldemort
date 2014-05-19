package voldemort.client.protocol.admin;

import java.util.Properties;

import voldemort.utils.Props;

/**
 * Client Configuration properties for {@link AdminClient}
 * 
 * 
 */
public class AdminClientConfig {

    private volatile int maxConnectionsPerNode = 50;
    private volatile int maxThreads = 6;
    private volatile long threadIdleMs = 100000;
    private volatile long adminConnectionTimeoutSec = 60;
    private volatile long adminSocketTimeoutSec = 24 * 60 * 60;
    private volatile int adminSocketBufferSize = 16 * 1024 * 1024;
    private volatile boolean adminSocketKeepAlive = false;
    private volatile int restoreDataTimeoutSec = 365 * 24 * 60 * 60;
    private volatile int maxBackoffDelayMs = 60 * 1000;

    public static final String MAX_CONNECTIONS_PER_NODE_PROPERTY = "max_connections";
    public static final String MAX_TOTAL_CONNECTIONS_PROPERTY = "max_total_connections";
    public static final String MAX_THREADS_PROPERTY = "max_threads";
    public static final String THREAD_IDLE_MS_PROPERTY = "thread_idle_ms";
    public static final String ADMIN_CONNECTION_TIMEOUT_SEC_PROPERTY = "admin_connection_timeout_sec";
    public static final String ADMIN_SOCKET_TIMEOUT_SEC_PROPERTY = "admin_socket_timeout_sec";
    public static final String ADMIN_SOCKET_BUFFER_SIZE_PROPERTY = "admin_socket_buffer_size";
    public static final String ADMIN_SOCKET_KEEPALIVE_PROPERTY = "admin_socket_keepalive";
    public static final String RESTORE_DATA_TIMEOUT_SEC = "restore.data.timeout.sec";
    public static final String MAX_BACKOFF_DELAY_MS = "max.backoff.delay.ms";

    // sets better default for AdminClient
    public AdminClientConfig() {
        this(new Properties());

    }

    public AdminClientConfig(Properties properties) {
        Props props = new Props(properties);
        if(props.containsKey(MAX_CONNECTIONS_PER_NODE_PROPERTY))
            this.setMaxConnectionsPerNode(props.getInt(MAX_CONNECTIONS_PER_NODE_PROPERTY));

        if(props.containsKey(MAX_THREADS_PROPERTY))
            this.setMaxThreads(props.getInt(MAX_THREADS_PROPERTY));

        if(props.containsKey(ADMIN_CONNECTION_TIMEOUT_SEC_PROPERTY))
            this.setAdminConnectionTimeoutSec(props.getInt(ADMIN_CONNECTION_TIMEOUT_SEC_PROPERTY));

        if(props.containsKey(ADMIN_SOCKET_TIMEOUT_SEC_PROPERTY))
            this.setAdminSocketTimeoutSec(props.getInt(ADMIN_SOCKET_TIMEOUT_SEC_PROPERTY));

        if(props.containsKey(ADMIN_SOCKET_BUFFER_SIZE_PROPERTY))
            this.setAdminSocketBufferSize(props.getInt(ADMIN_SOCKET_BUFFER_SIZE_PROPERTY));

        if(props.containsKey(ADMIN_SOCKET_KEEPALIVE_PROPERTY))
            this.setAdminSocketKeepAlive(props.getBoolean(ADMIN_SOCKET_KEEPALIVE_PROPERTY));

        if(props.containsKey(RESTORE_DATA_TIMEOUT_SEC))
            this.setRestoreDataTimeoutSec(props.getInt(RESTORE_DATA_TIMEOUT_SEC));

        if(props.containsKey(MAX_BACKOFF_DELAY_MS))
            this.setMaxBackoffDelayMs(props.getInt(MAX_BACKOFF_DELAY_MS));
    }

    /* Propery names for propery-based configuration */

    public int getMaxBackoffDelayMs() {
        return maxBackoffDelayMs;
    }

    public AdminClientConfig setMaxBackoffDelayMs(int maxBackoffDelayMs) {
        this.maxBackoffDelayMs = maxBackoffDelayMs;
        return this;
    }

    public int getMaxConnectionsPerNode() {
        return maxConnectionsPerNode;
    }

    public AdminClientConfig setMaxConnectionsPerNode(int maxConnectionsPerNode) {
        this.maxConnectionsPerNode = maxConnectionsPerNode;
        return this;
    }

    @Deprecated
    public int getMaxThreads() {
        return maxThreads;
    }

    @Deprecated
    public AdminClientConfig setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
        return this;
    }

    public long getThreadIdleMs() {
        return threadIdleMs;
    }

    public AdminClientConfig setThreadIdleMs(long threadIdleMs) {
        this.threadIdleMs = threadIdleMs;
        return this;
    }

    public long getAdminConnectionTimeoutSec() {
        return adminConnectionTimeoutSec;
    }

    public AdminClientConfig setAdminConnectionTimeoutSec(long adminConnectionTimeoutSec) {
        this.adminConnectionTimeoutSec = adminConnectionTimeoutSec;
        return this;
    }

    public long getAdminSocketTimeoutSec() {
        return adminSocketTimeoutSec;
    }

    public AdminClientConfig setAdminSocketTimeoutSec(long adminSocketTimeoutSec) {
        this.adminSocketTimeoutSec = adminSocketTimeoutSec;
        return this;
    }

    public int getAdminSocketBufferSize() {
        return adminSocketBufferSize;
    }

    public AdminClientConfig setAdminSocketBufferSize(int adminSocketBufferSize) {
        this.adminSocketBufferSize = adminSocketBufferSize;
        return this;
    }

    public boolean getAdminSocketKeepAlive() {
        return adminSocketKeepAlive;
    }

    public AdminClientConfig setAdminSocketKeepAlive(boolean adminSocketKeepAlive) {
        this.adminSocketKeepAlive = adminSocketKeepAlive;
        return this;
    }

    public void setRestoreDataTimeoutSec(int restoreDataTimeoutSec) {
        this.restoreDataTimeoutSec = restoreDataTimeoutSec;
    }

    public int getRestoreDataTimeoutSec() {
        return restoreDataTimeoutSec;
    }
}
