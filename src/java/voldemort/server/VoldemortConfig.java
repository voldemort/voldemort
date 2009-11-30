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

package voldemort.server;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import voldemort.client.protocol.RequestFormatType;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.CacheStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.ConfigurationException;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.utils.UndefinedPropertyException;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableList;

/**
 * Configuration parameters for the voldemort server.
 * 
 * @author jay
 * 
 */
public class VoldemortConfig implements Serializable {

    private static final long serialVersionUID = 1;
    public static final String VOLDEMORT_HOME_VAR_NAME = "VOLDEMORT_HOME";
    private static final String VOLDEMORT_NODE_ID_VAR_NAME = "VOLDEMORT_NODE_ID";
    public static int VOLDEMORT_DEFAULT_ADMIN_PORT = 6660;

    private int nodeId;

    private String voldemortHome;
    private String dataDirectory;
    private String metadataDirectory;

    private String slopStoreType;

    private long bdbCacheSize;
    private boolean bdbWriteTransactions;
    private boolean bdbFlushTransactions;
    private boolean bdbSortedDuplicates;
    private String bdbDataDirectory;
    private long bdbMaxLogFileSize;
    private int bdbBtreeFanout;
    private long bdbCheckpointBytes;
    private long bdbCheckpointMs;
    private boolean bdbOneEnvPerStore;
    private int bdbCleanerMinFileUtilization;
    private int bdbCleanerMinUtilization;
    private boolean bdbCursorPreload;

    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlDatabaseName;
    private String mysqlHost;
    private int mysqlPort;

    private int readOnlyBackups;
    private String readOnlyStorageDir;

    private List<String> fsStorageDirs;
    private int fsStorageDirDepth;
    private int fsStorageDirFanOut;
    private int fsStorageNumLockStripes;

    private int coreThreads;
    private int maxThreads;

    private int socketTimeoutMs;
    private int socketBufferSize;

    private boolean useNioConnector;
    private int nioConnectorSelectors;

    private int clientRoutingTimeoutMs;
    private int clientMaxConnectionsPerNode;
    private int clientConnectionTimeoutMs;
    private int clientNodeBannageMs;
    private int clientMaxThreads;
    private int clientThreadIdleMs;
    private int clientMaxQueuedRequests;

    private int schedulerThreads;

    private int numCleanupPermits;

    private RequestFormatType requestFormatType;

    private boolean enableSlop;
    private boolean enableGui;
    private boolean enableHttpServer;
    private boolean enableSocketServer;
    private boolean enableAdminServer;
    private boolean enableJmx;
    private boolean enableVerboseLogging;
    private boolean enableStatTracking;
    private boolean enableServerRouting;
    private boolean enableMetadataChecking;
    private boolean enableRedirectRouting;

    private List<String> storageConfigurations;

    private Props allProps;

    private final long pusherPollMs;

    private int adminCoreThreads;
    private int adminMaxThreads;
    private int adminStreamBufferSize;
    private int adminSocketTimeout;
    private int adminConnectionTimeout;

    public int getAdminSocketTimeout() {
        return adminSocketTimeout;
    }

    public void setAdminSocketTimeout(int adminSocketTimeout) {
        this.adminSocketTimeout = adminSocketTimeout;
    }

    public int getAdminConnectionTimeout() {
        return adminConnectionTimeout;
    }

    public void setAdminConnectionTimeout(int adminConnectionTimeout) {
        this.adminConnectionTimeout = adminConnectionTimeout;
    }

    private int streamMaxReadBytesPerSec;
    private int streamMaxWriteBytesPerSec;

    private int retentionCleanupFirstStartTimeInHour;
    private int retentionCleanupScheduledPeriodInHour;

    public VoldemortConfig(int nodeId, String voldemortHome) {
        this(new Props().with("node.id", nodeId).with("voldemort.home", voldemortHome));
    }

    public VoldemortConfig(Properties props) {
        this(new Props(props));
    }

    public VoldemortConfig(Props props) {
        try {
            this.nodeId = props.getInt("node.id");
        } catch(UndefinedPropertyException e) {
            this.nodeId = getIntEnvVariable(VOLDEMORT_NODE_ID_VAR_NAME);
        }
        this.voldemortHome = props.getString("voldemort.home");
        this.dataDirectory = props.getString("data.directory", this.voldemortHome + File.separator
                                                               + "data");
        this.metadataDirectory = props.getString("metadata.directory", voldemortHome
                                                                       + File.separator + "config");

        this.bdbCacheSize = props.getBytes("bdb.cache.size", 200 * 1024 * 1024);
        this.bdbWriteTransactions = props.getBoolean("bdb.write.transactions", false);
        this.bdbFlushTransactions = props.getBoolean("bdb.flush.transactions", false);
        this.bdbDataDirectory = props.getString("bdb.data.directory", this.dataDirectory
                                                                      + File.separator + "bdb");
        this.bdbMaxLogFileSize = props.getBytes("bdb.max.logfile.size", 60 * 1024 * 1024);
        this.bdbBtreeFanout = props.getInt("bdb.btree.fanout", 512);
        this.bdbCheckpointBytes = props.getLong("bdb.checkpoint.interval.bytes", 20 * 1024 * 1024);
        this.bdbCheckpointMs = props.getLong("bdb.checkpoint.interval.ms", 30 * Time.MS_PER_SECOND);
        this.bdbSortedDuplicates = props.getBoolean("bdb.enable.sorted.duplicates", true);
        this.bdbOneEnvPerStore = props.getBoolean("bdb.one.env.per.store", false);
        this.bdbCleanerMinFileUtilization = props.getInt("bdb.cleaner.min.file.utilization", 5);
        this.bdbCleanerMinUtilization = props.getInt("bdb.cleaner.minUtilization", 50);

        // enabling preload make cursor slow for insufficient bdb cache size.
        this.bdbCursorPreload = props.getBoolean("bdb.cursor.preload", false);

        this.readOnlyBackups = props.getInt("readonly.backups", 1);
        this.readOnlyStorageDir = props.getString("readonly.data.directory", this.dataDirectory
                                                                             + File.separator
                                                                             + "read-only");

        this.fsStorageDirs = props.getList("fs.storage.dirs",
                                           Collections.singletonList(this.dataDirectory
                                                                     + File.separator + "fs"));
        this.fsStorageDirDepth = props.getInt("fs.storage.dir.depth", 2);
        this.fsStorageDirFanOut = props.getInt("fs.storage.dir.fan.out", 1000);
        this.fsStorageNumLockStripes = props.getInt("fs.storage.lock.stripes", 200);

        this.slopStoreType = props.getString("slop.store.engine", BdbStorageConfiguration.TYPE_NAME);

        this.mysqlUsername = props.getString("mysql.user", "root");
        this.mysqlPassword = props.getString("mysql.password", "");
        this.mysqlHost = props.getString("mysql.host", "localhost");
        this.mysqlPort = props.getInt("mysql.port", 3306);
        this.mysqlDatabaseName = props.getString("mysql.database", "voldemort");

        this.maxThreads = props.getInt("max.threads", 100);
        this.coreThreads = props.getInt("core.threads", Math.max(1, maxThreads / 2));

        // Admin client should have less threads but very high buffer size.
        this.adminMaxThreads = props.getInt("admin.max.threads", 10);
        this.adminCoreThreads = props.getInt("admin.core.threads", Math.max(1, adminMaxThreads / 2));
        this.adminStreamBufferSize = (int) props.getBytes("admin.streams.buffer.size",
                                                          10 * 1000 * 1000);
        this.adminConnectionTimeout = props.getInt("admin.client.socket.timeout.ms", 5 * 60 * 1000);
        this.adminSocketTimeout = props.getInt("admin.client.socket.timeout.ms", 10000);

        this.streamMaxReadBytesPerSec = props.getInt("stream.read.byte.per.sec", 1 * 1000 * 1000);
        this.streamMaxWriteBytesPerSec = props.getInt("stream.write.byte.per.sec", 1 * 1000 * 1000);

        this.socketTimeoutMs = props.getInt("socket.timeout.ms", 4000);
        this.socketBufferSize = (int) props.getBytes("socket.buffer.size", 32 * 1024);

        this.useNioConnector = props.getBoolean("enable.nio.connector", false);
        this.nioConnectorSelectors = props.getInt("nio.connector.selectors",
                                                  Runtime.getRuntime().availableProcessors());

        this.clientMaxConnectionsPerNode = props.getInt("client.max.connections.per.node", 5);
        this.clientConnectionTimeoutMs = props.getInt("client.connection.timeout.ms", 400);
        this.clientRoutingTimeoutMs = props.getInt("client.routing.timeout.ms", 5000);
        this.clientNodeBannageMs = props.getInt("client.node.bannage.ms", 10000);
        this.clientMaxThreads = props.getInt("client.max.threads", 100);
        this.clientThreadIdleMs = props.getInt("client.thread.idle.ms", 5000);
        this.clientMaxQueuedRequests = props.getInt("client.max.queued.requests", 1000);

        this.enableHttpServer = props.getBoolean("http.enable", true);
        this.enableSocketServer = props.getBoolean("socket.enable", true);
        this.enableAdminServer = props.getBoolean("admin.enable", true);
        this.enableJmx = props.getBoolean("jmx.enable", true);
        this.enableSlop = props.getBoolean("slop.enable", true);
        this.enableVerboseLogging = props.getBoolean("enable.verbose.logging", true);
        this.enableStatTracking = props.getBoolean("enable.stat.tracking", true);
        this.enableServerRouting = props.getBoolean("enable.server.routing", true);
        this.enableMetadataChecking = props.getBoolean("enable.metadata.checking", true);
        this.enableRedirectRouting = props.getBoolean("enable.redirect.routing", true);

        this.pusherPollMs = props.getInt("pusher.poll.ms", 2 * 60 * 1000);

        this.schedulerThreads = props.getInt("scheduler.threads", 3);

        this.numCleanupPermits = props.getInt("num.cleanup.permits", 1);

        this.storageConfigurations = props.getList("storage.configs",
                                                   ImmutableList.of(BdbStorageConfiguration.class.getName(),
                                                                    MysqlStorageConfiguration.class.getName(),
                                                                    InMemoryStorageConfiguration.class.getName(),
                                                                    CacheStorageConfiguration.class.getName(),
                                                                    ReadOnlyStorageConfiguration.class.getName()));

        // start at midnight (0-23)
        this.retentionCleanupFirstStartTimeInHour = props.getInt("retention.cleanup.first.start.hour",
                                                                 0);
        // repeat every 24 hours
        this.retentionCleanupScheduledPeriodInHour = props.getInt("retention.cleanup.period.hours",
                                                                  24);

        // save props for access from plugins
        this.allProps = props;

        String requestFormatName = props.getString("request.format",
                                                   RequestFormatType.VOLDEMORT_V1.getCode());
        this.requestFormatType = RequestFormatType.fromCode(requestFormatName);

        validateParams();
    }

    private void validateParams() {
        if(coreThreads < 0)
            throw new IllegalArgumentException("core.threads cannot be less than 1");
        else if(coreThreads > maxThreads)
            throw new IllegalArgumentException("core.threads cannot be greater than max.threads.");
        if(maxThreads < 1)
            throw new ConfigurationException("max.threads cannot be less than 1.");
        if(pusherPollMs < 1)
            throw new ConfigurationException("pusher.poll.ms cannot be less than 1.");
        if(socketTimeoutMs < 0)
            throw new ConfigurationException("socket.timeout.ms must be 0 or more ms.");
        if(clientRoutingTimeoutMs < 0)
            throw new ConfigurationException("routing.timeout.ms must be 0 or more ms.");
        if(schedulerThreads < 1)
            throw new ConfigurationException("Must have at least 1 scheduler thread, "
                                             + this.schedulerThreads + " set.");
        if(enableServerRouting && !enableSocketServer)
            throw new ConfigurationException("Server-side routing is enabled, this requires the socket server to also be enabled.");
    }

    private int getIntEnvVariable(String name) {
        String var = System.getenv(name);
        if(var == null)
            throw new ConfigurationException("The environment variable " + name
                                             + " is not defined.");
        try {
            return Integer.parseInt(var);
        } catch(NumberFormatException e) {
            throw new ConfigurationException("Invalid format for environment variable " + name
                                             + ", expecting an integer.", e);
        }
    }

    public static VoldemortConfig loadFromEnvironmentVariable() {
        String voldemortHome = System.getenv(VoldemortConfig.VOLDEMORT_HOME_VAR_NAME);
        if(voldemortHome == null)
            throw new ConfigurationException("No environment variable "
                                             + VoldemortConfig.VOLDEMORT_HOME_VAR_NAME
                                             + " has been defined, set it!");

        return loadFromVoldemortHome(voldemortHome);
    }

    public static VoldemortConfig loadFromVoldemortHome(String voldemortHome) {
        if(!Utils.isReadableDir(voldemortHome))
            throw new ConfigurationException("Attempt to load configuration from VOLDEMORT_HOME, "
                                             + voldemortHome
                                             + " failed. That is not a readable directory.");

        String propertiesFile = voldemortHome + File.separator + "config" + File.separator
                                + "server.properties";
        if(!Utils.isReadableFile(propertiesFile))
            throw new ConfigurationException(propertiesFile
                                             + " is not a readable configuration file.");

        Props properties = null;
        try {
            properties = new Props(new File(propertiesFile));
            properties.put("voldemort.home", voldemortHome);
        } catch(IOException e) {
            throw new ConfigurationException(e);
        }

        return new VoldemortConfig(properties);
    }

    /**
     * The node id given by "node.id" property default: VOLDEMORT_NODE_ID
     * environment variable
     */
    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * The node id given by "voldemort.home" default: VOLDEMORT_HOME environment
     * variable
     */
    public String getVoldemortHome() {
        return voldemortHome;
    }

    public void setVoldemortHome(String voldemortHome) {
        this.voldemortHome = voldemortHome;
    }

    /**
     * The directory name given by "data.directory" default: voldemort.home/data
     */
    public String getDataDirectory() {
        return dataDirectory;
    }

    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    /**
     * The directory name given by "metadata.directory" default:
     * voldemort.home/config
     */
    public String getMetadataDirectory() {
        return metadataDirectory;
    }

    public void setMetadataDirectory(String metadataDirectory) {
        this.metadataDirectory = metadataDirectory;
    }

    /**
     * The cache size given by "bdb.cache.size" in bytes default: 200MB
     */
    public long getBdbCacheSize() {
        return bdbCacheSize;
    }

    public void setBdbCacheSize(int bdbCacheSize) {
        this.bdbCacheSize = bdbCacheSize;
    }

    /**
     * Given by "bdb.flush.transactions". If true then sync transactions to disk
     * immediately. default: false
     */
    public boolean isBdbFlushTransactionsEnabled() {
        return bdbFlushTransactions;
    }

    public void setBdbFlushTransactions(boolean bdbSyncTransactions) {
        this.bdbFlushTransactions = bdbSyncTransactions;
    }

    /**
     * The directory in which bdb data is stored. Given by "bdb.data.directory"
     * default: data.directory/bdb
     */
    public String getBdbDataDirectory() {
        return bdbDataDirectory;
    }

    public void setBdbDataDirectory(String bdbDataDirectory) {
        this.bdbDataDirectory = bdbDataDirectory;
    }

    /**
     * The maximum size of a single .jdb log file in bytes. Given by
     * "bdb.max.logfile.size" default: 60MB
     */
    public long getBdbMaxLogFileSize() {
        return this.bdbMaxLogFileSize;
    }

    public void setBdbMaxLogFileSize(long bdbMaxLogFileSize) {
        this.bdbMaxLogFileSize = bdbMaxLogFileSize;
    }

    /**
     * A log file will be cleaned if its utilization percentage is below this
     * value, irrespective of total utilization.
     * 
     * <ul>
     * <li>property: "bdb.cleaner.minFileUtilization"</li>
     * <li>default: 5</li>
     * <li>minimum: 0</li>
     * <li>maximum: 50</li>
     * </ul>
     */
    public int getBdbCleanerMinFileUtilization() {
        return bdbCleanerMinFileUtilization;
    }

    public final void setBdbCleanerMinFileUtilization(int minFileUtilization) {
        if(minFileUtilization < 0 || minFileUtilization > 50)
            throw new IllegalArgumentException("minFileUtilization should be between 0 and 50 (both inclusive)");
        this.bdbCleanerMinFileUtilization = minFileUtilization;
    }

    /**
     * 
     * The cleaner will keep the total disk space utilization percentage above
     * this value.
     * 
     * <ul>
     * <li>property: "bdb.cleaner.minUtilization"</li>
     * <li>default: 50</li>
     * <li>minimum: 0</li>
     * <li>maximum: 90</li>
     * </ul>
     */
    public int getBdbCleanerMinUtilization() {
        return bdbCleanerMinUtilization;
    }

    public final void setBdbCleanerMinUtilization(int minUtilization) {
        if(minUtilization < 0 || minUtilization > 90)
            throw new IllegalArgumentException("minUtilization should be between 0 and 90 (both inclusive)");
        this.bdbCleanerMinUtilization = minUtilization;
    }

    /**
     * 
     * The btree node fanout. Given by "bdb.btree.fanout". default: 512
     */
    public int getBdbBtreeFanout() {
        return this.bdbBtreeFanout;
    }

    public void setBdbBtreeFanout(int bdbBtreeFanout) {
        this.bdbBtreeFanout = bdbBtreeFanout;
    }

    /**
     * Do we preload the cursor or not? The advantage of preloading for cursor
     * is faster streaming performance, as entries are fetched in disk order.
     * Incidentally, pre-loading is only a side-effect of what we're really
     * trying to do: fetch in disk (as opposed to key) order, but there doesn't
     * seem to be an easy/intuitive way to do for BDB JE.
     */
    public boolean getBdbCursorPreload() {
        return this.bdbCursorPreload;
    }

    public void setBdbCursorPreload(boolean bdbCursorPreload) {
        this.bdbCursorPreload = bdbCursorPreload;
    }

    /**
     * The comfortable number of threads the threadpool will attempt to
     * maintain. Specified by "core.threads" default: max(1, floor(0.5 *
     * max.threads))
     */
    public int getCoreThreads() {
        return coreThreads;
    }

    public void setCoreThreads(int coreThreads) {
        this.coreThreads = coreThreads;
    }

    /**
     * The maximum number of threadpool threads set by "max.threads" default:
     * 100
     */
    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public int getAdminCoreThreads() {
        return adminCoreThreads;
    }

    public void setAdminCoreThreads(int coreThreads) {
        this.adminCoreThreads = coreThreads;
    }

    public int getAdminMaxThreads() {
        return adminMaxThreads;
    }

    public void setAdminMaxThreads(int maxThreads) {
        this.adminMaxThreads = maxThreads;
    }

    public boolean isHttpServerEnabled() {
        return enableHttpServer;
    }

    public void setEnableHttpServer(boolean enableHttpServer) {
        this.enableHttpServer = enableHttpServer;
    }

    public boolean isSocketServerEnabled() {
        return enableSocketServer;
    }

    public void setAdminServerEnabled(boolean enableSocketServer) {
        this.enableSocketServer = enableSocketServer;
    }

    public boolean isAdminServerEnabled() {
        return enableAdminServer;
    }

    public int getStreamMaxReadBytesPerSec() {
        return streamMaxReadBytesPerSec;
    }

    public void setStreamMaxReadBytesPerSec(int streamMaxReadBytesPerSec) {
        this.streamMaxReadBytesPerSec = streamMaxReadBytesPerSec;
    }

    public int getStreamMaxWriteBytesPerSec() {
        return streamMaxWriteBytesPerSec;
    }

    public void setStreamMaxWriteBytesPerSec(int streamMaxWriteBytesPerSec) {
        this.streamMaxWriteBytesPerSec = streamMaxWriteBytesPerSec;
    }

    public void setEnableAdminServer(boolean enableAdminServer) {
        this.enableAdminServer = enableAdminServer;
    }

    public boolean isJmxEnabled() {
        return enableJmx;
    }

    public void setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
    }

    public long getPusherPollMs() {
        return pusherPollMs;
    }

    public boolean isGuiEnabled() {
        return enableGui;
    }

    public void setEnableGui(boolean enableGui) {
        this.enableGui = enableGui;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public String getMysqlDatabaseName() {
        return mysqlDatabaseName;
    }

    public void setMysqlDatabaseName(String mysqlDatabaseName) {
        this.mysqlDatabaseName = mysqlDatabaseName;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    public void setMysqlHost(String mysqlHost) {
        this.mysqlHost = mysqlHost;
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    public void setMysqlPort(int mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public String getSlopStoreType() {
        return slopStoreType;
    }

    public void setSlopStoreType(String slopStoreType) {
        this.slopStoreType = slopStoreType;
    }

    public int getSocketTimeoutMs() {
        return this.socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getRoutingTimeoutMs() {
        return this.clientRoutingTimeoutMs;
    }

    public void setClientRoutingTimeoutMs(int routingTimeoutMs) {
        this.clientRoutingTimeoutMs = routingTimeoutMs;
    }

    public int getClientMaxConnectionsPerNode() {
        return clientMaxConnectionsPerNode;
    }

    public void setClientMaxConnectionsPerNode(int maxConnectionsPerNode) {
        this.clientMaxConnectionsPerNode = maxConnectionsPerNode;
    }

    public int getClientConnectionTimeoutMs() {
        return clientConnectionTimeoutMs;
    }

    public void setClientConnectionTimeoutMs(int connectionTimeoutMs) {
        this.clientConnectionTimeoutMs = connectionTimeoutMs;
    }

    public int getClientNodeBannageMs() {
        return clientNodeBannageMs;
    }

    public void setClientNodeBannageMs(int nodeBannageMs) {
        this.clientNodeBannageMs = nodeBannageMs;
    }

    public int getClientMaxThreads() {
        return clientMaxThreads;
    }

    public void setClientMaxThreads(int clientMaxThreads) {
        this.clientMaxThreads = clientMaxThreads;
    }

    public int getClientThreadIdleMs() {
        return clientThreadIdleMs;
    }

    public void setClientThreadIdleMs(int clientThreadIdleMs) {
        this.clientThreadIdleMs = clientThreadIdleMs;
    }

    public int getClientMaxQueuedRequests() {
        return clientMaxQueuedRequests;
    }

    public void setClientMaxQueuedRequests(int clientMaxQueuedRequests) {
        this.clientMaxQueuedRequests = clientMaxQueuedRequests;
    }

    public boolean isSlopEnabled() {
        return this.enableSlop;
    }

    public void setEnableSlop(boolean enableSlop) {
        this.enableSlop = enableSlop;
    }

    public boolean isVerboseLoggingEnabled() {
        return this.enableVerboseLogging;
    }

    public void setEnableVerboseLogging(boolean enableVerboseLogging) {
        this.enableVerboseLogging = enableVerboseLogging;
    }

    public boolean isStatTrackingEnabled() {
        return this.enableStatTracking;
    }

    public void setEnableStatTracking(boolean enableStatTracking) {
        this.enableStatTracking = enableStatTracking;
    }

    public boolean isMetadataCheckingEnabled() {
        return enableMetadataChecking;
    }

    public void setEnableMetadataChecking(boolean enableMetadataChecking) {
        this.enableMetadataChecking = enableMetadataChecking;
    }

    public boolean isRedirectRoutingEnabled() {
        return enableRedirectRouting;
    }

    public void setEnableRedirectRouting(boolean enableRedirectRouting) {
        this.enableRedirectRouting = enableRedirectRouting;
    }

    public long getBdbCheckpointBytes() {
        return this.bdbCheckpointBytes;
    }

    public void setBdbCheckpointBytes(long bdbCheckpointBytes) {
        this.bdbCheckpointBytes = bdbCheckpointBytes;
    }

    public long getBdbCheckpointMs() {
        return this.bdbCheckpointMs;
    }

    public void setBdbCheckpointMs(long bdbCheckpointMs) {
        this.bdbCheckpointMs = bdbCheckpointMs;
    }

    public int getSchedulerThreads() {
        return schedulerThreads;
    }

    public void setSchedulerThreads(int schedulerThreads) {
        this.schedulerThreads = schedulerThreads;
    }

    public String getReadOnlyDataStorageDirectory() {
        return this.readOnlyStorageDir;
    }

    public void setReadOnlyDataStorageDirectory(String readOnlyStorageDir) {
        this.readOnlyStorageDir = readOnlyStorageDir;
    }

    public int getReadOnlyBackups() {
        return readOnlyBackups;
    }

    public void setReadOnlyBackups(int readOnlyBackups) {
        this.readOnlyBackups = readOnlyBackups;
    }

    public boolean isBdbWriteTransactionsEnabled() {
        return bdbWriteTransactions;
    }

    public void setBdbWriteTransactions(boolean bdbWriteTransactions) {
        this.bdbWriteTransactions = bdbWriteTransactions;
    }

    public boolean isBdbSortedDuplicatesEnabled() {
        return this.bdbSortedDuplicates;
    }

    public void setBdbSortedDuplicates(boolean enable) {
        this.bdbSortedDuplicates = enable;
    }

    public void setBdbOneEnvPerStore(boolean bdbOneEnvPerStore) {
        this.bdbOneEnvPerStore = bdbOneEnvPerStore;
    }

    public boolean isBdbOneEnvPerStore() {
        return bdbOneEnvPerStore;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public boolean getUseNioConnector() {
        return this.useNioConnector;
    }

    public void setUseNioConnector(boolean useNio) {
        this.useNioConnector = useNio;
    }

    public int getNioConnectorSelectors() {
        return nioConnectorSelectors;
    }

    public void setNioConnectorSelectors(int nioConnectorSelectors) {
        this.nioConnectorSelectors = nioConnectorSelectors;
    }

    public int getAdminSocketBufferSize() {
        return adminStreamBufferSize;
    }

    public void setAdminSocketBufferSize(int socketBufferSize) {
        this.adminStreamBufferSize = socketBufferSize;
    }

    public List<String> getStorageConfigurations() {
        return storageConfigurations;
    }

    public void setStorageConfigurations(List<String> storageConfigurations) {
        this.storageConfigurations = storageConfigurations;
    }

    public Props getAllProps() {
        return this.allProps;
    }

    public void setRequestFormatType(RequestFormatType type) {
        this.requestFormatType = type;
    }

    public RequestFormatType getRequestFormatType() {
        return this.requestFormatType;
    }

    public boolean isServerRoutingEnabled() {
        return this.enableServerRouting;
    }

    public void setEnableServerRouting(boolean enableServerRouting) {
        this.enableServerRouting = enableServerRouting;
    }

    public int getNumCleanupPermits() {
        return numCleanupPermits;
    }

    public void setNumCleanupPermits(int numCleanupPermits) {
        this.numCleanupPermits = numCleanupPermits;
    }

    public int getRetentionCleanupFirstStartTimeInHour() {
        return retentionCleanupFirstStartTimeInHour;
    }

    public void setRetentionCleanupFirstStartTimeInHour(int retentionCleanupFirstStartTimeInHour) {
        this.retentionCleanupFirstStartTimeInHour = retentionCleanupFirstStartTimeInHour;
    }

    public int getRetentionCleanupScheduledPeriodInHour() {
        return retentionCleanupScheduledPeriodInHour;
    }

    public void setRetentionCleanupScheduledPeriodInHour(int retentionCleanupScheduledPeriodInHour) {
        this.retentionCleanupScheduledPeriodInHour = retentionCleanupScheduledPeriodInHour;
    }

    public List<String> getFsStorageDirs() {
        return fsStorageDirs;
    }

    public void setFsStorageDirs(List<String> fsStorageDirs) {
        this.fsStorageDirs = fsStorageDirs;
    }

    public int getFsStorageDirDepth() {
        return fsStorageDirDepth;
    }

    public void setFsStorageDirDepth(int fsStorageDirDepth) {
        this.fsStorageDirDepth = fsStorageDirDepth;
    }

    public int getFsStorageDirFanOut() {
        return fsStorageDirFanOut;
    }

    public void setFsStorageDirFanOut(int fsStorageDirFanOut) {
        this.fsStorageDirFanOut = fsStorageDirFanOut;
    }

    public int getFsStorageNumLockStripes() {
        return fsStorageNumLockStripes;
    }

    public void setFsStorageNumLockStripes(int fsStorageNumLockStripes) {
        this.fsStorageNumLockStripes = fsStorageNumLockStripes;
    }

}
