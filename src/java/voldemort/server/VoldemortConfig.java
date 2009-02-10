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
import java.util.Properties;

import voldemort.store.StorageEngineType;
import voldemort.utils.ConfigurationException;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.utils.UndefinedPropertyException;
import voldemort.utils.Utils;

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

    private int nodeId;

    private String voldemortHome;
    private String dataDirectory;
    private String metadataDirectory;

    private StorageEngineType slopStoreType;

    private long bdbCacheSize;
    private boolean bdbWriteTransactions;
    private boolean bdbFlushTransactions;
    private String bdbDataDirectory;
    private long bdbMaxLogFileSize;
    private int bdbBtreeFanout;
    private long bdbCheckpointBytes;
    private long bdbCheckpointMs;
    private boolean bdbOneEnvPerStore;

    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlDatabaseName;
    private String mysqlHost;
    private int mysqlPort;

    private int readOnlyFileHandles;
    private long readOnlyFileWaitTimeoutMs;
    private int readOnlyBackups;
    private String readOnlyStorageDir;
    private long readOnlyCacheSize;

    private int coreThreads;
    private int maxThreads;

    private int socketTimeoutMs;
    private int routingTimeoutMs;

    private int schedulerThreads;

    private boolean enableSlopDetection;
    private boolean enableGui;
    private boolean enableHttpServer;
    private boolean enableSocketServer;
    private boolean enableJmx;
    private boolean enableBdbEngine;
    private boolean enableMysqlEngine;
    private boolean enableCacheEngine;
    private boolean enableMemoryEngine;
    private boolean enableReadOnlyEngine;
    private boolean enableVerboseLogging;
    private boolean enableStatTracking;

    private final long pusherPollMs;

    public VoldemortConfig() {
        this(new Props());
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

        this.enableBdbEngine = props.getBoolean("enable.bdb.engine", true);
        this.enableMysqlEngine = props.getBoolean("enable.mysql.engine", false);
        this.enableMemoryEngine = props.getBoolean("enable.memory.engine", true);
        this.enableCacheEngine = props.getBoolean("enable.cache.engine", true);

        this.bdbCacheSize = props.getBytes("bdb.cache.size", 200 * 1024 * 1024);
        this.bdbWriteTransactions = props.getBoolean("bdb.write.transactions", false);
        this.bdbFlushTransactions = props.getBoolean("bdb.flush.transactions", false);
        this.bdbDataDirectory = props.getString("bdb.data.directory", this.dataDirectory
                                                                      + File.separator + "bdb");
        this.bdbMaxLogFileSize = props.getBytes("bdb.max.logfile.size", 1024 * 1024 * 1024);
        this.bdbBtreeFanout = props.getInt("bdb.btree.fanout", 512);
        this.bdbCheckpointBytes = props.getLong("bdb.checkpoint.interval.bytes", 20 * 1024 * 1024);
        this.bdbCheckpointMs = props.getLong("bdb.checkpoint.interval.ms", 30 * Time.MS_PER_SECOND);
        this.bdbOneEnvPerStore = props.getBoolean("bdb.one.env.per.store", true);

        this.enableReadOnlyEngine = props.getBoolean("enable.readonly.engine", false);
        this.readOnlyFileWaitTimeoutMs = props.getLong("readonly.file.wait.timeout.ms", 4000L);
        this.readOnlyBackups = props.getInt("readonly.backups", 1);
        this.readOnlyFileHandles = props.getInt("readonly.file.handles", 5);
        this.readOnlyStorageDir = props.getString("readonly.data.directory", this.dataDirectory
                                                                             + File.separator
                                                                             + "read-only");
        this.readOnlyCacheSize = props.getInt("readonly.cache.size", 100 * 1000 * 1000);

        this.slopStoreType = StorageEngineType.fromDisplay(props.getString("slop.store.engine",
                                                                           StorageEngineType.BDB.toDisplay()));

        this.mysqlUsername = props.getString("mysql.user", "root");
        this.mysqlPassword = props.getString("mysql.password", "");
        this.mysqlHost = props.getString("mysql.host", "localhost");
        this.mysqlPort = props.getInt("mysql.port", 3306);
        this.mysqlDatabaseName = props.getString("mysql.database", "voldemort");

        this.maxThreads = props.getInt("max.threads", 100);
        this.coreThreads = props.getInt("core.threads", Math.max(1, maxThreads / 2));

        this.socketTimeoutMs = props.getInt("socket.timeout.ms", 4000);
        this.routingTimeoutMs = props.getInt("routing.timeout.ms", 5000);

        this.enableHttpServer = props.getBoolean("http.enable", true);
        this.enableSocketServer = props.getBoolean("socket.enable", true);
        this.enableJmx = props.getBoolean("jmx.enable", true);
        this.enableSlopDetection = props.getBoolean("slop.detection.enable", false);
        this.enableVerboseLogging = props.getBoolean("enable.verbose.logging", true);
        this.enableStatTracking = props.getBoolean("enable.stat.tracking", true);

        this.pusherPollMs = props.getInt("pusher.poll.ms", 2 * 60 * 1000);

        this.schedulerThreads = props.getInt("scheduler.threads", 3);

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
        if(routingTimeoutMs < 0)
            throw new ConfigurationException("routing.timeout.ms must be 0 or more ms.");
        if(schedulerThreads < 1)
            throw new ConfigurationException("Must have at least 1 scheduler thread, "
                                             + this.schedulerThreads + " set.");
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
                                             + ", expecting an integer.");
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
     * Given by "bdb.sync.transactions". If true then sync transactions to disk
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
     * "bdb.max.logfile.size" default: 1G
     */
    public long getBdbMaxLogFileSize() {
        return this.bdbMaxLogFileSize;
    }

    public void setBdbMaxLogFileSize(long bdbMaxLogFileSize) {
        this.bdbMaxLogFileSize = bdbMaxLogFileSize;
    }

    /**
     * The btree node fanout. Given by "bdb.btree.fanout". default: 512
     */
    public int getBdbBtreeFanout() {
        return this.bdbBtreeFanout;
    }

    public void setBdbBtreeFanout(int bdbBtreeFanout) {
        this.bdbBtreeFanout = bdbBtreeFanout;
    }

    /**
     * The bdb Environment setting. use an environment(file) per store (if true)
     * or use a common environment (default: false)
     */
    public boolean getBdbOneEnvPerStore() {
        return this.bdbOneEnvPerStore;
    }

    public void setBdbOneEnvPerStore(boolean bdbOneEnvPerStore) {
        this.bdbOneEnvPerStore = bdbOneEnvPerStore;
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

    public boolean isHttpServerEnabled() {
        return enableHttpServer;
    }

    public void setEnableHttpServer(boolean enableHttpServer) {
        this.enableHttpServer = enableHttpServer;
    }

    public boolean isSocketServerEnabled() {
        return enableSocketServer;
    }

    public void setEnableSocketServer(boolean enableSocketServer) {
        this.enableSocketServer = enableSocketServer;
    }

    public boolean isJmxEnabled() {
        return enableJmx;
    }

    public void setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
    }

    public boolean iseBdbEngineEnabled() {
        return enableBdbEngine;
    }

    public void setEnableBdbEngine(boolean enableBdbEngine) {
        this.enableBdbEngine = enableBdbEngine;
    }

    public boolean isMysqlEngineEnabled() {
        return enableMysqlEngine;
    }

    public void setEnableMysqlEngine(boolean enableMysqlEngine) {
        this.enableMysqlEngine = enableMysqlEngine;
    }

    public boolean isMemoryEngineEnabled() {
        return enableMemoryEngine;
    }

    public void setEnableMemoryEngine(boolean enableMemoryEngine) {
        this.enableMemoryEngine = enableMemoryEngine;
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

    public StorageEngineType getSlopStoreType() {
        return slopStoreType;
    }

    public void setSlopStoreType(StorageEngineType slopStoreType) {
        this.slopStoreType = slopStoreType;
    }

    public int getSocketTimeoutMs() {
        return this.socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getRoutingTimeoutMs() {
        return this.routingTimeoutMs;
    }

    public void setRoutingTimeoutMs(int routingTimeoutMs) {
        this.routingTimeoutMs = routingTimeoutMs;
    }

    public boolean isSlopDetectionEnabled() {
        return this.enableSlopDetection;
    }

    public void setEnableSlopDetection(boolean enableSlopDetection) {
        this.enableSlopDetection = enableSlopDetection;
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

    public int getReadOnlyStorageFileHandles() {
        return readOnlyFileHandles;
    }

    public void setReadOnlyFileHandles(int readOnlyFileHandles) {
        this.readOnlyFileHandles = readOnlyFileHandles;
    }

    public long getReadOnlyFileWaitTimeoutMs() {
        return readOnlyFileWaitTimeoutMs;
    }

    public void setReadOnlyFileWaitTimeoutMs(long timeoutMs) {
        this.readOnlyFileWaitTimeoutMs = timeoutMs;
    }

    public int getReadOnlyBackups() {
        return readOnlyBackups;
    }

    public long getReadOnlyCacheSize() {
        return readOnlyCacheSize;
    }

    public void setReadOnlyBackups(int readOnlyBackups) {
        this.readOnlyBackups = readOnlyBackups;
    }

    public boolean isReadOnlyEngineEnabled() {
        return enableReadOnlyEngine;
    }

    public void setEnableReadOnlyEngine(boolean enableReadOnlyEngine) {
        this.enableReadOnlyEngine = enableReadOnlyEngine;
    }

    public boolean isCacheEngineEnabled() {
        return enableCacheEngine;
    }

    public void setEnableCacheEngine(boolean enableCacheEngine) {
        this.enableCacheEngine = enableCacheEngine;
    }

    public boolean isBdbWriteTransactionsEnabled() {
        return bdbWriteTransactions;
    }

    public void setBdbWriteTransactions(boolean bdbWriteTransactions) {
        this.bdbWriteTransactions = bdbWriteTransactions;
    }
}
