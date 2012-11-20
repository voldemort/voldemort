/*
 * Copyright 2008-2012 LinkedIn, Inc
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
import java.util.List;
import java.util.Properties;

import voldemort.client.TimeoutConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.common.OpTimeMap;
import voldemort.common.VoldemortOpCode;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.CacheStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.readonly.BinarySearchStrategy;
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
 * 
 */
public class VoldemortConfig implements Serializable {

    private static final long serialVersionUID = 1;
    public static final String VOLDEMORT_HOME_VAR_NAME = "VOLDEMORT_HOME";
    public static final String VOLDEMORT_CONFIG_DIR = "VOLDEMORT_CONFIG_DIR";
    private static final String VOLDEMORT_NODE_ID_VAR_NAME = "VOLDEMORT_NODE_ID";
    public static int VOLDEMORT_DEFAULT_ADMIN_PORT = 6660;
    public static final long REPORTING_INTERVAL_BYTES = 25 * 1024 * 1024;
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private int nodeId;

    private String voldemortHome;
    private String dataDirectory;
    private String metadataDirectory;

    private long bdbCacheSize;
    private boolean bdbWriteTransactions;
    private boolean bdbFlushTransactions;
    private String bdbDataDirectory;
    private long bdbMaxLogFileSize;
    private int bdbBtreeFanout;
    private long bdbCheckpointBytes;
    private long bdbCheckpointMs;
    private boolean bdbOneEnvPerStore;
    private int bdbCleanerMinFileUtilization;
    private int bdbCleanerMinUtilization;
    private int bdbCleanerLookAheadCacheSize;
    private boolean bdbCheckpointerHighPriority;
    private int bdbCleanerMaxBatchFiles;
    private boolean bdbReadUncommitted;
    private int bdbCleanerThreads;
    private long bdbLockTimeoutMs;
    private int bdbLockNLockTables;
    private int bdbLogFaultReadSize;
    private int bdbLogIteratorReadSize;
    private boolean bdbFairLatches;
    private long bdbStatsCacheTtlMs;
    private boolean bdbExposeSpaceUtilization;
    private long bdbMinimumSharedCache;
    private boolean bdbCleanerLazyMigration;
    private boolean bdbCacheModeEvictLN;
    private boolean bdbMinimizeScanImpact;
    private boolean bdbPrefixKeysWithPartitionId;
    private boolean bdbLevelBasedEviction;
    private boolean bdbProactiveBackgroundMigration;

    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlDatabaseName;
    private String mysqlHost;
    private int mysqlPort;

    private int readOnlyBackups;
    private String readOnlyStorageDir;
    private String readOnlySearchStrategy;
    private int readOnlyDeleteBackupTimeMs;
    private long maxBytesPerSecond;
    private long minBytesPerSecond;
    private long reportingIntervalBytes;
    private int fetcherBufferSize;

    private OpTimeMap testingSlowQueueingDelays;
    private OpTimeMap testingSlowConcurrentDelays;

    private int coreThreads;
    private int maxThreads;

    private int socketTimeoutMs;
    private int socketBufferSize;
    private boolean socketKeepAlive;

    private boolean useNioConnector;
    private int nioConnectorSelectors;
    private int nioAdminConnectorSelectors;
    private int nioAcceptorBacklog;

    private int clientSelectors;
    private int clientRoutingTimeoutMs;
    private TimeoutConfig clientTimeoutConfig;
    private int clientMaxConnectionsPerNode;
    private int clientConnectionTimeoutMs;
    private int clientMaxThreads;
    private int clientThreadIdleMs;
    private int clientMaxQueuedRequests;

    private int schedulerThreads;
    private boolean mayInterruptService;

    private int numScanPermits;

    private RequestFormatType requestFormatType;

    private boolean enableSlop;
    private boolean enableSlopPusherJob;
    private boolean enableRepair;
    private boolean enableGui;
    private boolean enableHttpServer;
    private boolean enableSocketServer;
    private boolean enableAdminServer;
    private boolean enableJmx;
    private boolean enablePipelineRoutedStore;
    private boolean enableVerboseLogging;
    private boolean enableStatTracking;
    private boolean enableServerRouting;
    private boolean enableMetadataChecking;
    private boolean enableNetworkClassLoader;
    private boolean enableGossip;
    private boolean enableRebalanceService;
    private boolean enableJmxClusterName;

    private List<String> storageConfigurations;

    private Props allProps;

    private String slopStoreType;
    private String pusherType;
    private long slopFrequencyMs;
    private long repairStartMs;
    private long slopMaxWriteBytesPerSec;
    private long slopMaxReadBytesPerSec;
    private int slopBatchSize;
    private int slopZonesDownToTerminate;

    private int adminCoreThreads;
    private int adminMaxThreads;
    private int adminStreamBufferSize;
    private int adminSocketTimeout;
    private int adminConnectionTimeout;

    private long streamMaxReadBytesPerSec;
    private long streamMaxWriteBytesPerSec;

    private int gossipInterval;
    private String failureDetectorImplementation;
    private long failureDetectorBannagePeriod;
    private int failureDetectorThreshold;
    private int failureDetectorThresholdCountMinimum;
    private long failureDetectorThresholdInterval;
    private long failureDetectorAsyncRecoveryInterval;
    private volatile List<String> failureDetectorCatastrophicErrorTypes;
    private long failureDetectorRequestLengthThreshold;

    private int retentionCleanupFirstStartTimeInHour;
    private int retentionCleanupScheduledPeriodInHour;
    private int retentionCleanupFirstStartDayOfWeek;
    private boolean retentionCleanupPinStartTime;
    private boolean enforceRetentionPolicyOnRead;
    private boolean deleteExpiredValuesOnRead;

    private int maxRebalancingAttempt;
    private long rebalancingTimeoutSec;
    private int maxParallelStoresRebalancing;
    private boolean rebalancingOptimization;
    private boolean usePartitionScanForRebalance;

    // flag to indicate if we will mlock and pin index pages in memory
    private boolean useMlock;

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
        this.bdbOneEnvPerStore = props.getBoolean("bdb.one.env.per.store", false);
        this.bdbCleanerMinFileUtilization = props.getInt("bdb.cleaner.min.file.utilization", 5);
        this.bdbCleanerMinUtilization = props.getInt("bdb.cleaner.minUtilization", 50);
        this.bdbCleanerThreads = props.getInt("bdb.cleaner.threads", 1);
        this.bdbCleanerLookAheadCacheSize = props.getInt("bdb.cleaner.lookahead.cache.size", 8192);
        this.bdbLockTimeoutMs = props.getLong("bdb.lock.timeout.ms", 500);
        this.bdbLockNLockTables = props.getInt("bdb.lock.nLockTables", 1);
        this.bdbLogFaultReadSize = props.getInt("bdb.log.fault.read.size", 2048);
        this.bdbLogIteratorReadSize = props.getInt("bdb.log.iterator.read.size", 8192);
        this.bdbFairLatches = props.getBoolean("bdb.fair.latches", false);
        this.bdbCheckpointerHighPriority = props.getBoolean("bdb.checkpointer.high.priority", false);
        this.bdbCleanerMaxBatchFiles = props.getInt("bdb.cleaner.max.batch.files", 0);
        this.bdbReadUncommitted = props.getBoolean("bdb.lock.read_uncommitted", true);
        this.bdbStatsCacheTtlMs = props.getLong("bdb.stats.cache.ttl.ms", 5 * Time.MS_PER_SECOND);
        this.bdbExposeSpaceUtilization = props.getBoolean("bdb.expose.space.utilization", true);
        this.bdbMinimumSharedCache = props.getLong("bdb.minimum.shared.cache", 0);
        this.bdbCleanerLazyMigration = props.getBoolean("bdb.cleaner.lazy.migration", true);
        this.bdbCacheModeEvictLN = props.getBoolean("bdb.cache.evictln", false);
        this.bdbMinimizeScanImpact = props.getBoolean("bdb.minimize.scan.impact", false);
        this.bdbPrefixKeysWithPartitionId = props.getBoolean("bdb.prefix.keys.with.partitionid",
                                                             true);
        this.bdbLevelBasedEviction = props.getBoolean("bdb.evict.by.level", false);
        this.bdbProactiveBackgroundMigration = props.getBoolean("bdb.proactive.background.migration",
                                                                false);

        this.readOnlyBackups = props.getInt("readonly.backups", 1);
        this.readOnlySearchStrategy = props.getString("readonly.search.strategy",
                                                      BinarySearchStrategy.class.getName());
        this.readOnlyStorageDir = props.getString("readonly.data.directory", this.dataDirectory
                                                                             + File.separator
                                                                             + "read-only");
        this.readOnlyDeleteBackupTimeMs = props.getInt("readonly.delete.backup.ms", 0);
        this.maxBytesPerSecond = props.getBytes("fetcher.max.bytes.per.sec", 0);
        this.minBytesPerSecond = props.getBytes("fetcher.min.bytes.per.sec", 0);
        this.reportingIntervalBytes = props.getBytes("fetcher.reporting.interval.bytes",
                                                     REPORTING_INTERVAL_BYTES);
        this.fetcherBufferSize = (int) props.getBytes("hdfs.fetcher.buffer.size",
                                                      DEFAULT_BUFFER_SIZE);

        this.setUseMlock(props.getBoolean("readonly.mlock.index", false));

        this.mysqlUsername = props.getString("mysql.user", "root");
        this.mysqlPassword = props.getString("mysql.password", "");
        this.mysqlHost = props.getString("mysql.host", "localhost");
        this.mysqlPort = props.getInt("mysql.port", 3306);
        this.mysqlDatabaseName = props.getString("mysql.database", "voldemort");

        this.testingSlowQueueingDelays = new OpTimeMap(0);
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.GET_OP_CODE,
                                                 props.getInt("testing.slow.queueing.get.ms", 0));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.GET_ALL_OP_CODE,
                                                 props.getInt("testing.slow.queueing.getall.ms", 0));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.GET_VERSION_OP_CODE,
                                                 props.getInt("testing.slow.queueing.getversions.ms",
                                                              0));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.PUT_OP_CODE,
                                                 props.getInt("testing.slow.queueing.put.ms", 0));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.DELETE_OP_CODE,
                                                 props.getInt("testing.slow.queueing.delete.ms", 0));

        this.testingSlowConcurrentDelays = new OpTimeMap(0);
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.GET_OP_CODE,
                                                   props.getInt("testing.slow.concurrent.get.ms", 0));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.GET_ALL_OP_CODE,
                                                   props.getInt("testing.slow.concurrent.getall.ms",
                                                                0));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.GET_VERSION_OP_CODE,
                                                   props.getInt("testing.slow.concurrent.getversions.ms",
                                                                0));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.PUT_OP_CODE,
                                                   props.getInt("testing.slow.concurrent.put.ms", 0));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.DELETE_OP_CODE,
                                                   props.getInt("testing.slow.concurrent.delete.ms",
                                                                0));

        this.maxThreads = props.getInt("max.threads", 100);
        this.coreThreads = props.getInt("core.threads", Math.max(1, maxThreads / 2));

        // Admin client should have less threads but very high buffer size.
        this.adminMaxThreads = props.getInt("admin.max.threads", 20);
        this.adminCoreThreads = props.getInt("admin.core.threads", Math.max(1, adminMaxThreads / 2));
        this.adminStreamBufferSize = (int) props.getBytes("admin.streams.buffer.size",
                                                          10 * 1000 * 1000);
        this.adminConnectionTimeout = props.getInt("admin.client.connection.timeout.sec", 60);
        this.adminSocketTimeout = props.getInt("admin.client.socket.timeout.sec", 24 * 60 * 60);

        this.streamMaxReadBytesPerSec = props.getBytes("stream.read.byte.per.sec", 10 * 1000 * 1000);
        this.streamMaxWriteBytesPerSec = props.getBytes("stream.write.byte.per.sec",
                                                        10 * 1000 * 1000);

        this.socketTimeoutMs = props.getInt("socket.timeout.ms", 5000);
        this.socketBufferSize = (int) props.getBytes("socket.buffer.size", 64 * 1024);
        this.socketKeepAlive = props.getBoolean("socket.keepalive", false);

        this.useNioConnector = props.getBoolean("enable.nio.connector", true);
        this.nioConnectorSelectors = props.getInt("nio.connector.selectors",
                                                  Math.max(8, Runtime.getRuntime()
                                                                     .availableProcessors()));
        this.nioAdminConnectorSelectors = props.getInt("nio.admin.connector.selectors",
                                                       Math.max(8, Runtime.getRuntime()
                                                                          .availableProcessors()));
        // a value <= 0 forces the default to be used
        this.nioAcceptorBacklog = props.getInt("nio.acceptor.backlog", -1);

        this.clientSelectors = props.getInt("client.selectors", 4);
        this.clientMaxConnectionsPerNode = props.getInt("client.max.connections.per.node", 50);
        this.clientConnectionTimeoutMs = props.getInt("client.connection.timeout.ms", 500);
        this.clientRoutingTimeoutMs = props.getInt("client.routing.timeout.ms", 15000);
        this.clientTimeoutConfig = new TimeoutConfig(this.clientRoutingTimeoutMs, false);
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.GET_OP_CODE,
                                                     props.getInt("client.routing.get.timeout.ms",
                                                                  this.clientRoutingTimeoutMs));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE,
                                                     props.getInt("client.routing.getall.timeout.ms",
                                                                  this.clientRoutingTimeoutMs));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.PUT_OP_CODE,
                                                     props.getInt("client.routing.put.timeout.ms",
                                                                  this.clientRoutingTimeoutMs));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE,
                                                     props.getLong("client.routing.getversions.timeout.ms",
                                                                   this.clientTimeoutConfig.getOperationTimeout(VoldemortOpCode.PUT_OP_CODE)));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.DELETE_OP_CODE,
                                                     props.getInt("client.routing.delete.timeout.ms",
                                                                  this.clientRoutingTimeoutMs));
        this.clientTimeoutConfig.setPartialGetAllAllowed(props.getBoolean("client.routing.allow.partial.getall",
                                                                          false));
        this.clientMaxThreads = props.getInt("client.max.threads", 500);
        this.clientThreadIdleMs = props.getInt("client.thread.idle.ms", 100000);
        this.clientMaxQueuedRequests = props.getInt("client.max.queued.requests", 1000);

        this.enableHttpServer = props.getBoolean("http.enable", true);
        this.enableSocketServer = props.getBoolean("socket.enable", true);
        this.enableAdminServer = props.getBoolean("admin.enable", true);
        this.enableJmx = props.getBoolean("jmx.enable", true);
        this.enablePipelineRoutedStore = props.getBoolean("enable.pipeline.routed.store", true);
        this.enableSlop = props.getBoolean("slop.enable", true);
        this.enableSlopPusherJob = props.getBoolean("slop.pusher.enable", true);
        this.slopMaxWriteBytesPerSec = props.getBytes("slop.write.byte.per.sec", 10 * 1000 * 1000);
        this.enableVerboseLogging = props.getBoolean("enable.verbose.logging", true);
        this.enableStatTracking = props.getBoolean("enable.stat.tracking", true);
        this.enableServerRouting = props.getBoolean("enable.server.routing", true);
        this.enableMetadataChecking = props.getBoolean("enable.metadata.checking", true);
        this.enableGossip = props.getBoolean("enable.gossip", false);
        this.enableRebalanceService = props.getBoolean("enable.rebalancing", true);
        this.enableRepair = props.getBoolean("enable.repair", true);
        this.enableJmxClusterName = props.getBoolean("enable.jmx.clustername", false);

        this.gossipInterval = props.getInt("gossip.interval.ms", 30 * 1000);

        this.slopMaxWriteBytesPerSec = props.getBytes("slop.write.byte.per.sec", 10 * 1000 * 1000);
        this.slopMaxReadBytesPerSec = props.getBytes("slop.read.byte.per.sec", 10 * 1000 * 1000);
        this.slopStoreType = props.getString("slop.store.engine", BdbStorageConfiguration.TYPE_NAME);
        this.slopFrequencyMs = props.getLong("slop.frequency.ms", 5 * 60 * 1000);
        this.repairStartMs = props.getLong("repair.start.ms", 24 * 60 * 60 * 1000);
        this.slopBatchSize = props.getInt("slop.batch.size", 100);
        this.pusherType = props.getString("pusher.type", StreamingSlopPusherJob.TYPE_NAME);
        this.slopZonesDownToTerminate = props.getInt("slop.zones.terminate", 0);

        this.schedulerThreads = props.getInt("scheduler.threads", 6);
        this.mayInterruptService = props.getBoolean("service.interruptible", true);

        this.numScanPermits = props.getInt("num.scan.permits", 1);

        this.storageConfigurations = props.getList("storage.configs",
                                                   ImmutableList.of(BdbStorageConfiguration.class.getName(),
                                                                    MysqlStorageConfiguration.class.getName(),
                                                                    InMemoryStorageConfiguration.class.getName(),
                                                                    CacheStorageConfiguration.class.getName(),
                                                                    ReadOnlyStorageConfiguration.class.getName()));

        // start at midnight (0-23)
        this.retentionCleanupFirstStartTimeInHour = props.getInt("retention.cleanup.first.start.hour",
                                                                 0);
        // start next day by default (1=SUN, 2=MON, 3=TUE, 4=WED, 5=THU, 6=FRI,
        // 7=SAT)
        this.retentionCleanupFirstStartDayOfWeek = props.getInt("retention.cleanup.first.start.day",
                                                                Utils.getDayOfTheWeekFromNow(1));
        // repeat every 24 hours
        this.retentionCleanupScheduledPeriodInHour = props.getInt("retention.cleanup.period.hours",
                                                                  24);
        // should the retention job always start at the 'start time' specified
        this.retentionCleanupPinStartTime = props.getBoolean("retention.cleanup.pin.start.time",
                                                             true);
        // should the online reads filter out stale values when reading them ?
        this.enforceRetentionPolicyOnRead = props.getBoolean("enforce.retention.policy.on.read",
                                                             false);
        // should the online reads issue deletes to clear out stale values when
        // reading them?
        this.deleteExpiredValuesOnRead = props.getBoolean("delete.expired.values.on.read", false);

        // save props for access from plugins
        this.allProps = props;

        String requestFormatName = props.getString("request.format",
                                                   RequestFormatType.VOLDEMORT_V1.getCode());
        this.requestFormatType = RequestFormatType.fromCode(requestFormatName);

        // rebalancing parameters
        this.maxRebalancingAttempt = props.getInt("max.rebalancing.attempts", 3);
        this.rebalancingTimeoutSec = props.getLong("rebalancing.timeout.seconds", 10 * 24 * 60 * 60);
        this.maxParallelStoresRebalancing = props.getInt("max.parallel.stores.rebalancing", 3);
        this.rebalancingOptimization = props.getBoolean("rebalancing.optimization", true);
        this.usePartitionScanForRebalance = props.getBoolean("use.partition.scan.for.rebalance",
                                                             true);

        this.failureDetectorImplementation = props.getString("failuredetector.implementation",
                                                             FailureDetectorConfig.DEFAULT_IMPLEMENTATION_CLASS_NAME);

        // We're changing the property from "client.node.bannage.ms" to
        // "failuredetector.bannage.period" so if we have the old one, migrate
        // it over.
        if(props.containsKey("client.node.bannage.ms")
           && !props.containsKey("failuredetector.bannage.period")) {
            props.put("failuredetector.bannage.period", props.get("client.node.bannage.ms"));
        }

        this.failureDetectorBannagePeriod = props.getLong("failuredetector.bannage.period",
                                                          FailureDetectorConfig.DEFAULT_BANNAGE_PERIOD);
        this.failureDetectorThreshold = props.getInt("failuredetector.threshold",
                                                     FailureDetectorConfig.DEFAULT_THRESHOLD);
        this.failureDetectorThresholdCountMinimum = props.getInt("failuredetector.threshold.countminimum",
                                                                 FailureDetectorConfig.DEFAULT_THRESHOLD_COUNT_MINIMUM);
        this.failureDetectorThresholdInterval = props.getLong("failuredetector.threshold.interval",
                                                              FailureDetectorConfig.DEFAULT_THRESHOLD_INTERVAL);
        this.failureDetectorAsyncRecoveryInterval = props.getLong("failuredetector.asyncrecovery.interval",
                                                                  FailureDetectorConfig.DEFAULT_ASYNC_RECOVERY_INTERVAL);
        this.failureDetectorCatastrophicErrorTypes = props.getList("failuredetector.catastrophic.error.types",
                                                                   FailureDetectorConfig.DEFAULT_CATASTROPHIC_ERROR_TYPES);
        this.failureDetectorRequestLengthThreshold = props.getLong("failuredetector.request.length.threshold",
                                                                   getSocketTimeoutMs());

        // network class loader disable by default.
        this.enableNetworkClassLoader = props.getBoolean("enable.network.classloader", false);

        validateParams();
    }

    private void validateParams() {
        if(coreThreads < 0)
            throw new IllegalArgumentException("core.threads cannot be less than 1");
        else if(coreThreads > maxThreads)
            throw new IllegalArgumentException("core.threads cannot be greater than max.threads.");
        if(maxThreads < 1)
            throw new ConfigurationException("max.threads cannot be less than 1.");
        if(slopFrequencyMs < 1)
            throw new ConfigurationException("slop.frequency.ms cannot be less than 1.");
        if(socketTimeoutMs < 0)
            throw new ConfigurationException("socket.timeout.ms must be 0 or more ms.");
        if(clientSelectors < 1)
            throw new ConfigurationException("client.selectors must be 1 or more.");
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

        String voldemortConfigDir = System.getenv(VoldemortConfig.VOLDEMORT_CONFIG_DIR);
        if(voldemortConfigDir != null) {
            if(!Utils.isReadableDir(voldemortConfigDir))
                throw new ConfigurationException("Attempt to load configuration from VOLDEMORT_CONFIG_DIR, "
                                                 + voldemortConfigDir
                                                 + " failed. That is not a readable directory.");
        }
        return loadFromVoldemortHome(voldemortHome, voldemortConfigDir);
    }

    public static VoldemortConfig loadFromVoldemortHome(String voldemortHome) {
        String voldemortConfigDir = voldemortHome + File.separator + "config";
        return loadFromVoldemortHome(voldemortHome, voldemortConfigDir);

    }

    public static VoldemortConfig loadFromVoldemortHome(String voldemortHome,
                                                        String voldemortConfigDir) {
        if(!Utils.isReadableDir(voldemortHome))
            throw new ConfigurationException("Attempt to load configuration from VOLDEMORT_HOME, "
                                             + voldemortHome
                                             + " failed. That is not a readable directory.");

        if(voldemortConfigDir == null) {
            voldemortConfigDir = voldemortHome + File.separator + "config";
        }
        String propertiesFile = voldemortConfigDir + File.separator + "server.properties";
        if(!Utils.isReadableFile(propertiesFile))
            throw new ConfigurationException(propertiesFile
                                             + " is not a readable configuration file.");

        Props properties = null;
        try {
            properties = new Props(new File(propertiesFile));
            properties.put("voldemort.home", voldemortHome);
            properties.put("metadata.directory", voldemortConfigDir);
        } catch(IOException e) {
            throw new ConfigurationException(e);
        }

        return new VoldemortConfig(properties);
    }

    /**
     * The interval at which gossip is run to exchange metadata
     */
    public int getGossipInterval() {
        return gossipInterval;
    }

    public void setGossipInterval(int gossipInterval) {
        this.gossipInterval = gossipInterval;
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
     * <ul>
     * <li>Property : "voldemort.home"</li>
     * <li>Default : VOLDEMORT_HOME environment variable</li>
     * </ul>
     */
    public String getVoldemortHome() {
        return voldemortHome;
    }

    public void setVoldemortHome(String voldemortHome) {
        this.voldemortHome = voldemortHome;
    }

    /**
     * The directory name given by "data.directory" default: voldemort.home/data
     * 
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
     * The size of BDB Cache to hold portions of the BTree.
     * 
     * <ul>
     * <li>Property : "bdb.cache.size"</li>
     * <li>Default : 200MB</li>
     * </ul>
     */
    public long getBdbCacheSize() {
        return bdbCacheSize;
    }

    public void setBdbCacheSize(int bdbCacheSize) {
        this.bdbCacheSize = bdbCacheSize;
    }

    /**
     * This parameter controls whether we expose space utilization via MBean. If
     * set to false, stat will always return 0;
     * 
     * <ul>
     * <li>Property : "bdb.expose.space.utilization"</li>
     * <li>Default : true</li>
     * </ul>
     */
    public boolean getBdbExposeSpaceUtilization() {
        return bdbExposeSpaceUtilization;
    }

    public void setBdbExposeSpaceUtilization(boolean bdbExposeSpaceUtilization) {
        this.bdbExposeSpaceUtilization = bdbExposeSpaceUtilization;
    }

    /**
     * If true then sync transactions to disk immediately.
     * 
     * <ul>
     * <li>Property : "bdb.flush.transactions"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     */
    public boolean isBdbFlushTransactionsEnabled() {
        return bdbFlushTransactions;
    }

    public void setBdbFlushTransactions(boolean bdbSyncTransactions) {
        this.bdbFlushTransactions = bdbSyncTransactions;
    }

    /**
     * The directory in which bdb data is stored.
     * 
     * <ul>
     * <li>Property : "bdb.data.directory"</li>
     * <li>Default : data.directory/bdb</li>
     * </ul>
     */
    public String getBdbDataDirectory() {
        return bdbDataDirectory;
    }

    public void setBdbDataDirectory(String bdbDataDirectory) {
        this.bdbDataDirectory = bdbDataDirectory;
    }

    /**
     * The maximum size of a single .jdb log file in bytes.
     * 
     * <ul>
     * <li>Property : "bdb.max.logfile.size"</li>
     * <li>Default : 60MB</li>
     * </ul>
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
     * If true, the checkpointer uses more resources in order to complete the
     * checkpoint in a shorter time interval.
     * 
     * <ul>
     * <li>property: "bdb.checkpointer.high.priority"</li>
     * <li>default: false</li>
     * </ul>
     */
    public boolean getBdbCheckpointerHighPriority() {
        return bdbCheckpointerHighPriority;
    }

    public final void setBdbCheckpointerHighPriority(boolean bdbCheckpointerHighPriority) {
        this.bdbCheckpointerHighPriority = bdbCheckpointerHighPriority;
    }

    /**
     * The maximum number of log files in the cleaner's backlog, or zero if
     * there is no limit
     * 
     * <ul>
     * <li>property: "bdb.cleaner.max.batch.files"</li>
     * <li>default: 0</li>
     * <li>minimum: 0</li>
     * <li>maximum: 100000</li>
     * </ul>
     */
    public int getBdbCleanerMaxBatchFiles() {
        return bdbCleanerMaxBatchFiles;
    }

    public final void setBdbCleanerMaxBatchFiles(int bdbCleanerMaxBatchFiles) {
        if(bdbCleanerMaxBatchFiles < 0 || bdbCleanerMaxBatchFiles > 100000)
            throw new IllegalArgumentException("bdbCleanerMaxBatchFiles should be between 0 and 100000 (both inclusive)");
        this.bdbCleanerMaxBatchFiles = bdbCleanerMaxBatchFiles;
    }

    /**
     * 
     * The number of cleaner threads
     * 
     * <ul>
     * <li>property: "bdb.cleaner.threads"</li>
     * <li>default: 1</li>
     * <li>minimum: 1</li>
     * </ul>
     */
    public int getBdbCleanerThreads() {
        return bdbCleanerThreads;
    }

    public final void setBdbCleanerThreads(int bdbCleanerThreads) {
        if(bdbCleanerThreads <= 0)
            throw new IllegalArgumentException("bdbCleanerThreads should be greater than 0");
        this.bdbCleanerThreads = bdbCleanerThreads;
    }

    /**
     * Buffer size used by cleaner to fetch BTree nodes during cleaning.
     * 
     * <ul>
     * <li>property: "bdb.cleaner.lookahead.cache.size"</li>
     * <li>default: 8192</li>
     * </ul>
     * 
     * @return
     */
    public int getBdbCleanerLookAheadCacheSize() {
        return bdbCleanerLookAheadCacheSize;
    }

    public final void setBdbCleanerLookAheadCacheSize(int bdbCleanerLookAheadCacheSize) {
        if(bdbCleanerLookAheadCacheSize < 0)
            throw new IllegalArgumentException("bdbCleanerLookAheadCacheSize should be at least 0");
        this.bdbCleanerLookAheadCacheSize = bdbCleanerLookAheadCacheSize;
    }

    /**
     * 
     * The lock timeout for all transactional and non-transactional operations.
     * Value of zero disables lock timeouts i.e. a deadlock scenario will block
     * forever. High locktimeout combined with a highly concurrent workload,
     * might have adverse impact on latency for all stores
     * 
     * <ul>
     * <li>property: "bdb.lock.timeout.ms"</li>
     * <li>default: 500</li>
     * <li>minimum: 0</li>
     * <li>maximum: 75 * 60 * 1000</li>
     * </ul>
     */
    public long getBdbLockTimeoutMs() {
        return bdbLockTimeoutMs;
    }

    public final void setBdbLockTimeoutMs(long bdbLockTimeoutMs) {
        if(bdbLockTimeoutMs < 0)
            throw new IllegalArgumentException("bdbLockTimeoutMs should be greater than 0");
        this.bdbLockTimeoutMs = bdbLockTimeoutMs;
    }

    /**
     * The size of the lock table used by BDB JE
     * 
     * <ul>
     * <li>Property : bdb.lock.nLockTables"</li>
     * <li>Default : 1</li>
     * </ul>
     * 
     * @return
     */
    public int getBdbLockNLockTables() {
        return bdbLockNLockTables;
    }

    public void setBdbLockNLockTables(int bdbLockNLockTables) {
        if(bdbLockNLockTables < 1 || bdbLockNLockTables > 32767)
            throw new IllegalArgumentException("bdbLockNLockTables should be greater than 0 and "
                                               + "less than 32767");
        this.bdbLockNLockTables = bdbLockNLockTables;
    }

    /**
     * Buffer for faulting in objects from disk
     * 
     * <ul>
     * <li>Property : "bdb.log.fault.read.size"</li>
     * <li>Default : 2048</li>
     * </ul>
     * 
     * @return
     */
    public int getBdbLogFaultReadSize() {
        return bdbLogFaultReadSize;
    }

    public void setBdbLogFaultReadSize(int bdbLogFaultReadSize) {
        this.bdbLogFaultReadSize = bdbLogFaultReadSize;
    }

    /**
     * Buffer size used by BDB JE for reading the log eg: Cleaning.
     * 
     * <ul>
     * <li>Property : "bdb.log.iterator.read.size"</li>
     * <li>Default : 8192</li>
     * </ul>
     * 
     * @return
     */
    public int getBdbLogIteratorReadSize() {
        return bdbLogIteratorReadSize;
    }

    public void setBdbLogIteratorReadSize(int bdbLogIteratorReadSize) {
        this.bdbLogIteratorReadSize = bdbLogIteratorReadSize;
    }

    /**
     * Controls whether BDB JE should use latches instead of synchronized blocks
     * 
     * <ul>
     * <li>Property : "bdb.fair.latches"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbFairLatches() {
        return bdbFairLatches;
    }

    public void setBdbFairLatches(boolean bdbFairLatches) {
        this.bdbFairLatches = bdbFairLatches;
    }

    /**
     * If true, BDB JE get() will not be blocked by put()
     * 
     * <ul>
     * <li>Property : "bdb.lock.read_uncommitted"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbReadUncommitted() {
        return bdbReadUncommitted;
    }

    public void setBdbReadUncommitted(boolean bdbReadUncommitted) {
        this.bdbReadUncommitted = bdbReadUncommitted;
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
     * The btree node fanout. Given by "". default: 512
     * 
     * <ul>
     * <li>property: "bdb.btree.fanout"</li>
     * <li>default: 512</li>
     * </ul>
     */
    public int getBdbBtreeFanout() {
        return this.bdbBtreeFanout;
    }

    public void setBdbBtreeFanout(int bdbBtreeFanout) {
        this.bdbBtreeFanout = bdbBtreeFanout;
    }

    /**
     * If true, Cleaner offloads some work to application threads, to keep up
     * with the write rate. Side effect is that data is staged on the JVM till
     * it is flushed down by Checkpointer, hence not GC friendly (Will cause
     * promotions)
     * 
     * <ul>
     * <li>property: "bdb.cleaner.lazy.migration"</li>
     * <li>default : true</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbCleanerLazyMigration() {
        return bdbCleanerLazyMigration;
    }

    public final void setBdbCleanerLazyMigration(boolean bdbCleanerLazyMigration) {
        this.bdbCleanerLazyMigration = bdbCleanerLazyMigration;
    }

    /**
     * If true, BDB will not cache data in the JVM.
     * 
     * <ul>
     * <li>Property : "bdb.cache.evictln"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbCacheModeEvictLN() {
        return bdbCacheModeEvictLN;
    }

    public void setBdbCacheModeEvictLN(boolean bdbCacheModeEvictLN) {
        this.bdbCacheModeEvictLN = bdbCacheModeEvictLN;
    }

    /**
     * If true, attempts are made to minimize impact to BDB cache during scan
     * jobs
     * 
     * <ul>
     * <li>Property : "bdb.minimize.scan.impact"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbMinimizeScanImpact() {
        return bdbMinimizeScanImpact;
    }

    public void setBdbMinimizeScanImpact(boolean bdbMinimizeScanImpact) {
        this.bdbMinimizeScanImpact = bdbMinimizeScanImpact;
    }

    /**
     * Controls persistence mode for BDB JE Transaction. By default, we rely on
     * the checkpointer to flush the writes
     * 
     * <ul>
     * <li>Property : "bdb.write.transactions"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @return
     */
    public boolean isBdbWriteTransactionsEnabled() {
        return bdbWriteTransactions;
    }

    public void setBdbWriteTransactions(boolean bdbWriteTransactions) {
        this.bdbWriteTransactions = bdbWriteTransactions;
    }

    /**
     * If true, use separate BDB JE environment per store
     * 
     * <ul>
     * <li>Property : "bdb.one.env.per.store"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @param bdbOneEnvPerStore
     */
    public void setBdbOneEnvPerStore(boolean bdbOneEnvPerStore) {
        this.bdbOneEnvPerStore = bdbOneEnvPerStore;
    }

    public boolean isBdbOneEnvPerStore() {
        return bdbOneEnvPerStore;
    }

    /**
     * If true, keys will be prefixed by the partition Id on disk. This can
     * dramatically speed up rebalancing, restore operations, at the cost of 2
     * bytes of extra storage per key
     * 
     * <ul>
     * <li>Property : "bdb.prefix.keys.with.partitionid"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbPrefixKeysWithPartitionId() {
        return bdbPrefixKeysWithPartitionId;
    }

    public void setBdbPrefixKeysWithPartitionId(boolean bdbPrefixKeysWithPartitionId) {
        this.bdbPrefixKeysWithPartitionId = bdbPrefixKeysWithPartitionId;
    }

    /**
     * Checkpointer is woken up and a checkpoint is written once this many bytes
     * have been logged
     * 
     * <ul>
     * <li>Property : "bdb.checkpoint.interval.bytes"</li>
     * <li>Default : 20MB</li>
     * </ul>
     * 
     * @return
     */
    public long getBdbCheckpointBytes() {
        return this.bdbCheckpointBytes;
    }

    public void setBdbCheckpointBytes(long bdbCheckpointBytes) {
        this.bdbCheckpointBytes = bdbCheckpointBytes;
    }

    /**
     * BDB JE Checkpointer wakes up whenever this time period elapses
     * 
     * <ul>
     * <li>Property : "bdb.checkpoint.interval.ms"</li>
     * <li>Default : 30s or 30000 ms</li>
     * </ul>
     * 
     * @return
     */
    public long getBdbCheckpointMs() {
        return this.bdbCheckpointMs;
    }

    public void setBdbCheckpointMs(long bdbCheckpointMs) {
        this.bdbCheckpointMs = bdbCheckpointMs;
    }

    /**
     * Interval to reuse environment stats fetched from BDB. Once the interval
     * expires, a fresh call will be made
     * 
     * <ul>
     * <li>Property : "bdb.stats.cache.ttl.ms"</li>
     * <li>Default : 5s</li>
     * </ul>
     * 
     * @return
     */
    public long getBdbStatsCacheTtlMs() {
        return this.bdbStatsCacheTtlMs;
    }

    public void setBdbStatsCacheTtlMs(long statsCacheTtlMs) {
        this.bdbStatsCacheTtlMs = statsCacheTtlMs;
    }

    /**
     * When using partitioned caches, this parameter controls the minimum amount
     * of memory reserved for the global pool. Any memory-footprint reservation
     * that will break this guarantee will fail.
     * 
     * <ul>
     * <li>Property : "bdb.minimum.shared.cache"</li>
     * <li>Default : 0</li>
     * </ul>
     * 
     * @return
     */
    public long getBdbMinimumSharedCache() {
        return this.bdbMinimumSharedCache;
    }

    public void setBdbMinimumSharedCache(long minimumSharedCache) {
        this.bdbMinimumSharedCache = minimumSharedCache;
    }

    /**
     * Controls if BDB JE cache eviction happens based on LRU or by BTree level.
     * 
     * <ul>
     * <li>Property : "bdb.evict.by.level"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @return
     */
    public boolean isBdbLevelBasedEviction() {
        return bdbLevelBasedEviction;
    }

    public void setBdbLevelBasedEviction(boolean bdbLevelBasedEviction) {
        this.bdbLevelBasedEviction = bdbLevelBasedEviction;
    }

    /**
     * Exposes BDB JE EnvironmentConfig.CLEANER_PROACTIVE_BACKGROUND_MIGRATION.
     * 
     * <ul>
     * <li>Property : "bdb.proactive.background.migration"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     * @return
     */
    public boolean getBdbProactiveBackgroundMigration() {
        return bdbProactiveBackgroundMigration;
    }

    public void setBdbProactiveBackgroundMigration(boolean bdbProactiveBackgroundMigration) {
        this.bdbProactiveBackgroundMigration = bdbProactiveBackgroundMigration;
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

    public long getStreamMaxReadBytesPerSec() {
        return streamMaxReadBytesPerSec;
    }

    public void setStreamMaxReadBytesPerSec(long streamMaxReadBytesPerSec) {
        this.streamMaxReadBytesPerSec = streamMaxReadBytesPerSec;
    }

    public long getStreamMaxWriteBytesPerSec() {
        return streamMaxWriteBytesPerSec;
    }

    public void setStreamMaxWriteBytesPerSec(long streamMaxWriteBytesPerSec) {
        this.streamMaxWriteBytesPerSec = streamMaxWriteBytesPerSec;
    }

    public long getSlopMaxWriteBytesPerSec() {
        return slopMaxWriteBytesPerSec;
    }

    public void setSlopMaxWriteBytesPerSec(long slopMaxWriteBytesPerSec) {
        this.slopMaxWriteBytesPerSec = slopMaxWriteBytesPerSec;
    }

    public long getSlopMaxReadBytesPerSec() {
        return slopMaxReadBytesPerSec;
    }

    public void setSlopMaxReadBytesPerSec(long slopMaxReadBytesPerSec) {
        this.slopMaxReadBytesPerSec = slopMaxReadBytesPerSec;
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

    public boolean isPipelineRoutedStoreEnabled() {
        return enablePipelineRoutedStore;
    }

    public void setEnablePipelineRoutedStore(boolean enablePipelineRoutedStore) {
        this.enablePipelineRoutedStore = enablePipelineRoutedStore;
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

    /**
     * The underlying store type which will be used to store slops. Defaults to
     * Bdb
     */
    public String getSlopStoreType() {
        return slopStoreType;
    }

    public void setSlopStoreType(String slopStoreType) {
        this.slopStoreType = slopStoreType;
    }

    /**
     * The type of streaming job we would want to use to send hints. Defaults to
     * streaming
     */
    public String getPusherType() {
        return this.pusherType;
    }

    public void setPusherType(String pusherType) {
        this.pusherType = pusherType;
    }

    /**
     * Number of zones declared down before we terminate the pusher job
     */
    public int getSlopZonesDownToTerminate() {
        return this.slopZonesDownToTerminate;
    }

    public void setSlopZonesDownToTerminate(int slopZonesDownToTerminate) {
        this.slopZonesDownToTerminate = slopZonesDownToTerminate;
    }

    /**
     * Returns the size of the batch used while streaming slops
     */
    public int getSlopBatchSize() {
        return this.slopBatchSize;
    }

    public void setSlopBatchSize(int slopBatchSize) {
        this.slopBatchSize = slopBatchSize;
    }

    public int getSocketTimeoutMs() {
        return this.socketTimeoutMs;
    }

    public long getSlopFrequencyMs() {
        return this.slopFrequencyMs;
    }

    public void setSlopFrequencyMs(long slopFrequencyMs) {
        this.slopFrequencyMs = slopFrequencyMs;
    }

    public long getRepairStartMs() {
        return this.repairStartMs;
    }

    public void setRepairStartMs(long repairStartMs) {
        this.repairStartMs = repairStartMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getClientSelectors() {
        return clientSelectors;
    }

    public void setClientSelectors(int clientSelectors) {
        this.clientSelectors = clientSelectors;
    }

    public int getClientRoutingTimeoutMs() {
        return this.clientRoutingTimeoutMs;
    }

    public void setClientRoutingTimeoutMs(int routingTimeoutMs) {
        this.clientRoutingTimeoutMs = routingTimeoutMs;
    }

    public TimeoutConfig getTimeoutConfig() {
        return this.clientTimeoutConfig;
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

    /**
     * @deprecated Use {@link #getFailureDetectorBannagePeriod()} instead
     */

    @Deprecated
    public int getClientNodeBannageMs() {
        return (int) failureDetectorBannagePeriod;
    }

    /**
     * @deprecated Use {@link #setFailureDetectorBannagePeriod(long)} instead
     */

    @Deprecated
    public void setClientNodeBannageMs(int nodeBannageMs) {
        this.failureDetectorBannagePeriod = nodeBannageMs;
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

    public boolean isSlopPusherJobEnabled() {
        return enableSlopPusherJob;
    }

    public void setEnableSlopPusherJob(boolean enableSlopPusherJob) {
        this.enableSlopPusherJob = enableSlopPusherJob;
    }

    public boolean isRepairEnabled() {
        return this.enableRepair;
    }

    public void setEnableRepair(boolean enableRepair) {
        this.enableRepair = enableRepair;
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

    public int getSchedulerThreads() {
        return schedulerThreads;
    }

    public void setSchedulerThreads(int schedulerThreads) {
        this.schedulerThreads = schedulerThreads;
    }

    public boolean canInterruptService() {
        return mayInterruptService;
    }

    public void setInterruptible(boolean canInterrupt) {
        this.mayInterruptService = canInterrupt;
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

    /**
     * Amount of time we will wait before we start deleting the backup. This
     * happens during swaps when old backups need to be deleted. Some delay is
     * required so that we don't cause a sudden increase of IOPs during swap.
     * 
     * @return The start time in ms
     */
    public int getReadOnlyDeleteBackupMs() {
        return readOnlyDeleteBackupTimeMs;
    }

    public void setReadOnlyDeleteBackupMs(int readOnlyDeleteBackupTimeMs) {
        this.readOnlyDeleteBackupTimeMs = readOnlyDeleteBackupTimeMs;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public boolean getSocketKeepAlive() {
        return this.socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean on) {
        this.socketKeepAlive = on;
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

    public int getNioAdminConnectorSelectors() {
        return nioAdminConnectorSelectors;
    }

    public void setNioAdminConnectorSelectors(int nioAdminConnectorSelectors) {
        this.nioAdminConnectorSelectors = nioAdminConnectorSelectors;
    }

    public int getNioAcceptorBacklog() {
        return nioAcceptorBacklog;
    }

    public void setNioAcceptorBacklog(int nioAcceptorBacklog) {
        this.nioAcceptorBacklog = nioAcceptorBacklog;
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

    public int getNumScanPermits() {
        return numScanPermits;
    }

    public void setNumScanPermits(int numScanPermits) {
        this.numScanPermits = numScanPermits;
    }

    public String getFailureDetectorImplementation() {
        return failureDetectorImplementation;
    }

    public void setFailureDetectorImplementation(String failureDetectorImplementation) {
        this.failureDetectorImplementation = failureDetectorImplementation;
    }

    public long getFailureDetectorBannagePeriod() {
        return failureDetectorBannagePeriod;
    }

    public void setFailureDetectorBannagePeriod(long failureDetectorBannagePeriod) {
        this.failureDetectorBannagePeriod = failureDetectorBannagePeriod;
    }

    public int getFailureDetectorThreshold() {
        return failureDetectorThreshold;
    }

    public void setFailureDetectorThreshold(int failureDetectorThreshold) {
        this.failureDetectorThreshold = failureDetectorThreshold;
    }

    public int getFailureDetectorThresholdCountMinimum() {
        return failureDetectorThresholdCountMinimum;
    }

    public void setFailureDetectorThresholdCountMinimum(int failureDetectorThresholdCountMinimum) {
        this.failureDetectorThresholdCountMinimum = failureDetectorThresholdCountMinimum;
    }

    public long getFailureDetectorThresholdInterval() {
        return failureDetectorThresholdInterval;
    }

    public void setFailureDetectorThresholdInterval(long failureDetectorThresholdInterval) {
        this.failureDetectorThresholdInterval = failureDetectorThresholdInterval;
    }

    public long getFailureDetectorAsyncRecoveryInterval() {
        return failureDetectorAsyncRecoveryInterval;
    }

    public void setFailureDetectorAsyncRecoveryInterval(long failureDetectorAsyncRecoveryInterval) {
        this.failureDetectorAsyncRecoveryInterval = failureDetectorAsyncRecoveryInterval;
    }

    public List<String> getFailureDetectorCatastrophicErrorTypes() {
        return failureDetectorCatastrophicErrorTypes;
    }

    public void setFailureDetectorCatastrophicErrorTypes(List<String> failureDetectorCatastrophicErrorTypes) {
        this.failureDetectorCatastrophicErrorTypes = failureDetectorCatastrophicErrorTypes;
    }

    public long getFailureDetectorRequestLengthThreshold() {
        return failureDetectorRequestLengthThreshold;
    }

    public void setFailureDetectorRequestLengthThreshold(long failureDetectorRequestLengthThreshold) {
        this.failureDetectorRequestLengthThreshold = failureDetectorRequestLengthThreshold;
    }

    public int getRetentionCleanupFirstStartTimeInHour() {
        return retentionCleanupFirstStartTimeInHour;
    }

    public void setRetentionCleanupFirstStartTimeInHour(int retentionCleanupFirstStartTimeInHour) {
        this.retentionCleanupFirstStartTimeInHour = retentionCleanupFirstStartTimeInHour;
    }

    public int getRetentionCleanupFirstStartDayOfWeek() {
        return retentionCleanupFirstStartDayOfWeek;
    }

    public void setRetentionCleanupFirstStartDayOfWeek(int retentionCleanupFirstStartDayOfWeek) {
        this.retentionCleanupFirstStartDayOfWeek = retentionCleanupFirstStartDayOfWeek;
    }

    public int getRetentionCleanupScheduledPeriodInHour() {
        return retentionCleanupScheduledPeriodInHour;
    }

    public void setRetentionCleanupScheduledPeriodInHour(int retentionCleanupScheduledPeriodInHour) {
        this.retentionCleanupScheduledPeriodInHour = retentionCleanupScheduledPeriodInHour;
    }

    public boolean getRetentionCleanupPinStartTime() {
        return retentionCleanupPinStartTime;
    }

    public void setRetentionCleanupPinStartTime(boolean retentionCleanupFixStartTime) {
        this.retentionCleanupPinStartTime = retentionCleanupFixStartTime;
    }

    public boolean isEnforceRetentionPolicyOnRead() {
        return enforceRetentionPolicyOnRead;
    }

    public void setEnforceRetentionPolicyOnRead(boolean enforceRetentionPolicyOnRead) {
        this.enforceRetentionPolicyOnRead = enforceRetentionPolicyOnRead;
    }

    public boolean isDeleteExpiredValuesOnRead() {
        return deleteExpiredValuesOnRead;
    }

    public void setDeleteExpiredValuesOnRead(boolean deleteExpiredValuesOnRead) {
        this.deleteExpiredValuesOnRead = deleteExpiredValuesOnRead;
    }

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

    public void setMaxRebalancingAttempt(int maxRebalancingAttempt) {
        this.maxRebalancingAttempt = maxRebalancingAttempt;
    }

    public int getMaxRebalancingAttempt() {
        return this.maxRebalancingAttempt;
    }

    public long getRebalancingTimeoutSec() {
        return rebalancingTimeoutSec;
    }

    public void setRebalancingTimeoutSec(long rebalancingTimeoutSec) {
        this.rebalancingTimeoutSec = rebalancingTimeoutSec;
    }

    public VoldemortConfig(int nodeId, String voldemortHome) {
        this(new Props().with("node.id", nodeId).with("voldemort.home", voldemortHome));
    }

    public boolean isGossipEnabled() {
        return enableGossip;
    }

    public void setEnableGossip(boolean enableGossip) {
        this.enableGossip = enableGossip;
    }

    public String getReadOnlySearchStrategy() {
        return readOnlySearchStrategy;
    }

    public long getMaxBytesPerSecond() {
        return maxBytesPerSecond;
    }

    public void setMaxBytesPerSecond(long maxBytesPerSecond) {
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    public long getMinBytesPerSecond() {
        return minBytesPerSecond;
    }

    public void setMinBytesPerSecond(long minBytesPerSecond) {
        this.minBytesPerSecond = minBytesPerSecond;
    }

    public long getReportingIntervalBytes() {
        return reportingIntervalBytes;
    }

    public void setReportingIntervalBytes(long reportingIntervalBytes) {
        this.reportingIntervalBytes = reportingIntervalBytes;
    }

    public int getFetcherBufferSize() {
        return fetcherBufferSize;
    }

    public void setFetcherBufferSize(int fetcherBufferSize) {
        this.fetcherBufferSize = fetcherBufferSize;
    }

    public void setReadOnlySearchStrategy(String readOnlySearchStrategy) {
        this.readOnlySearchStrategy = readOnlySearchStrategy;
    }

    public boolean isNetworkClassLoaderEnabled() {
        return enableNetworkClassLoader;
    }

    public void setEnableNetworkClassLoader(boolean enableNetworkClassLoader) {
        this.enableNetworkClassLoader = enableNetworkClassLoader;
    }

    public void setEnableRebalanceService(boolean enableRebalanceService) {
        this.enableRebalanceService = enableRebalanceService;
    }

    public boolean isEnableRebalanceService() {
        return enableRebalanceService;
    }

    public int getMaxParallelStoresRebalancing() {
        return maxParallelStoresRebalancing;
    }

    public void setMaxParallelStoresRebalancing(int maxParallelStoresRebalancing) {
        this.maxParallelStoresRebalancing = maxParallelStoresRebalancing;
    }

    public boolean getRebalancingOptimization() {
        return rebalancingOptimization;
    }

    public void setMaxParallelStoresRebalancing(boolean rebalancingOptimization) {
        this.rebalancingOptimization = rebalancingOptimization;
    }

    public boolean usePartitionScanForRebalance() {
        return usePartitionScanForRebalance;
    }

    public void setUsePartitionScanForRebalance(boolean usePartitionScanForRebalance) {
        this.usePartitionScanForRebalance = usePartitionScanForRebalance;
    }

    public boolean isEnableJmxClusterName() {
        return enableJmxClusterName;
    }

    public void setEnableJmxClusterName(boolean enableJmxClusterName) {
        this.enableJmxClusterName = enableJmxClusterName;
    }

    public OpTimeMap testingGetSlowQueueingDelays() {
        return this.testingSlowQueueingDelays;
    }

    public OpTimeMap testingGetSlowConcurrentDelays() {
        return this.testingSlowConcurrentDelays;
    }

    public boolean isUseMlock() {
        return useMlock;
    }

    public void setUseMlock(boolean useMlock) {
        this.useMlock = useMlock;
    }
}
