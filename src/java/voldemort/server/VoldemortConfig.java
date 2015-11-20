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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.TimeoutConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.pb.VAdminProto.VoldemortFilter;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.common.OpTimeMap;
import voldemort.common.VoldemortOpCode;
import voldemort.common.service.SchedulerService;
import voldemort.rest.server.RestService;
import voldemort.server.http.HttpService;
import voldemort.server.niosocket.NioSocketService;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.slop.BlockingSlopPusherJob;
import voldemort.server.scheduler.slop.SlopPurgeJob;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.server.storage.prunejob.VersionedPutPruneJob;
import voldemort.server.storage.repairjob.RepairJob;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StorageEngine;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.invalidmetadata.InvalidMetadataCheckingStore;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.CacheStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.readonly.BinarySearchStrategy;
import voldemort.store.readonly.InterpolationSearchStrategy;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.rocksdb.RocksDbStorageConfiguration;
import voldemort.store.stats.StatTrackingStore;
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
    public static final int DEFAULT_FETCHER_BUFFER_SIZE = 64 * 1024;
    public static final int DEFAULT_FETCHER_SOCKET_TIMEOUT = 1000 * 60 * 30; // 30 minutes
    public static final int DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS = 1000;

    // -1 represents no storage space quota constraint
    public static final long DEFAULT_STORAGE_SPACE_QUOTA_IN_KB = -1L;
    
    // Kerberos support for read-only fetches (constants)
    public static final String DEFAULT_KERBEROS_PRINCIPAL = "voldemrt";
    public static final String DEFAULT_KEYTAB_PATH = "/voldemrt.headless.keytab";
    private static final String DEFAULT_KERBEROS_KDC = "";
    private static final String DEFAULT_KERBEROS_REALM = "";
    private static final String DEFAULT_FILE_FETCHER_CLASS = null; // FIXME: Some unit tests fail without this default.
    private static final String DEFAULT_RO_COMPRESSION_CODEC = "NO_CODEC";
    public static final int DEFAULT_RO_MAX_VALUE_BUFFER_ALLOCATION_SIZE = 25 * 1024 * 1024;

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
    private int bdbMaxDelta;
    private int bdbBinDelta;
    private long bdbCheckpointBytes;
    private long bdbCheckpointMs;
    private boolean bdbOneEnvPerStore;
    private int bdbCleanerMinFileUtilization;
    private int bdbCleanerMinUtilization;
    private int bdbCleanerLookAheadCacheSize;
    private long bdbCleanerBytesInterval;
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
    private boolean bdbCheckpointerOffForBatchWrites;
    private boolean bdbCleanerFetchObsoleteSize;
    private boolean bdbCleanerAdjustUtilization;
    private boolean bdbRecoveryForceCheckpoint;
    private String bdbRawPropertyString;

    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlDatabaseName;
    private String mysqlHost;
    private int mysqlPort;

    private String rocksdbDataDirectory;
    private boolean rocksdbPrefixKeysWithPartitionId;
    private boolean rocksdbEnableReadLocks;
    // Options prefixed with the following two values can be used to tune RocksDB performance and are passed directly
    // to the RocksDB configuration code. See the RocksDB documentation for details:
    //   https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
    //   https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    public static final String ROCKSDB_DB_OPTIONS = "rocksdb.db.options.";
    public static final String ROCKSDB_CF_OPTIONS = "rocksdb.cf.options.";

    private int numReadOnlyVersions;
    private String readOnlyStorageDir;
    private String readOnlySearchStrategy;
    private int readOnlyDeleteBackupTimeMs;
    private long readOnlyFetcherMaxBytesPerSecond;
    private long readOnlyFetcherReportingIntervalBytes;
    private int readOnlyFetcherThrottlerInterval;
    private int readOnlyFetchRetryCount;
    private long readOnlyFetchRetryDelayMs;
    private int fetcherBufferSize;
    private int fetcherSocketTimeout;
    private String readOnlyKeytabPath;
    private String readOnlyKerberosUser;
    public static final String HADOOP_CONFIG_PATH = "readonly.hadoop.config.path";
    private String hadoopConfigPath;
    private String readOnlyKerberosKdc;
    private String readOnlykerberosRealm;
    private String fileFetcherClass;
    private String readOnlyCompressionCodec;
    private boolean readOnlyStatsFileEnabled;
    private int readOnlyMaxVersionsStatsFile;
    private int readOnlyMaxValueBufferAllocationSize;
    private long readOnlyLoginIntervalMs;
    private long defaultStorageSpaceQuotaInKB;
    private String modifiedProtocol;
    private int modifiedPort;

    public static final String PUSH_HA_ENABLED = "push.ha.enabled";
    private boolean highAvailabilityPushEnabled;
    public static final String PUSH_HA_CLUSTER_ID = "push.ha.cluster.id";
    private String highAvailabilityPushClusterId;
    public static final String PUSH_HA_LOCK_PATH = "push.ha.lock.path";
    private String highAvailabilityPushLockPath;
    public static final String PUSH_HA_LOCK_IMPLEMENTATION = "push.ha.lock.implementation";
    private String highAvailabilityPushLockImplementation;
    public static final String PUSH_HA_MAX_NODE_FAILURES = "push.ha.max.node.failure";
    private int highAvailabilityPushMaxNodeFailures;

    private OpTimeMap testingSlowQueueingDelays;
    private OpTimeMap testingSlowConcurrentDelays;

    private int coreThreads;
    private int maxThreads;
    private int socketTimeoutMs;
    private int socketBufferSize;
    private boolean socketKeepAlive;

    private boolean useNioConnector;
    private boolean nioConnectorKeepAlive;
    private int nioConnectorSelectors;
    private int nioAdminConnectorSelectors;
    private boolean nioAdminConnectorKeepAlive;
    private int nioAcceptorBacklog;
    private long nioSelectorMaxHeartBeatTimeMs;

    private int clientSelectors;
    private TimeoutConfig clientTimeoutConfig;
    private int clientMaxConnectionsPerNode;
    private int clientConnectionTimeoutMs;
    private int clientRoutingTimeoutMs;
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
    private boolean enablePruneJob;
    private boolean enableSlopPurgeJob;
    private boolean enableHttpServer;
    private boolean enableSocketServer;
    private boolean enableAdminServer;
    private boolean enableJmx;
    private boolean enableVerboseLogging;
    private boolean enableStatTracking;
    private boolean enableServerRouting;
    private boolean enableMetadataChecking;
    private boolean enableNetworkClassLoader;
    private boolean enableGossip;
    private boolean enableRebalanceService;
    private boolean enableJmxClusterName;
    private boolean enableQuotaLimiting;

    private List<String> storageConfigurations;

    private Props allProps;
    private String slopStoreType;
    private String pusherType;
    private long slopFrequencyMs;
    private long slopMaxWriteBytesPerSec;
    private long slopMaxReadBytesPerSec;
    private int slopBatchSize;
    private int slopZonesDownToTerminate;
    private boolean autoPurgeDeadSlops;

    private int adminCoreThreads;
    private int adminMaxThreads;
    private int adminStreamBufferSize;
    private int adminSocketTimeout;
    private int adminConnectionTimeout;

    private long streamMaxReadBytesPerSec;
    private long streamMaxWriteBytesPerSec;
    private boolean multiVersionStreamingPutsEnabled;
    private int gossipIntervalMs;

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
    private long rebalancingTimeoutSec;
    private int maxParallelStoresRebalancing;
    private boolean usePartitionScanForRebalance;
    private int maxProxyPutThreads;
    @Deprecated
    // Should be removed once the proxy put implementation is stable.
    private boolean proxyPutsDuringRebalance;

    private boolean enableRestService;
    private int numRestServiceNettyServerBacklog;
    private int numRestServiceNettyBossThreads;
    private int numRestServiceNettyWorkerThreads;
    private int numRestServiceStorageThreads;
    private int restServiceStorageThreadPoolQueueSize;
    private int maxHttpAggregatedContentLength;

    private int repairJobMaxKeysScannedPerSec;
    private int pruneJobMaxKeysScannedPerSec;
    private int slopPurgeJobMaxKeysScannedPerSec;

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
        this.bdbMaxDelta = props.getInt("bdb.max.delta", 100);
        this.bdbBinDelta = props.getInt("bdb.bin.delta", 75);
        this.bdbCheckpointBytes = props.getLong("bdb.checkpoint.interval.bytes", 200 * 1024 * 1024);
        this.bdbCheckpointMs = props.getLong("bdb.checkpoint.interval.ms", 30 * Time.MS_PER_SECOND);
        this.bdbOneEnvPerStore = props.getBoolean("bdb.one.env.per.store", false);
        this.bdbCleanerMinFileUtilization = props.getInt("bdb.cleaner.min.file.utilization", 0);
        this.bdbCleanerMinUtilization = props.getInt("bdb.cleaner.minUtilization", 50);
        this.bdbCleanerThreads = props.getInt("bdb.cleaner.threads", 1);
        // by default, wake up the cleaner everytime we write log file size *
        // utilization% bytes. So, by default 30MB
        this.bdbCleanerBytesInterval = props.getLong("bdb.cleaner.interval.bytes", 30 * 1024 * 1024);
        this.bdbCleanerLookAheadCacheSize = props.getInt("bdb.cleaner.lookahead.cache.size", 8192);
        this.bdbLockTimeoutMs = props.getLong("bdb.lock.timeout.ms", 500);
        this.bdbLockNLockTables = props.getInt("bdb.lock.nLockTables", 7);
        this.bdbLogFaultReadSize = props.getInt("bdb.log.fault.read.size", 2048);
        this.bdbLogIteratorReadSize = props.getInt("bdb.log.iterator.read.size", 8192);
        this.bdbFairLatches = props.getBoolean("bdb.fair.latches", false);
        this.bdbCheckpointerHighPriority = props.getBoolean("bdb.checkpointer.high.priority", false);
        this.bdbCleanerMaxBatchFiles = props.getInt("bdb.cleaner.max.batch.files", 0);
        this.bdbReadUncommitted = props.getBoolean("bdb.lock.read_uncommitted", true);
        this.bdbStatsCacheTtlMs = props.getLong("bdb.stats.cache.ttl.ms", 5 * Time.MS_PER_SECOND);
        this.bdbExposeSpaceUtilization = props.getBoolean("bdb.expose.space.utilization", true);
        this.bdbMinimumSharedCache = props.getLong("bdb.minimum.shared.cache", 0);
        this.bdbCleanerLazyMigration = props.getBoolean("bdb.cleaner.lazy.migration", false);
        this.bdbCacheModeEvictLN = props.getBoolean("bdb.cache.evictln", true);
        this.bdbMinimizeScanImpact = props.getBoolean("bdb.minimize.scan.impact", true);
        this.bdbPrefixKeysWithPartitionId = props.getBoolean("bdb.prefix.keys.with.partitionid",
                                                             true);
        this.bdbLevelBasedEviction = props.getBoolean("bdb.evict.by.level", false);
        this.bdbCheckpointerOffForBatchWrites = props.getBoolean("bdb.checkpointer.off.batch.writes",
                                                                 false);
        this.bdbCleanerFetchObsoleteSize = props.getBoolean("bdb.cleaner.fetch.obsolete.size", true);
        this.bdbCleanerAdjustUtilization = props.getBoolean("bdb.cleaner.adjust.utilization", false);
        this.bdbRecoveryForceCheckpoint = props.getBoolean("bdb.recovery.force.checkpoint", false);
        this.bdbRawPropertyString = props.getString("bdb.raw.property.string", null);

        this.numReadOnlyVersions = props.getInt("readonly.backups", 1);
        this.readOnlySearchStrategy = props.getString("readonly.search.strategy",
                BinarySearchStrategy.class.getName());
        this.readOnlyStorageDir = props.getString("readonly.data.directory", this.dataDirectory
                                                                             + File.separator
                                                                             + "read-only");
        this.readOnlyDeleteBackupTimeMs = props.getInt("readonly.delete.backup.ms", 0);
        this.readOnlyFetcherMaxBytesPerSecond = props.getBytes("fetcher.max.bytes.per.sec", 0);
        this.readOnlyFetcherReportingIntervalBytes = props.getBytes("fetcher.reporting.interval.bytes",
                                                                    REPORTING_INTERVAL_BYTES);
        this.readOnlyFetcherThrottlerInterval = props.getInt("fetcher.throttler.interval",
                                                             DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS);
        this.readOnlyFetchRetryCount = props.getInt("fetcher.retry.count", 5);
        this.readOnlyFetchRetryDelayMs = props.getLong("fetcher.retry.delay.ms", 5000);
        this.readOnlyLoginIntervalMs = props.getLong("fetcher.login.interval.ms", -1);
        this.defaultStorageSpaceQuotaInKB = props.getLong("default.storage.space.quota.in.kb",
                                                          DEFAULT_STORAGE_SPACE_QUOTA_IN_KB);
        this.fetcherBufferSize = (int) props.getBytes("hdfs.fetcher.buffer.size",
                                                      DEFAULT_FETCHER_BUFFER_SIZE);
        this.fetcherSocketTimeout = props.getInt("hdfs.fetcher.socket.timeout",
                                                 DEFAULT_FETCHER_SOCKET_TIMEOUT);
        this.readOnlyKeytabPath = props.getString("readonly.keytab.path",
                                                  this.metadataDirectory
                                                          + VoldemortConfig.DEFAULT_KEYTAB_PATH);
        this.readOnlyKerberosUser = props.getString("readonly.kerberos.user",
                                                    VoldemortConfig.DEFAULT_KERBEROS_PRINCIPAL);
        this.hadoopConfigPath = (props.getString(VoldemortConfig.HADOOP_CONFIG_PATH,
                                                 this.metadataDirectory + "/hadoop-conf"));
        this.readOnlyKerberosKdc = props.getString("readonly.kerberos.kdc",
                                                   VoldemortConfig.DEFAULT_KERBEROS_KDC);
        this.readOnlykerberosRealm = props.getString("readonly.kerberos.realm",
                                                     VoldemortConfig.DEFAULT_KERBEROS_REALM);
        this.fileFetcherClass = props.getString("file.fetcher.class",
                                                VoldemortConfig.DEFAULT_FILE_FETCHER_CLASS);
        this.readOnlyStatsFileEnabled = props.getBoolean("readonly.stats.file.enabled", true);
        this.readOnlyMaxVersionsStatsFile = props.getInt("readonly.stats.file.max.versions", 1000);
        this.readOnlyMaxValueBufferAllocationSize = props.getInt("readonly.max.value.buffer.allocation.size",
                                                                 VoldemortConfig.DEFAULT_RO_MAX_VALUE_BUFFER_ALLOCATION_SIZE);

        // To set the Voldemort RO server compression codec to GZIP, explicitly
        // set this
        // property "readonly.compression.codec" to "GZIP"
        this.readOnlyCompressionCodec = props.getString("readonly.compression.codec",
                                                        VoldemortConfig.DEFAULT_RO_COMPRESSION_CODEC);

        this.modifiedProtocol = props.getString("readonly.modify.protocol", null);
        this.modifiedPort = props.getInt("readonly.modify.port", -1);

        this.highAvailabilityPushClusterId = props.getString(PUSH_HA_CLUSTER_ID, null);
        this.highAvailabilityPushLockPath = props.getString(PUSH_HA_LOCK_PATH, null);
        this.highAvailabilityPushLockImplementation = props.getString(PUSH_HA_LOCK_IMPLEMENTATION, null);
        this.highAvailabilityPushMaxNodeFailures = props.getInt(PUSH_HA_MAX_NODE_FAILURES, 0);
        this.highAvailabilityPushEnabled = props.getBoolean(PUSH_HA_ENABLED, false);

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
        this.multiVersionStreamingPutsEnabled = props.getBoolean("use.multi.version.streaming.puts",
                                                                 true);

        this.socketTimeoutMs = props.getInt("socket.timeout.ms", 5000);
        this.socketBufferSize = (int) props.getBytes("socket.buffer.size", 64 * 1024);
        this.socketKeepAlive = props.getBoolean("socket.keepalive", false);

        this.useNioConnector = props.getBoolean("enable.nio.connector", true);
        this.nioConnectorKeepAlive = props.getBoolean("nio.connector.keepalive", false);
        this.nioConnectorSelectors = props.getInt("nio.connector.selectors",
                                                  Math.max(8, Runtime.getRuntime()
                                                                     .availableProcessors()));
        this.nioAdminConnectorSelectors = props.getInt("nio.admin.connector.selectors",
                                                       Math.max(8, Runtime.getRuntime()
                                                                          .availableProcessors()));
        this.nioAdminConnectorKeepAlive = props.getBoolean("nio.admin.connector.keepalive", false);
        // a value <= 0 forces the default to be used
        this.nioAcceptorBacklog = props.getInt("nio.acceptor.backlog", 256);
        this.nioSelectorMaxHeartBeatTimeMs = props.getLong("nio.selector.max.heart.beat.time.ms", 
                                                           TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));

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

        this.enableHttpServer = props.getBoolean("http.enable", false);
        this.enableSocketServer = props.getBoolean("socket.enable", true);
        this.enableAdminServer = props.getBoolean("admin.enable", true);
        this.enableJmx = props.getBoolean("jmx.enable", true);
        this.enableSlop = props.getBoolean("slop.enable", true);
        this.enableSlopPusherJob = props.getBoolean("slop.pusher.enable", true);
        this.slopMaxWriteBytesPerSec = props.getBytes("slop.write.byte.per.sec", 10 * 1000 * 1000);
        this.enableVerboseLogging = props.getBoolean("enable.verbose.logging", true);
        this.enableStatTracking = props.getBoolean("enable.stat.tracking", true);
        this.enableServerRouting = props.getBoolean("enable.server.routing", false);
        this.enableMetadataChecking = props.getBoolean("enable.metadata.checking", true);
        this.enableGossip = props.getBoolean("enable.gossip", false);
        this.enableRebalanceService = props.getBoolean("enable.rebalancing", true);
        this.enableRepair = props.getBoolean("enable.repair", true);
        this.enablePruneJob = props.getBoolean("enable.prunejob", true);
        this.enableSlopPurgeJob = props.getBoolean("enable.slop.purge.job", true);
        this.enableJmxClusterName = props.getBoolean("enable.jmx.clustername", false);
        this.enableQuotaLimiting = props.getBoolean("enable.quota.limiting", true);

        this.gossipIntervalMs = props.getInt("gossip.interval.ms", 30 * 1000);

        this.slopMaxWriteBytesPerSec = props.getBytes("slop.write.byte.per.sec", 10 * 1000 * 1000);
        this.slopMaxReadBytesPerSec = props.getBytes("slop.read.byte.per.sec", 10 * 1000 * 1000);
        this.slopStoreType = props.getString("slop.store.engine", BdbStorageConfiguration.TYPE_NAME);
        this.slopFrequencyMs = props.getLong("slop.frequency.ms", 5 * 60 * 1000);
        this.slopBatchSize = props.getInt("slop.batch.size", 100);
        this.pusherType = props.getString("pusher.type", StreamingSlopPusherJob.TYPE_NAME);
        this.slopZonesDownToTerminate = props.getInt("slop.zones.terminate", 0);
        this.autoPurgeDeadSlops = props.getBoolean("auto.purge.dead.slops", true);

        this.schedulerThreads = props.getInt("scheduler.threads", 6);
        this.mayInterruptService = props.getBoolean("service.interruptible", true);

        this.numScanPermits = props.getInt("num.scan.permits", 1);

        this.storageConfigurations = props.getList("storage.configs",
                                                   ImmutableList.of(BdbStorageConfiguration.class.getName(),
                                                                    MysqlStorageConfiguration.class.getName(),
                                                                    InMemoryStorageConfiguration.class.getName(),
                                                                    CacheStorageConfiguration.class.getName(),
                                                                    ReadOnlyStorageConfiguration.class.getName(),
                                                                    RocksDbStorageConfiguration.class.getName()));

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
        this.rebalancingTimeoutSec = props.getLong("rebalancing.timeout.seconds", 10 * 24 * 60 * 60);
        this.maxParallelStoresRebalancing = props.getInt("max.parallel.stores.rebalancing", 3);
        this.usePartitionScanForRebalance = props.getBoolean("use.partition.scan.for.rebalance",
                                                             true);
        this.maxProxyPutThreads = props.getInt("max.proxy.put.threads",
                                               Math.max(8, Runtime.getRuntime()
                                                                  .availableProcessors()));
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

        // TODO: REST-Server decide on the numbers
        this.enableRestService = props.getBoolean("rest.enable", false);
        this.numRestServiceNettyServerBacklog = props.getInt("num.rest.service.netty.server.backlog",
                                                             1000);
        this.numRestServiceNettyBossThreads = props.getInt("num.rest.service.netty.boss.threads", 1);
        this.numRestServiceNettyWorkerThreads = props.getInt("num.rest.service.netty.worker.threads",
                                                             20);
        this.numRestServiceStorageThreads = props.getInt("num.rest.service.storage.threads", 50);
        this.restServiceStorageThreadPoolQueueSize = props.getInt("rest.service.storage.thread.pool.queue.size",
                                                                  numRestServiceStorageThreads);
        this.maxHttpAggregatedContentLength = props.getInt("max.http.aggregated.content.length",
                                                           1048576);

        this.repairJobMaxKeysScannedPerSec = props.getInt("repairjob.max.keys.scanned.per.sec",
                                                          Integer.MAX_VALUE);
        this.pruneJobMaxKeysScannedPerSec = props.getInt("prunejob.max.keys.scanned.per.sec",
                                                         Integer.MAX_VALUE);
        this.slopPurgeJobMaxKeysScannedPerSec = props.getInt("slop.purgejob.max.keys.scanned.per.sec",
                                                             10000);

        // RocksDB config
        this.rocksdbDataDirectory = props.getString("rocksdb.data.dir", this.dataDirectory
                                                                    + File.separator + "rocksdb");
        this.rocksdbPrefixKeysWithPartitionId = props.getBoolean("rocksdb.prefix.keys.with.partitionid",
                                                                 true);
        this.rocksdbEnableReadLocks = props.getBoolean("rocksdb.enable.read.locks", false);

        validateParams();
    }

    public VoldemortConfig(int nodeId, String voldemortHome) {
        this(new Props().with("node.id", nodeId).with("voldemort.home", voldemortHome));
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
        if(numRestServiceNettyBossThreads < 1)
            throw new ConfigurationException("num.rest.service.netty.boss.threads cannot be less than 1");
        if(numRestServiceNettyWorkerThreads < 1)
            throw new ConfigurationException("num.rest.service.netty.worker.threads cannot be less than 1");
        if(numRestServiceStorageThreads < 1)
            throw new ConfigurationException("num.rest.service.storage.threads cannot be less than 1");
        if(numRestServiceNettyServerBacklog < 0)
            throw new ConfigurationException("num.rest.service.netty.server.backlog cannot be negative");
        if(restServiceStorageThreadPoolQueueSize < 0)
            throw new ConfigurationException("rest.service.storage.thread.pool.queue.size cannot be negative.");
        if(maxHttpAggregatedContentLength <= 0)
            throw new ConfigurationException("max.http.aggregated.content.length must be positive");
        if (this.highAvailabilityPushEnabled) {
            if (this.highAvailabilityPushClusterId == null)
                throw new ConfigurationException(PUSH_HA_CLUSTER_ID + " must be set if " + PUSH_HA_ENABLED + "=true");
            if (this.highAvailabilityPushLockPath == null)
                throw new ConfigurationException(PUSH_HA_LOCK_PATH + " must be set if " + PUSH_HA_ENABLED + "=true");
            if (this.highAvailabilityPushLockImplementation == null)
                throw new ConfigurationException(PUSH_HA_LOCK_IMPLEMENTATION + " must be set if " + PUSH_HA_ENABLED + "=true");
            if (this.highAvailabilityPushMaxNodeFailures < 1)
                throw new ConfigurationException(PUSH_HA_MAX_NODE_FAILURES + " must be 1 or more if " + PUSH_HA_ENABLED + "=true");
        }
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

    public int getNodeId() {
        return nodeId;
    }

    /**
     * Id of the server within the cluster. The server matches up this id with
     * the information in cluster.xml to determine what partitions belong to it
     * 
     * <ul>
     * <li>Property : "node.id"</li>
     * <li>Default : VOLDEMORT_NODE_ID env variable</li>
     * </ul>
     */
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getVoldemortHome() {
        return voldemortHome;
    }

    /**
     * <ul>
     * <li>Property : "voldemort.home"</li>
     * <li>Default : VOLDEMORT_HOME environment variable</li>
     * </ul>
     */
    public void setVoldemortHome(String voldemortHome) {
        this.voldemortHome = voldemortHome;
    }

    public String getDataDirectory() {
        return dataDirectory;
    }

    /**
     * The directory name given by "data.directory" default: voldemort.home/data
     * 
     * <ul>
     * <li>Property : "data.directory"</li>
     * <li>Default : VOLDEMORT_HOME/data</li>
     * </ul>
     */
    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public String getMetadataDirectory() {
        return metadataDirectory;
    }

    /**
     * The directory name given by "metadata.directory" default:
     * voldemort.home/config
     * 
     * <ul>
     * <li>Property : "metadata.directory"</li>
     * <li>Default : VOLDEMORT_HOME/config</li>
     * </ul>
     */
    public void setMetadataDirectory(String metadataDirectory) {
        this.metadataDirectory = metadataDirectory;
    }

    public long getBdbCacheSize() {
        return bdbCacheSize;
    }

    /**
     * The size of BDB Cache to hold portions of the BTree.
     * 
     * <ul>
     * <li>Property : "bdb.cache.size"</li>
     * <li>Default : 200MB</li>
     * </ul>
     */
    public void setBdbCacheSize(int bdbCacheSize) {
        this.bdbCacheSize = bdbCacheSize;
    }

    public boolean getBdbExposeSpaceUtilization() {
        return bdbExposeSpaceUtilization;
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
    public void setBdbExposeSpaceUtilization(boolean bdbExposeSpaceUtilization) {
        this.bdbExposeSpaceUtilization = bdbExposeSpaceUtilization;
    }

    public boolean isBdbFlushTransactionsEnabled() {
        return bdbFlushTransactions;
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
    public void setBdbFlushTransactions(boolean bdbSyncTransactions) {
        this.bdbFlushTransactions = bdbSyncTransactions;
    }

    public String getBdbDataDirectory() {
        return bdbDataDirectory;
    }

    /**
     * The directory in which bdb data is stored.
     * 
     * <ul>
     * <li>Property : "bdb.data.directory"</li>
     * <li>Default : data.directory/bdb</li>
     * </ul>
     */
    public void setBdbDataDirectory(String bdbDataDirectory) {
        this.bdbDataDirectory = bdbDataDirectory;
    }

    public String getBdbRawPropertyString() {
        return bdbRawPropertyString;
    }

    /**
     * When supplied with comma separated propkey=propvalue strings, enables
     * admin to arbitrarily set any BDB JE environment property
     * 
     * eg:
     * bdb.raw.property.string=je.cleaner.threads=1,je.cleaner.lazyMigration=
     * true
     * 
     * Since this is applied after the regular BDB parameter in this class, this
     * has the effect of overriding previous configs if they are specified here
     * again.
     * 
     * <ul>
     * <li>Property : "bdb.raw.property.string"</li>
     * <li>Default : null</li>
     * </ul>
     */
    public void setBdbRawPropertyString(String bdbRawPropString) {
        this.bdbRawPropertyString = bdbRawPropString;
    }

    public long getBdbMaxLogFileSize() {
        return this.bdbMaxLogFileSize;
    }

    /**
     * The maximum size of a single .jdb log file in bytes.
     * 
     * <ul>
     * <li>Property : "bdb.max.logfile.size"</li>
     * <li>Default : 60MB</li>
     * </ul>
     */
    public void setBdbMaxLogFileSize(long bdbMaxLogFileSize) {
        this.bdbMaxLogFileSize = bdbMaxLogFileSize;
    }

    public int getBdbCleanerMinFileUtilization() {
        return bdbCleanerMinFileUtilization;
    }

    /**
     * A log file will be cleaned if its utilization percentage is below this
     * value, irrespective of total utilization. In practice, setting this to a
     * value greater than 0, might potentially hurt if the workload generates a
     * cleaning pattern with a heavy skew of utilization distribution amongs the
     * jdb files
     * 
     * <ul>
     * <li>property: "bdb.cleaner.minFileUtilization"</li>
     * <li>default: 0</li>
     * <li>minimum: 0</li>
     * <li>maximum: 50</li>
     * </ul>
     */
    public final void setBdbCleanerMinFileUtilization(int minFileUtilization) {
        if(minFileUtilization < 0 || minFileUtilization > 50)
            throw new IllegalArgumentException("minFileUtilization should be between 0 and 50 (both inclusive)");
        this.bdbCleanerMinFileUtilization = minFileUtilization;
    }

    public boolean getBdbCheckpointerHighPriority() {
        return bdbCheckpointerHighPriority;
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
    public final void setBdbCheckpointerHighPriority(boolean bdbCheckpointerHighPriority) {
        this.bdbCheckpointerHighPriority = bdbCheckpointerHighPriority;
    }

    public int getBdbCleanerMaxBatchFiles() {
        return bdbCleanerMaxBatchFiles;
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
    public final void setBdbCleanerMaxBatchFiles(int bdbCleanerMaxBatchFiles) {
        if(bdbCleanerMaxBatchFiles < 0 || bdbCleanerMaxBatchFiles > 100000)
            throw new IllegalArgumentException("bdbCleanerMaxBatchFiles should be between 0 and 100000 (both inclusive)");
        this.bdbCleanerMaxBatchFiles = bdbCleanerMaxBatchFiles;
    }

    public int getBdbCleanerThreads() {
        return bdbCleanerThreads;
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
    public final void setBdbCleanerThreads(int bdbCleanerThreads) {
        if(bdbCleanerThreads <= 0)
            throw new IllegalArgumentException("bdbCleanerThreads should be greater than 0");
        this.bdbCleanerThreads = bdbCleanerThreads;
    }

    public long getBdbCleanerBytesInterval() {
        return bdbCleanerBytesInterval;
    }

    /**
     * 
     * Amount of bytes written before the Cleaner wakes up to check for
     * utilization
     * 
     * <ul>
     * <li>property: "bdb.cleaner.interval.bytes"</li>
     * <li>default: 30MB</li>
     * </ul>
     */
    public final void setCleanerBytesInterval(long bdbCleanerBytesInterval) {
        this.bdbCleanerBytesInterval = bdbCleanerBytesInterval;
    }

    public int getBdbCleanerLookAheadCacheSize() {
        return bdbCleanerLookAheadCacheSize;
    }

    /**
     * Buffer size used by cleaner to fetch BTree nodes during cleaning.
     * 
     * <ul>
     * <li>property: "bdb.cleaner.lookahead.cache.size"</li>
     * <li>default: 8192</li>
     * </ul>
     * 
     */
    public final void setBdbCleanerLookAheadCacheSize(int bdbCleanerLookAheadCacheSize) {
        if(bdbCleanerLookAheadCacheSize < 0)
            throw new IllegalArgumentException("bdbCleanerLookAheadCacheSize should be at least 0");
        this.bdbCleanerLookAheadCacheSize = bdbCleanerLookAheadCacheSize;
    }

    public long getBdbLockTimeoutMs() {
        return bdbLockTimeoutMs;
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
    public final void setBdbLockTimeoutMs(long bdbLockTimeoutMs) {
        if(bdbLockTimeoutMs < 0)
            throw new IllegalArgumentException("bdbLockTimeoutMs should be greater than 0");
        this.bdbLockTimeoutMs = bdbLockTimeoutMs;
    }

    public int getBdbLockNLockTables() {
        return bdbLockNLockTables;
    }

    /**
     * The size of the lock table used by BDB JE
     * 
     * <ul>
     * <li>Property : bdb.lock.nLockTables"</li>
     * <li>Default : 7</li>
     * </ul>
     * 
     */
    public void setBdbLockNLockTables(int bdbLockNLockTables) {
        if(bdbLockNLockTables < 1 || bdbLockNLockTables > 32767)
            throw new IllegalArgumentException("bdbLockNLockTables should be greater than 0 and "
                                               + "less than 32767");
        this.bdbLockNLockTables = bdbLockNLockTables;
    }

    public int getBdbLogFaultReadSize() {
        return bdbLogFaultReadSize;
    }

    /**
     * Buffer for faulting in objects from disk
     * 
     * <ul>
     * <li>Property : "bdb.log.fault.read.size"</li>
     * <li>Default : 2048</li>
     * </ul>
     */
    public void setBdbLogFaultReadSize(int bdbLogFaultReadSize) {
        this.bdbLogFaultReadSize = bdbLogFaultReadSize;
    }

    public int getBdbLogIteratorReadSize() {
        return bdbLogIteratorReadSize;
    }

    /**
     * Buffer size used by BDB JE for reading the log eg: Cleaning.
     * 
     * <ul>
     * <li>Property : "bdb.log.iterator.read.size"</li>
     * <li>Default : 8192</li>
     * </ul>
     * 
     */
    public void setBdbLogIteratorReadSize(int bdbLogIteratorReadSize) {
        this.bdbLogIteratorReadSize = bdbLogIteratorReadSize;
    }

    public boolean getBdbFairLatches() {
        return bdbFairLatches;
    }

    /**
     * Controls whether BDB JE should use latches instead of synchronized blocks
     * 
     * <ul>
     * <li>Property : "bdb.fair.latches"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     */
    public void setBdbFairLatches(boolean bdbFairLatches) {
        this.bdbFairLatches = bdbFairLatches;
    }

    public boolean getBdbReadUncommitted() {
        return bdbReadUncommitted;
    }

    /**
     * If true, BDB JE get() will not be blocked by put()
     * 
     * <ul>
     * <li>Property : "bdb.lock.read_uncommitted"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     */
    public void setBdbReadUncommitted(boolean bdbReadUncommitted) {
        this.bdbReadUncommitted = bdbReadUncommitted;
    }

    public int getBdbCleanerMinUtilization() {
        return bdbCleanerMinUtilization;
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
    public final void setBdbCleanerMinUtilization(int minUtilization) {
        if(minUtilization < 0 || minUtilization > 90)
            throw new IllegalArgumentException("minUtilization should be between 0 and 90 (both inclusive)");
        this.bdbCleanerMinUtilization = minUtilization;
    }

    public int getBdbBtreeFanout() {
        return this.bdbBtreeFanout;
    }

    /**
     * The btree node fanout. Given by "". default: 512
     * 
     * <ul>
     * <li>property: "bdb.btree.fanout"</li>
     * <li>default: 512</li>
     * </ul>
     */
    public void setBdbBtreeFanout(int bdbBtreeFanout) {
        this.bdbBtreeFanout = bdbBtreeFanout;
    }

    /**
     * Exposes BDB JE EnvironmentConfig.TREE_MAX_DELTA.
     * 
     * <ul>
     * <li>Property : "bdb.max.delta"</li>
     * <li>Default : 100</li>
     * </ul>
     * 
     */
    public void setBdbMaxDelta(int maxDelta) {
        this.bdbMaxDelta = maxDelta;
    }

    public int getBdbMaxDelta() {
        return this.bdbMaxDelta;
    }

    /**
     * Exposes BDB JE EnvironmentConfig.TREE_BIN_DELTA.
     * 
     * <ul>
     * <li>Property : "bdb.bin.delta"</li>
     * <li>Default : 75</li>
     * </ul>
     * 
     */
    public void setBdbBinDelta(int binDelta) {
        this.bdbBinDelta = binDelta;
    }

    public int getBdbBinDelta() {
        return this.bdbBinDelta;
    }

    public boolean getBdbCleanerFetchObsoleteSize() {
        return bdbCleanerFetchObsoleteSize;
    }

    /**
     * If true, Cleaner also fetches the old value to determine the size during
     * an update/delete to compute file utilization. Without this, BDB will auto
     * compute utilization based on heuristics.. (which may or may not work,
     * depending on your use case)
     * 
     * <ul>
     * <li>property: "bdb.cleaner.fetch.obsolete.size"</li>
     * <li>default : true</li>
     * </ul>
     * 
     */
    public final void setBdbCleanerFetchObsoleteSize(boolean bdbCleanerFetchObsoleteSize) {
        this.bdbCleanerFetchObsoleteSize = bdbCleanerFetchObsoleteSize;
    }

    public boolean getBdbCleanerAdjustUtilization() {
        return bdbCleanerAdjustUtilization;
    }

    /**
     * If true, Cleaner does not perform any predictive adjustment of the
     * internally computed utilization values
     * 
     * <ul>
     * <li>property: "bdb.cleaner.adjust.utilization"</li>
     * <li>default : false</li>
     * </ul>
     * 
     */
    public final void setBdbCleanerAdjustUtilization(boolean bdbCleanerAdjustUtilization) {
        this.bdbCleanerAdjustUtilization = bdbCleanerAdjustUtilization;
    }

    public boolean getBdbRecoveryForceCheckpoint() {
        return bdbRecoveryForceCheckpoint;
    }

    /**
     * When this parameter is set to true, the last .jdb file restored from
     * snapshot will not be modified when opening the Environment, and a new
     * .jdb file will be created and become the end-of-log file. If using
     * incremental backup, this parameter must be true.
     * 
     * <ul>
     * <li>property: "bdb.recovery.force.checkpoint"</li>
     * <li>default : false</li>
     * </ul>
     * 
     */
    public final void setBdbRecoveryForceCheckpoint(boolean bdbRecoveryForceCheckpoint) {
        this.bdbRecoveryForceCheckpoint = bdbRecoveryForceCheckpoint;
    }

    public boolean getBdbCleanerLazyMigration() {
        return bdbCleanerLazyMigration;
    }

    /**
     * If true, Cleaner offloads some work to application threads, to keep up
     * with the write rate. Side effect is that data is staged on the JVM till
     * it is flushed down by Checkpointer, hence not GC friendly (Will cause
     * promotions). Use if you have lots of spare RAM but running low on
     * threads/IOPS
     * 
     * <ul>
     * <li>property: "bdb.cleaner.lazy.migration"</li>
     * <li>default : false</li>
     * </ul>
     * 
     */
    public final void setBdbCleanerLazyMigration(boolean bdbCleanerLazyMigration) {
        this.bdbCleanerLazyMigration = bdbCleanerLazyMigration;
    }

    public boolean getBdbCacheModeEvictLN() {
        return bdbCacheModeEvictLN;
    }

    /**
     * If true, BDB will not cache data in the JVM. This is very Java GC
     * friendly, and brings a lot of predictability in performance, by greatly
     * reducing constant CMS activity
     * 
     * <ul>
     * <li>Property : "bdb.cache.evictln"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     */
    public void setBdbCacheModeEvictLN(boolean bdbCacheModeEvictLN) {
        this.bdbCacheModeEvictLN = bdbCacheModeEvictLN;
    }

    public boolean getBdbMinimizeScanImpact() {
        return bdbMinimizeScanImpact;
    }

    /**
     * If true, attempts are made to minimize impact to BDB cache during scan
     * jobs
     * 
     * <ul>
     * <li>Property : "bdb.minimize.scan.impact"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     */
    public void setBdbMinimizeScanImpact(boolean bdbMinimizeScanImpact) {
        this.bdbMinimizeScanImpact = bdbMinimizeScanImpact;
    }

    public boolean isBdbWriteTransactionsEnabled() {
        return bdbWriteTransactions;
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
     */
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
     */
    public void setBdbOneEnvPerStore(boolean bdbOneEnvPerStore) {
        this.bdbOneEnvPerStore = bdbOneEnvPerStore;
    }

    public boolean isBdbOneEnvPerStore() {
        return bdbOneEnvPerStore;
    }

    public boolean getBdbPrefixKeysWithPartitionId() {
        return bdbPrefixKeysWithPartitionId;
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
     */
    public void setBdbPrefixKeysWithPartitionId(boolean bdbPrefixKeysWithPartitionId) {
        this.bdbPrefixKeysWithPartitionId = bdbPrefixKeysWithPartitionId;
    }

    public long getBdbCheckpointBytes() {
        return this.bdbCheckpointBytes;
    }

    /**
     * Checkpointer is woken up and a checkpoint is written once this many bytes
     * have been logged
     * 
     * <ul>
     * <li>Property : "bdb.checkpoint.interval.bytes"</li>
     * <li>Default : 200MB</li>
     * </ul>
     * 
     */
    public void setBdbCheckpointBytes(long bdbCheckpointBytes) {
        this.bdbCheckpointBytes = bdbCheckpointBytes;
    }

    public boolean getBdbCheckpointerOffForBatchWrites() {
        return this.bdbCheckpointerOffForBatchWrites;
    }

    /**
     * BDB JE Checkpointer will be turned off during batch writes. This helps
     * save redundant writing of index updates, as we do say large streaming
     * updates
     * 
     * <ul>
     * <li>Property : "bdb.checkpointer.off.batch.writes"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     */
    public void setBdbCheckpointerOffForBatchWrites(boolean bdbCheckpointerOffForBulkWrites) {
        this.bdbCheckpointerOffForBatchWrites = bdbCheckpointerOffForBulkWrites;
    }

    public long getBdbCheckpointMs() {
        return this.bdbCheckpointMs;
    }

    /**
     * BDB JE Checkpointer wakes up whenever this time period elapses
     * 
     * <ul>
     * <li>Property : "bdb.checkpoint.interval.ms"</li>
     * <li>Default : 30s or 30000 ms</li>
     * </ul>
     * 
     */
    public void setBdbCheckpointMs(long bdbCheckpointMs) {
        this.bdbCheckpointMs = bdbCheckpointMs;
    }

    public long getBdbStatsCacheTtlMs() {
        return this.bdbStatsCacheTtlMs;
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
     */
    public void setBdbStatsCacheTtlMs(long statsCacheTtlMs) {
        this.bdbStatsCacheTtlMs = statsCacheTtlMs;
    }

    public long getBdbMinimumSharedCache() {
        return this.bdbMinimumSharedCache;
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
     */
    public void setBdbMinimumSharedCache(long minimumSharedCache) {
        this.bdbMinimumSharedCache = minimumSharedCache;
    }

    public boolean isBdbLevelBasedEviction() {
        return bdbLevelBasedEviction;
    }

    /**
     * Controls if BDB JE cache eviction happens based on LRU or by BTree level.
     * 
     * <ul>
     * <li>Property : "bdb.evict.by.level"</li>
     * <li>Default : false</li>
     * </ul>
     * 
     */
    public void setBdbLevelBasedEviction(boolean bdbLevelBasedEviction) {
        this.bdbLevelBasedEviction = bdbLevelBasedEviction;
    }

    public int getCoreThreads() {
        return coreThreads;
    }

    /**
     * The comfortable number of threads the threadpool will attempt to
     * maintain. Not applicable with enable.nio=true and not officially
     * supported anymore
     * 
     * <ul>
     * <li>Property : "core.threads"</li>
     * <li>Default : max(1, floor(0.5 * max.threads)</li>
     * </ul>
     * 
     */
    @Deprecated
    public void setCoreThreads(int coreThreads) {
        this.coreThreads = coreThreads;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * The maximum number of threads in the server thread pool. Not applicable
     * with enable.nio.connector=true. Not officially supported anymore
     * 
     * <ul>
     * <li>Property : "max.threads"</li>
     * <li>Default : 100</li>
     * </ul>
     * 
     */
    @Deprecated
    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public int getAdminCoreThreads() {
        return adminCoreThreads;
    }

    /**
     * Number of threads that the admin service thread pool will attempt to keep
     * around. Not applicable with enable.nio.connector=true
     * 
     * <ul>
     * <li>Property : "admin.core.threads"</li>
     * <li>Default : max(1, adminMaxThreads/2)</li>
     * </ul>
     * 
     */
    public void setAdminCoreThreads(int coreThreads) {
        this.adminCoreThreads = coreThreads;
    }

    public int getAdminMaxThreads() {
        return adminMaxThreads;
    }

    /**
     * Maximum number of threads in the admin service thread pool. Not
     * applicable with enable.nio=true
     * 
     * <ul>
     * <li>Property : "admin.max.threads"</li>
     * <li>Default : 20</li>
     * </ul>
     * 
     */
    public void setAdminMaxThreads(int maxThreads) {
        this.adminMaxThreads = maxThreads;
    }

    public boolean getUseNioConnector() {
        return this.useNioConnector;
    }

    /**
     * Determines whether the server will use NIO style selectors while handling
     * requests. This is recommended over using old style BIO.
     * 
     * <ul>
     * <li>Property : "enable.nio.connector"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     */
    public void setUseNioConnector(boolean useNio) {
        this.useNioConnector = useNio;
    }

    public int getNioConnectorSelectors() {
        return nioConnectorSelectors;
    }

    /**
     * Number of NIO server threads to use to process client requests
     * 
     * <ul>
     * <li>Property : nio.connector.selectors</li>
     * <li>Default : max(8, number of available processors)</li>
     * </ul>
     * 
     * 
     */
    public void setNioConnectorSelectors(int nioConnectorSelectors) {
        this.nioConnectorSelectors = nioConnectorSelectors;
    }

    public int getNioAdminConnectorSelectors() {
        return nioAdminConnectorSelectors;
    }

    /**
     * Number of admin NIO server threads to spin up.
     * 
     * <ul>
     * <li>Property : nio.admin.connector.selectors</li>
     * <li>Default : max(8, number of available processors)</li>
     * </ul>
     * 
     * 
     */
    public void setNioAdminConnectorSelectors(int nioAdminConnectorSelectors) {
        this.nioAdminConnectorSelectors = nioAdminConnectorSelectors;
    }

    @Deprecated
    public boolean isHttpServerEnabled() {
        return enableHttpServer;
    }

    /**
     * Whether or not the {@link HttpService} is enabled
     * <ul>
     * <li>Property :"http.enable"</li>
     * <li>Default :false</li>
     * </ul>
     * 
     */
    @Deprecated
    public void setEnableHttpServer(boolean enableHttpServer) {
        this.enableHttpServer = enableHttpServer;
    }

    /**
     * Determines whether the socket server will be enabled for BIO/NIO request
     * handling
     * 
     * <ul>
     * <li>Property :"socket.enable"</li>
     * <li>Default :true</li>
     * </ul>
     * 
     */
    public boolean isSocketServerEnabled() {
        return enableSocketServer;
    }

    public boolean isAdminServerEnabled() {
        return enableAdminServer;
    }

    /**
     * Determine whether the admin service has been enabled to perform
     * maintenance operations on the server
     * 
     * <ul>
     * <li>Property : "admin.enable"</li>
     * <li>Default : true</li>
     * </ul>
     */
    public void setAdminServerEnabled(boolean enableAdminServer) {
        this.enableAdminServer = enableAdminServer;
    }

    public long getStreamMaxReadBytesPerSec() {
        return streamMaxReadBytesPerSec;
    }

    /**
     * Maximum amount of data read out of the server by streaming operations
     * 
     * <ul>
     * <li>Property : "stream.read.byte.per.sec"</li>
     * <li>Default : 10MB</li>
     * </ul>
     * 
     */
    public void setStreamMaxReadBytesPerSec(long streamMaxReadBytesPerSec) {
        this.streamMaxReadBytesPerSec = streamMaxReadBytesPerSec;
    }

    public long getStreamMaxWriteBytesPerSec() {
        return streamMaxWriteBytesPerSec;
    }

    /**
     * Maximum amount of data to be written into the server by streaming
     * operations
     * 
     * <ul>
     * <li>Property : "stream.write.byte.per.sec"</li>
     * <li>Default : 10MB</li>
     * </ul>
     * 
     */
    public void setStreamMaxWriteBytesPerSec(long streamMaxWriteBytesPerSec) {
        this.streamMaxWriteBytesPerSec = streamMaxWriteBytesPerSec;
    }

    public long getSlopMaxWriteBytesPerSec() {
        return slopMaxWriteBytesPerSec;
    }

    /**
     * If true, multiple successive versions of the same key, will be atomically
     * written to storage in a single operation. Currently not supported for
     * MySqlStorageEngine
     * 
     * <ul>
     * <li>Property : "use.multi.version.streaming.puts"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     */
    public void setMultiVersionStreamingPutsEnabled(boolean multiVersionStreamingPutsEnabled) {
        this.multiVersionStreamingPutsEnabled = multiVersionStreamingPutsEnabled;
    }

    public boolean getMultiVersionStreamingPutsEnabled() {
        return this.multiVersionStreamingPutsEnabled;
    }

    /**
     * Controls the rate at which the {@link StreamingSlopPusherJob} will send
     * slop writes over the wire
     * 
     * <ul>
     * <li>Property :"slop.write.byte.per.sec"</li>
     * <li>Default :10MB</li>
     * </ul>
     * 
     */
    public void setSlopMaxWriteBytesPerSec(long slopMaxWriteBytesPerSec) {
        this.slopMaxWriteBytesPerSec = slopMaxWriteBytesPerSec;
    }

    public long getSlopMaxReadBytesPerSec() {
        return slopMaxReadBytesPerSec;
    }

    /**
     * Controls the rate at which the {@link StreamingSlopPusherJob} reads the
     * 'slop' store and drains it off to another server
     * 
     * <ul>
     * <li>Property :"slop.read.byte.per.sec"</li>
     * <li>Default :10MB</li>
     * </ul>
     * 
     */
    public void setSlopMaxReadBytesPerSec(long slopMaxReadBytesPerSec) {
        this.slopMaxReadBytesPerSec = slopMaxReadBytesPerSec;
    }

    public boolean isJmxEnabled() {
        return enableJmx;
    }

    /**
     * Is JMX monitoring enabled on the server?
     * 
     * <ul>
     * <li>Property :"jmx.enable"</li>
     * <li>Default : true</li>
     * </ul>
     * 
     */
    public void setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    /**
     * user name to use with MySQL storage engine
     * 
     * <ul>
     * <li>Property : "mysql.user"</li>
     * <li>Default : "root"</li>
     * </ul>
     */
    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    /**
     * Password to use with MySQL storage engine
     * 
     * <ul>
     * <li>Property :"mysql.password"</li>
     * <li>Default :""</li>
     * </ul>
     */
    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public String getMysqlDatabaseName() {
        return mysqlDatabaseName;
    }

    /**
     * MySQL database name to use
     * 
     * <ul>
     * <li>Property :</li>
     * <li>Default :</li>
     * </ul>
     */
    public void setMysqlDatabaseName(String mysqlDatabaseName) {
        this.mysqlDatabaseName = mysqlDatabaseName;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    /**
     * Hostname of the database server for MySQL storage engine
     * 
     * <ul>
     * <li>Property :"mysql.host"</li>
     * <li>Default :"localhost"</li>
     * </ul>
     */
    public void setMysqlHost(String mysqlHost) {
        this.mysqlHost = mysqlHost;
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    /**
     * Port number for the MySQL database server
     * 
     * <ul>
     * <li>Property :"mysql.port"</li>
     * <li>Default :3306</li>
     * </ul>
     */
    public void setMysqlPort(int mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public String getSlopStoreType() {
        return slopStoreType;
    }

    /**
     * The underlying store type which will be used to store slops. Defaults to
     * Bdb torageConfiguration.class.getName())
     * 
     * <ul>
     * <li>Property :"slop.store.engine"</li>
     * <li>Default :BdbStorageConfiguration.TYPE_NAME</li>
     * </ul>
     */
    public void setSlopStoreType(String slopStoreType) {
        this.slopStoreType = slopStoreType;
    }

    public String getPusherType() {
        return this.pusherType;
    }

    /**
     * The type of streaming job we would want to use to send hints. Defaults to
     * 
     * <ul>
     * <li>Property :"pusher.type"</li>
     * <li>Default :StreamingSlopPusherJob.TYPE_NAME</li>
     * </ul>
     */
    public void setPusherType(String pusherType) {
        this.pusherType = pusherType;
    }

    public int getSlopZonesDownToTerminate() {
        return this.slopZonesDownToTerminate;
    }

    /**
     * Number of zones declared down before we terminate the pusher job
     * 
     * <ul>
     * <li>Property :"slop.zones.terminate"</li>
     * <li>Default :0</li>
     * </ul>
     */
    public void setSlopZonesDownToTerminate(int slopZonesDownToTerminate) {
        this.slopZonesDownToTerminate = slopZonesDownToTerminate;
    }

    public boolean getAutoPurgeDeadSlops() {
        return this.autoPurgeDeadSlops;
    }

    /**
     * if true, dead slops accumulated for nodes/stores that are no longer part
     * of the cluster, will be automatically deleted by the slop pusher, when it
     * runs. If false, they will be ignored.
     * 
     * <ul>
     * <li>Property :"auto.purge.dead.slops"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setAutoPurgeDeadSlops(boolean autoPurgeDeadSlops) {
        this.autoPurgeDeadSlops = autoPurgeDeadSlops;
    }

    public int getSlopBatchSize() {
        return this.slopBatchSize;
    }

    /**
     * Returns the size of the batch used while streaming slops
     * 
     * <ul>
     * <li>Property :"slop.batch.size"</li>
     * <li>Default :100</li>
     * </ul>
     */
    public void setSlopBatchSize(int slopBatchSize) {
        this.slopBatchSize = slopBatchSize;
    }

    public int getSocketTimeoutMs() {
        return this.socketTimeoutMs;
    }

    public long getSlopFrequencyMs() {
        return this.slopFrequencyMs;
    }

    /**
     * Frequency at which the slop pusher attempts to push slops
     * 
     * <ul>
     * <li>Property :"slop.frequency.ms"</li>
     * <li>Default :300 seconds</li>
     * </ul>
     */
    public void setSlopFrequencyMs(long slopFrequencyMs) {
        this.slopFrequencyMs = slopFrequencyMs;
    }

    /**
     * {@link ClientConfig#setSocketTimeout(int, java.util.concurrent.TimeUnit)}
     * 
     * <ul>
     * <li>Property :"socket.timeout.ms"</li>
     * <li>Default :5000</li>
     * </ul>
     */
    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getClientSelectors() {
        return clientSelectors;
    }

    /**
     * {@link ClientConfig#setSelectors(int)}
     * 
     * <ul>
     * <li>Property :"client.selectors"</li>
     * <li>Default :4</li>
     * </ul>
     */
    public void setClientSelectors(int clientSelectors) {
        this.clientSelectors = clientSelectors;
    }

    public int getClientRoutingTimeoutMs() {
        return this.clientRoutingTimeoutMs;
    }

    /**
     * {@link ClientConfig#setRoutingTimeout(int, java.util.concurrent.TimeUnit)}
     * 
     * <ul>
     * <li>Property :"client.routing.timeout.ms"</li>
     * <li>Default :15000</li>
     * </ul>
     */
    public void setClientRoutingTimeoutMs(int routingTimeoutMs) {
        this.clientRoutingTimeoutMs = routingTimeoutMs;
    }

    /**
     * {@link ClientConfig#setTimeoutConfig(TimeoutConfig)}
     * 
     */
    public TimeoutConfig getTimeoutConfig() {
        return this.clientTimeoutConfig;
    }

    public int getClientMaxConnectionsPerNode() {
        return clientMaxConnectionsPerNode;
    }

    /**
     * {@link ClientConfig#setMaxConnectionsPerNode(int)}
     * 
     * <ul>
     * <li>Property :"client.max.connections.per.node"</li>
     * <li>Default :50</li>
     * </ul>
     */
    public void setClientMaxConnectionsPerNode(int maxConnectionsPerNode) {
        this.clientMaxConnectionsPerNode = maxConnectionsPerNode;
    }

    public int getClientConnectionTimeoutMs() {
        return clientConnectionTimeoutMs;
    }

    /**
     * {@link ClientConfig#setConnectionTimeout(int, java.util.concurrent.TimeUnit)}
     * 
     * <ul>
     * <li>Property :"client.connection.timeout.ms"</li>
     * <li>Default :500</li>
     * </ul>
     */
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

    /**
     * {@link ClientConfig#setMaxThreads(int)}
     * 
     * <ul>
     * <li>Property :"client.max.threads"</li>
     * <li>Default :500</li>
     * </ul>
     */
    public void setClientMaxThreads(int clientMaxThreads) {
        this.clientMaxThreads = clientMaxThreads;
    }

    public int getClientThreadIdleMs() {
        return clientThreadIdleMs;
    }

    /**
     * {@link ClientConfig#setThreadIdleTime(long, java.util.concurrent.TimeUnit)}
     * 
     * <ul>
     * <li>Property :"client.thread.idle.ms"</li>
     * <li>Default :100000</li>
     * </ul>
     */
    public void setClientThreadIdleMs(int clientThreadIdleMs) {
        this.clientThreadIdleMs = clientThreadIdleMs;
    }

    public int getClientMaxQueuedRequests() {
        return clientMaxQueuedRequests;
    }

    /**
     * {@link ClientConfig#setMaxQueuedRequests(int)}
     * <ul>
     * <li>Property :</li>
     * <li>Default :</li>
     * </ul>
     */
    public void setClientMaxQueuedRequests(int clientMaxQueuedRequests) {
        this.clientMaxQueuedRequests = clientMaxQueuedRequests;
    }

    public boolean isSlopEnabled() {
        return this.enableSlop;
    }

    /**
     * Whether or not slop store should be created on the server.
     * 
     * <ul>
     * <li>Property :"slop.enable"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableSlop(boolean enableSlop) {
        this.enableSlop = enableSlop;
    }

    public boolean isSlopPusherJobEnabled() {
        return enableSlopPusherJob;
    }

    /**
     * Whether or not {@link StreamingSlopPusherJob} or
     * {@link BlockingSlopPusherJob} should be enabled to asynchronous push
     * slops to failed servers
     * 
     * <ul>
     * <li>Property :"slop.pusher.enable"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableSlopPusherJob(boolean enableSlopPusherJob) {
        this.enableSlopPusherJob = enableSlopPusherJob;
    }

    public boolean isRepairEnabled() {
        return this.enableRepair;
    }

    /**
     * Whether {@link RepairJob} will be enabled
     * 
     * <ul>
     * <li>Property :"enable.repair"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableRepair(boolean enableRepair) {
        this.enableRepair = enableRepair;
    }

    public boolean isPruneJobEnabled() {
        return this.enablePruneJob;
    }

    /**
     * Whether {@link VersionedPutPruneJob} will be enabled
     * 
     * <ul>
     * <li>Property :"enable.prunejob"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnablePruneJob(boolean enablePruneJob) {
        this.enablePruneJob = enablePruneJob;
    }

    public boolean isSlopPurgeJobEnabled() {
        return this.enableSlopPurgeJob;
    }

    /**
     * Whether will {@link SlopPurgeJob} be enabled
     * 
     * <ul>
     * <li>Property :"enable.slop.purge.job"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableSlopPurgeJob(boolean enableSlopPurgeJob) {
        this.enableSlopPurgeJob = enableSlopPurgeJob;
    }

    public boolean isVerboseLoggingEnabled() {
        return this.enableVerboseLogging;
    }

    /**
     * if enabled, {@link LoggingStore} will be enable to ouput more detailed
     * trace debugging if needed
     * 
     * <ul>
     * <li>Property :"enable.verbose.logging"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableVerboseLogging(boolean enableVerboseLogging) {
        this.enableVerboseLogging = enableVerboseLogging;
    }

    public boolean isStatTrackingEnabled() {
        return this.enableStatTracking;
    }

    /**
     * If enabled, {@link StatTrackingStore} will be enabled to account
     * performance statistics
     * 
     * <ul>
     * <li>Property :"enable.stat.tracking"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableStatTracking(boolean enableStatTracking) {
        this.enableStatTracking = enableStatTracking;
    }

    public boolean isMetadataCheckingEnabled() {
        return enableMetadataChecking;
    }

    /**
     * If enabled, {@link InvalidMetadataCheckingStore} will reject traffic that
     * does not belong to this server with a {@link InvalidMetadataException}
     * 
     * <ul>
     * <li>Property :"enable.metadata.checking"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableMetadataChecking(boolean enableMetadataChecking) {
        this.enableMetadataChecking = enableMetadataChecking;
    }

    public int getSchedulerThreads() {
        return schedulerThreads;
    }

    /**
     * Number of {@link SchedulerService} threads to create that run all the
     * background async jobs
     * 
     * <ul>
     * <li>Property :"client.max.queued.requests"</li>
     * <li>Default :1000</li>
     * </ul>
     */
    public void setSchedulerThreads(int schedulerThreads) {
        this.schedulerThreads = schedulerThreads;
    }

    public boolean canInterruptService() {
        return mayInterruptService;
    }

    /**
     * Determines whether the scheduler can be allowed to interrupt a
     * {@link AsyncOperation}, when terminating the job
     * 
     * <ul>
     * <li>Property :"service.interruptible"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setInterruptible(boolean canInterrupt) {
        this.mayInterruptService = canInterrupt;
    }

    public String getReadOnlyDataStorageDirectory() {
        return this.readOnlyStorageDir;
    }

    /**
     * Directory to store the read-only data and index files in
     * 
     * <ul>
     * <li>Property :"readonly.data.directory"</li>
     * <li>Default : DATA_DIR/read-only</li>
     * </ul>
     */
    public void setReadOnlyDataStorageDirectory(String readOnlyStorageDir) {
        this.readOnlyStorageDir = readOnlyStorageDir;
    }

    public int getNumReadOnlyVersions() {
        return numReadOnlyVersions;
    }

    /**
     * Number of previous versions to keep around for
     * {@link ReadOnlyStorageEngine}
     * 
     * <ul>
     * <li>Property :"readonly.backups"</li>
     * <li>Default :1</li>
     * </ul>
     */
    public void setNumReadOnlyVersions(int readOnlyBackups) {
        this.numReadOnlyVersions = readOnlyBackups;
    }

    public int getReadOnlyDeleteBackupMs() {
        return readOnlyDeleteBackupTimeMs;
    }

    /**
     * Amount of time we will wait before we start deleting the backup. This
     * happens during swaps when old backups need to be deleted. Some delay is
     * 
     * <ul>
     * <li>Property :"readonly.delete.backup.ms"</li>
     * <li>Default :0</li>
     * </ul>
     */
    public void setReadOnlyDeleteBackupMs(int readOnlyDeleteBackupTimeMs) {
        this.readOnlyDeleteBackupTimeMs = readOnlyDeleteBackupTimeMs;
    }

    public String getReadOnlyKeytabPath() {
        return readOnlyKeytabPath;
    }

    /**
     * Path to keytab for principal used for kerberized Hadoop grids
     * 
     * <ul>
     * <li>Property :"readonly.keytab.path"</li>
     * <li>Default :METADATA_DIR/voldemrt.headless.keytab</li>
     * </ul>
     */
    public void setReadOnlyKeytabPath(String readOnlyKeytabPath) {
        this.readOnlyKeytabPath = readOnlyKeytabPath;
    }

    public String getReadOnlyKerberosUser() {
        return readOnlyKerberosUser;
    }

    /**
     * Principal used in kerberized Hadoop grids
     * 
     * <ul>
     * <li>Property :"readonly.kerberos.user"</li>
     * <li>Default :"voldemrt"</li>
     * </ul>
     */
    public void setReadOnlyKerberosUser(String readOnlyKerberosUser) {
        this.readOnlyKerberosUser = readOnlyKerberosUser;
    }

    public String getHadoopConfigPath() {
        return hadoopConfigPath;
    }

    /**
     * Path to the hadoop config
     * 
     * <ul>
     * <li>Property :"readonly.hadoop.config.path"</li>
     * <li>Default : METADATA_DIR/hadoop-conf</li>
     * </ul>
     */
    public void setHadoopConfigPath(String hadoopConfigPath) {
        this.hadoopConfigPath = hadoopConfigPath;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /**
     * {@link ClientConfig#setSocketBufferSize(int)}
     * 
     * <ul>
     * <li>Property :"socket.buffer.size"</li>
     * <li>Default :64kb</li>
     * </ul>
     */
    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public boolean getSocketKeepAlive() {
        return this.socketKeepAlive;
    }

    /**
     * {@link ClientConfig#setSocketKeepAlive(boolean)}
     * 
     * <ul>
     * <li>Property :"socket.keepalive"</li>
     * <li>Default :false</li>
     * </ul>
     */
    public void setSocketKeepAlive(boolean on) {
        this.socketKeepAlive = on;
    }

    public int getNioAcceptorBacklog() {
        return nioAcceptorBacklog;
    }

    public long getNioSelectorMaxHeartBeatTimeMs() {
        return nioSelectorMaxHeartBeatTimeMs;
    }

    public void setNioSelectorMaxHeartBeatTimeMs(long nioSelectorMaxHeartBeatTimeMs) {
        this.nioSelectorMaxHeartBeatTimeMs = nioSelectorMaxHeartBeatTimeMs;
    }

    /**
     * Determines the size of the {@link NioSocketService}'s accept backlog
     * queue. A large enough backlog queue prevents connections from being
     * dropped during connection bursts
     * 
     * <ul>
     * <li>Property :"nio.acceptor.backlog"</li>
     * <li>Default : 256</li>
     * </ul>
     */
    public void setNioAcceptorBacklog(int nioAcceptorBacklog) {
        this.nioAcceptorBacklog = nioAcceptorBacklog;
    }

    public int getAdminSocketBufferSize() {
        return adminStreamBufferSize;
    }

    /**
     * {@link ClientConfig#setSocketBufferSize(int)} to use for network
     * operations during admin operations
     * <ul>
     * <li>Property :"admin.streams.buffer.size"</li>
     * <li>Default :10MB</li>
     * </ul>
     */
    public void setAdminSocketBufferSize(int socketBufferSize) {
        this.adminStreamBufferSize = socketBufferSize;
    }

    public List<String> getStorageConfigurations() {
        return storageConfigurations;
    }

    /**
     * List of fully qualified class names of {@link StorageEngine} types to
     * enable on the server
     * 
     * <ul>
     * <li>Property :"storage.configs"</li>
     * <li>Default : {@link BdbStorageConfiguration}
     * {@link MysqlStorageConfiguration} {@link InMemoryStorageConfiguration}
     * {@link CacheStorageConfiguration} {@link ReadOnlyStorageConfiguration}</li>
     * <ul>
     */
    public void setStorageConfigurations(List<String> storageConfigurations) {
        this.storageConfigurations = storageConfigurations;
    }

    public Props getAllProps() {
        return this.allProps;
    }

    /**
     * {@link ClientConfig#setRequestFormatType(RequestFormatType)}
     * 
     * <ul>
     * <li>Property :"request.format"</li>
     * <li>Default :"vp1"</li>
     * </ul>
     */
    public void setRequestFormatType(RequestFormatType type) {
        this.requestFormatType = type;
    }

    public RequestFormatType getRequestFormatType() {
        return this.requestFormatType;
    }

    public boolean isServerRoutingEnabled() {
        return this.enableServerRouting;
    }

    /**
     * If enabled, Routing may happen in the server,depending on store
     * definition. Note that the Java Client {@link DefaultStoreClient} does not
     * support this yet.
     * 
     * <ul>
     * <li>Property :"enable.server.routing"</li>
     * <li>Default : true</li>
     * </ul>
     */
    public void setEnableServerRouting(boolean enableServerRouting) {
        this.enableServerRouting = enableServerRouting;
    }

    public int getNumScanPermits() {
        return numScanPermits;
    }

    /**
     * Maximum number of background tasks to run parallely with the online
     * traffic. This trades off between time to finish background work and
     * impact on online performance eg: {@link DataCleanupJob} and
     * {@link StreamingSlopPusherJob}
     * 
     * <ul>
     * <li>Property :"num.scan.permits"</li>
     * <li>Default :1</li>
     * </ul>
     */
    public void setNumScanPermits(int numScanPermits) {
        this.numScanPermits = numScanPermits;
    }

    public String getFailureDetectorImplementation() {
        return failureDetectorImplementation;
    }

    /**
     * {@link ClientConfig#setFailureDetectorImplementation(String)}
     * 
     * <ul>
     * <li>Property :"failuredetector.implementation"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_IMPLEMENTATION_CLASS_NAME</li>
     * </ul>
     */
    public void setFailureDetectorImplementation(String failureDetectorImplementation) {
        this.failureDetectorImplementation = failureDetectorImplementation;
    }

    public long getFailureDetectorBannagePeriod() {
        return failureDetectorBannagePeriod;
    }

    /**
     * {@link ClientConfig#setFailureDetectorBannagePeriod(long)}
     * 
     * <ul>
     * <li>Property :"failuredetector.bannage.period"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_BANNAGE_PERIOD</li>
     * </ul>
     */
    public void setFailureDetectorBannagePeriod(long failureDetectorBannagePeriod) {
        this.failureDetectorBannagePeriod = failureDetectorBannagePeriod;
    }

    public int getFailureDetectorThreshold() {
        return failureDetectorThreshold;
    }

    /**
     * {@link ClientConfig#setFailureDetectorThreshold(int)}
     * 
     * <ul>
     * <li>Property :"failuredetector.threshold"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_THRESHOLD</li>
     * </ul>
     */
    public void setFailureDetectorThreshold(int failureDetectorThreshold) {
        this.failureDetectorThreshold = failureDetectorThreshold;
    }

    public int getFailureDetectorThresholdCountMinimum() {
        return failureDetectorThresholdCountMinimum;
    }

    /**
     * {@link ClientConfig#setFailureDetectorThresholdCountMinimum(int)}
     * 
     * <ul>
     * <li>Property :"failuredetector.threshold.countminimum"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_THRESHOLD_COUNT_MINIMUM</li>
     * </ul>
     */
    public void setFailureDetectorThresholdCountMinimum(int failureDetectorThresholdCountMinimum) {
        this.failureDetectorThresholdCountMinimum = failureDetectorThresholdCountMinimum;
    }

    public long getFailureDetectorThresholdInterval() {
        return failureDetectorThresholdInterval;
    }

    /**
     * {@link ClientConfig#setFailureDetectorThresholdInterval(long)}
     * 
     * <ul>
     * <li>Property :"failuredetector.threshold.interval"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_THRESHOLD_INTERVAL</li>
     * </ul>
     */
    public void setFailureDetectorThresholdInterval(long failureDetectorThresholdInterval) {
        this.failureDetectorThresholdInterval = failureDetectorThresholdInterval;
    }

    public long getFailureDetectorAsyncRecoveryInterval() {
        return failureDetectorAsyncRecoveryInterval;
    }

    /**
     * {@link ClientConfig#setFailureDetectorAsyncRecoveryInterval(long)}
     * 
     * <ul>
     * <li>Property :"failuredetector.asyncrecovery.interval"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_ASYNC_RECOVERY_INTERVAL</li>
     * </ul>
     */
    public void setFailureDetectorAsyncRecoveryInterval(long failureDetectorAsyncRecoveryInterval) {
        this.failureDetectorAsyncRecoveryInterval = failureDetectorAsyncRecoveryInterval;
    }

    public List<String> getFailureDetectorCatastrophicErrorTypes() {
        return failureDetectorCatastrophicErrorTypes;
    }

    /**
     * {@link ClientConfig#setFailureDetectorCatastrophicErrorTypes(List)}
     * 
     * <ul>
     * <li>Property :"failuredetector.catastrophic.error.types"</li>
     * <li>Default :FailureDetectorConfig.DEFAULT_CATASTROPHIC_ERROR_TYPES</li>
     * </ul>
     */
    public void setFailureDetectorCatastrophicErrorTypes(List<String> failureDetectorCatastrophicErrorTypes) {
        this.failureDetectorCatastrophicErrorTypes = failureDetectorCatastrophicErrorTypes;
    }

    public long getFailureDetectorRequestLengthThreshold() {
        return failureDetectorRequestLengthThreshold;
    }

    /**
     * {@link ClientConfig#setFailureDetectorRequestLengthThreshold(long)}
     * 
     * <ul>
     * <li>Property :"failuredetector.request.length.threshold"</li>
     * <li>Default :same as socket timeout</li>
     * </ul>
     */
    public void setFailureDetectorRequestLengthThreshold(long failureDetectorRequestLengthThreshold) {
        this.failureDetectorRequestLengthThreshold = failureDetectorRequestLengthThreshold;
    }

    public int getRetentionCleanupFirstStartTimeInHour() {
        return retentionCleanupFirstStartTimeInHour;
    }

    /**
     * The first hour in the day, when the {@link DataCleanupJob} will start
     * <ul>
     * <li>Property :"retention.cleanup.first.start.hour"</li>
     * <li>Default :0</li>
     * </ul>
     */
    public void setRetentionCleanupFirstStartTimeInHour(int retentionCleanupFirstStartTimeInHour) {
        this.retentionCleanupFirstStartTimeInHour = retentionCleanupFirstStartTimeInHour;
    }

    public int getRetentionCleanupFirstStartDayOfWeek() {
        return retentionCleanupFirstStartDayOfWeek;
    }

    /**
     * First day of the week to run {@link DataCleanupJob}, after server starts
     * up. From there on, it will run with the configured frequency. 1=SUN,
     * 2=MON, 3=TUE, 4=WED, 5=THU, 6=FRI,7=SAT
     * 
     * <ul>
     * <li>Property :"retention.cleanup.first.start.day"</li>
     * <li>Default :tomorrow</li>
     * </ul>
     */
    public void setRetentionCleanupFirstStartDayOfWeek(int retentionCleanupFirstStartDayOfWeek) {
        this.retentionCleanupFirstStartDayOfWeek = retentionCleanupFirstStartDayOfWeek;
    }

    public int getRetentionCleanupScheduledPeriodInHour() {
        return retentionCleanupScheduledPeriodInHour;
    }

    /**
     * Frequency to run {@link DataCleanupJob}
     * 
     * <ul>
     * <li>Property :</li>
     * <li>Default :</li>
     * </ul>
     */
    public void setRetentionCleanupScheduledPeriodInHour(int retentionCleanupScheduledPeriodInHour) {
        this.retentionCleanupScheduledPeriodInHour = retentionCleanupScheduledPeriodInHour;
    }

    public boolean getRetentionCleanupPinStartTime() {
        return retentionCleanupPinStartTime;
    }

    /**
     * if enabled, {@link DataCleanupJob} will be pinned to the same time each
     * run interval. Otherwise, it will slowly shift based on how long the job
     * actually takes to complete. See
     * {@link Timer#scheduleAtFixedRate(TimerTask, java.util.Date, long)}
     * 
     * <ul>
     * <li>Property :"retention.cleanup.pin.start.time"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setRetentionCleanupPinStartTime(boolean retentionCleanupFixStartTime) {
        this.retentionCleanupPinStartTime = retentionCleanupFixStartTime;
    }

    public boolean isEnforceRetentionPolicyOnRead() {
        return enforceRetentionPolicyOnRead;
    }

    /**
     * If enabled, the server will perform an expiry check for get and getall
     * and will not return stale entries
     * 
     * <ul>
     * <li>Property :"enforce.retention.policy.on.read"</li>
     * <li>Default :false</li>
     * </ul>
     */
    public void setEnforceRetentionPolicyOnRead(boolean enforceRetentionPolicyOnRead) {
        this.enforceRetentionPolicyOnRead = enforceRetentionPolicyOnRead;
    }

    public boolean isDeleteExpiredValuesOnRead() {
        return deleteExpiredValuesOnRead;
    }

    /**
     * If enabled, in addition to filtering stale entries, the server will also
     * delete the stale value
     * 
     * <ul>
     * <li>Property :"delete.expired.values.on.read"</li>
     * <li>Default :false</li>
     * </ul>
     */
    public void setDeleteExpiredValuesOnRead(boolean deleteExpiredValuesOnRead) {
        this.deleteExpiredValuesOnRead = deleteExpiredValuesOnRead;
    }

    public int getAdminSocketTimeout() {
        return adminSocketTimeout;
    }

    /**
     * {@link ClientConfig#setSocketTimeout(int, java.util.concurrent.TimeUnit)}
     * to use in AdminService
     * 
     * <ul>
     * <li>Property :"admin.client.socket.timeout.sec"</li>
     * <li>Default :24 * 60 * 60</li>
     * </ul>
     */
    public void setAdminSocketTimeout(int adminSocketTimeout) {
        this.adminSocketTimeout = adminSocketTimeout;
    }

    public int getAdminConnectionTimeout() {
        return adminConnectionTimeout;
    }

    /**
     * (
     * {@link ClientConfig#setConnectionTimeout(int, java.util.concurrent.TimeUnit)}
     * to use in AdminService
     * 
     * <ul>
     * <li>Property :"admin.client.connection.timeout.sec"</li>
     * <li>Default :60</li>
     * </ul>
     */
    public void setAdminConnectionTimeout(int adminConnectionTimeout) {
        this.adminConnectionTimeout = adminConnectionTimeout;
    }

    public long getRebalancingTimeoutSec() {
        return rebalancingTimeoutSec;
    }

    /**
     * The maximum amount of time the server will wait for the remote
     * rebalancing tasks to finish.
     * 
     * <ul>
     * <li>Property :"rebalancing.timeout.seconds"</li>
     * <li>Default :10 * 24 * 60 * 60</li>
     * </ul>
     */
    public void setRebalancingTimeoutSec(long rebalancingTimeoutSec) {
        this.rebalancingTimeoutSec = rebalancingTimeoutSec;
    }

    public boolean isGossipEnabled() {
        return enableGossip;
    }

    /**
     * Enabled gossip between servers, in server side routing.. Has no effect
     * when using client side routing, as in {@link DefaultStoreClient}
     * <ul>
     * <li>Property :"enable.gossip"</li>
     * <li>Default :false</li>
     * </ul>
     */
    public void setEnableGossip(boolean enableGossip) {
        this.enableGossip = enableGossip;
    }

    public String getReadOnlySearchStrategy() {
        return readOnlySearchStrategy;
    }

    public long getReadOnlyFetcherMaxBytesPerSecond() {
        return readOnlyFetcherMaxBytesPerSecond;
    }

    /**
     * Global throttle limit for all hadoop fetches. New flows will dynamically
     * share bandwidth with existing flows, to respect this parameter at all
     * times.
     *
     * <ul>
     * <li>Property :"fetcher.max.bytes.per.sec"</li>
     * <li>Default :0, No throttling</li>
     * </ul>
     */
    public void setReadOnlyFetcherMaxBytesPerSecond(long maxBytesPerSecond) {
        this.readOnlyFetcherMaxBytesPerSecond = maxBytesPerSecond;
    }

    public int getReadOnlyFetcherThrottlerInterval() {
        return readOnlyFetcherThrottlerInterval;
    }

    /**
     * When measuring the download rate of HDFS fetches, this parameter defines
     * the length in milliseconds of the two rolling windows. This is an
     * implementation detail which should typically not require any change.
     *
     * <ul>
     * <li>Property :"fetcher.throttler.interval"</li>
     * <li>Default : 1000 ms</li>
     * </ul>
     */
    public void setReadOnlyFetcherThrottlerInterval(int throttlerInterval) {
        this.readOnlyFetcherThrottlerInterval = throttlerInterval;
    }

    public long getReadOnlyFetcherReportingIntervalBytes() {
        return readOnlyFetcherReportingIntervalBytes;
    }

    /**
     * Interval to report statistics for HDFS fetches
     * 
     * <ul>
     * <li>Property :"fetcher.reporting.interval.bytes"</li>
     * <li>Default :25MB</li>
     * </ul>
     */
    public void setReadOnlyFetcherReportingIntervalBytes(long reportingIntervalBytes) {
        this.readOnlyFetcherReportingIntervalBytes = reportingIntervalBytes;
    }

    public int getReadOnlyFetchRetryCount() {
        return readOnlyFetchRetryCount;
    }

    /**
     * Number of attempts the readonly fetcher will make, before giving up on a
     * failed fetch from Hadoop
     * 
     * <ul>
     * <li>Property :"fetcher.retry.count"</li>
     * <li>Default :5</li>
     * </ul>
     */
    public void setReadOnlyFetchRetryCount(int readOnlyFetchRetryCount) {
        this.readOnlyFetchRetryCount = readOnlyFetchRetryCount;
    }

    public long getReadOnlyFetchRetryDelayMs() {
        return readOnlyFetchRetryDelayMs;
    }

    /**
     * Minimum delay in ms between readonly fetcher retries, to fetch data
     * from Hadoop. The maximum delay is 2x this amount, determined randomly.
     *
     * <ul>
     * <li>Property :"fetcher.retry.delay.ms"</li>
     * <li>Default :5000 (5 seconds)</li>
     * </ul>
     */
    public void setReadOnlyFetchRetryDelayMs(long readOnlyFetchRetryDelayMs) {
        this.readOnlyFetchRetryDelayMs = readOnlyFetchRetryDelayMs;
    }

    public long getReadOnlyLoginIntervalMs() {
        return readOnlyLoginIntervalMs;
    }

    /**
     * Minimum elapsed interval between HDFS logins. Setting this to a positive value
     * attempts to re-use HDFS authentication tokens across fetches, in order to
     * minimize load on KDC infrastructure.
     *
     * Setting this to -1 (which is the default) forces re-logging in every time.
     *
     * FIXME: Concurrent fetches sharing the same authentication token seem to clear each others' state,
     *        which prevents the later fetch from completing successfully.
     *
     * <ul>
     * <li>Property :"fetcher.login.interval.ms"</li>
     * <li>Default : -1</li>
     * </ul>
     */
    public void setReadOnlyLoginIntervalMs(long readOnlyLoginIntervalMs) {
        this.readOnlyLoginIntervalMs = readOnlyLoginIntervalMs;
    }

    public long getDefaultStorageSpaceQuotaInKB() {
        return defaultStorageSpaceQuotaInKB;
    }

    /**
     * Default storage space quota size in KB to be used for new stores that are
     * automatically created via Build And Push flow.
     * 
     * Setting this to -1 (which is the default) indicates no restriction in
     * disk quota and will continue to work the same way as if there were no
     * storage space quota.
     *  
     * <ul>
     * <li>Property :"default.storage.space.quota.in.kb"</li>
     * <li>Default : -1</li>
     * </ul>
     */
    public void setDefaultStorageSpaceQuotaInKB(long defaultStorageSpaceQuotaInKB) {
        this.defaultStorageSpaceQuotaInKB = defaultStorageSpaceQuotaInKB;
    }

    public int getFetcherBufferSize() {
        return fetcherBufferSize;
    }

    /**
     * Size of buffer to be used for HdfsFetcher. Note that this does not apply
     * to WebHDFS fetches.
     * 
     * <ul>
     * <li>Property :"hdfs.fetcher.buffer.size"</li>
     * <li>Default :64kb</li>
     * </ul>
     */
    public void setFetcherBufferSize(int fetcherBufferSize) {
        this.fetcherBufferSize = fetcherBufferSize;
    }


    public int getFetcherSocketTimeout() {
        return fetcherSocketTimeout;
    }

    /**
     * Amount of time (in ms) to block while waiting for content on a socket used
     * in the HdfsFetcher. Note that this does not apply to WebHDFS fetches.
     *
     * <ul>
     * <li>Property :"hdfs.fetcher.socket.timeout"</li>
     * <li>Default : 30 minutes</li>
     * </ul>
     */
    public void setFetcherSocketTimeout(int fetcherSocketTimeout) {
        this.fetcherSocketTimeout = fetcherSocketTimeout;
    }

    /**
     * Strategy to be used to search the read-only index for a given key. Either
     * {@link BinarySearchStrategy} or {@link InterpolationSearchStrategy}
     * 
     * <ul>
     * <li>Property :"readonly.search.strategy"</li>
     * <li>Default :BinarySearchStrategy.class.getName()</li>
     * </ul>
     */
    public void setReadOnlySearchStrategy(String readOnlySearchStrategy) {
        this.readOnlySearchStrategy = readOnlySearchStrategy;
    }

    public String getModifiedProtocol() {
        return this.modifiedProtocol;
    }

    /**
     * Set modified protocol used to fetch file.
     *
     * <ul>
     * <li>Property : "readonly.modify.protocol"</li>
     * <li>Default : null</li>
     * </ul>
     */
    public void setModifiedProtocol(String modifiedProtocol) {
        this.modifiedProtocol = modifiedProtocol;
    }

    public int getModifiedPort() {
        return this.modifiedPort;
    }

    /**
     * Set modifed port used to fetch file.
     *
     * <ul>
     * <li>Property : "readonly.modify.port"</li>
     * <li>Default : -1</li>
     * </ul>
     */
    public void setModifiedPort(int modifiedPort) {
        this.modifiedPort = modifiedPort;
    }

    public boolean isHighAvailabilityPushEnabled() {
        return highAvailabilityPushEnabled;
    }

    /**
     * Sets whether a high-availability strategy is to be used while pushing whole
     * data sets (such as when bulk loading Read-Only stores).
     *
     * <ul>
     * <li>Property : "push.ha.enabled"</li>
     * <li>Default : false</li>
     * </ul>
     */
    public void setHighAvailabilityPushEnabled(boolean highAvailabilityPushEnabled) {
        this.highAvailabilityPushEnabled = highAvailabilityPushEnabled;
    }

    public String getHighAvailabilityPushClusterId() {
        return highAvailabilityPushClusterId;
    }

    /**
     * When using a high-availability push strategy, the cluster ID uniquely
     * identifies the failure domain within which nodes can fail.
     *
     * Clusters containing the same stores but located in different data centers
     * should have different cluster IDs.
     *
     * <ul>
     * <li>Property : "push.ha.cluster.id"</li>
     * <li>Default : null</li>
     * </ul>
     */
    public void setHighAvailabilityPushClusterId(String highAvailabilityPushClusterId) {
        this.highAvailabilityPushClusterId = highAvailabilityPushClusterId;
    }

    public int getHighAvailabilityPushMaxNodeFailures() {
        return highAvailabilityPushMaxNodeFailures;
    }

    /**
     * When using a high-availability push strategy, there is a maximum amount of nodes
     * which can be allowed to have failed ingesting new data within a single failure
     * domain (which is identified by "push.ha.cluster.id").
     *
     * This should be set to a number which is equal or less than the lowest replication
     * factor across all stores hosted in the cluster.
     *
     * <ul>
     * <li>Property : "push.ha.max.node.failure"</li>
     * <li>Default : 0</li>
     * </ul>
     */
    public void setHighAvailabilityPushMaxNodeFailures(int highAvailabilityPushMaxNodeFailures) {
        this.highAvailabilityPushMaxNodeFailures = highAvailabilityPushMaxNodeFailures;
    }

    public String getHighAvailabilityPushLockPath() {
        return highAvailabilityPushLockPath;
    }

    /**
     * When using a high-availability push strategy, there is a certain path which is
     * used as a central lock in order to make sure we do not have disabled stores on
     * more nodes than specified by "push.ha.max.node.failure".
     *
     * At the moment, only an HDFS path is supported, but this could be extended to
     * support a ZK path, or maybe a shared mounted file-system path.
     *
     * <ul>
     * <li>Property : "push.ha.lock.path"</li>
     * <li>Default : null</li>
     * </ul>
     */
    public void setHighAvailabilityPushLockPath(String highAvailabilityPushLockPath) {
        this.highAvailabilityPushLockPath = highAvailabilityPushLockPath;
    }

    public String getHighAvailabilityPushLockImplementation() {
        return highAvailabilityPushLockImplementation;
    }

    /**
     * When using a high-availability push strategy, there needs to be a central lock 
     * in order to make sure we do not have disabled stores on more nodes than specified 
     * by "push.ha.max.node.failure".
     *
     * At the moment, only HDFS is supported as a locking mechanism, but this could be 
     * extended to support ZK, or maybe a shared mounted file-system.
     *
     * <ul>
     * <li>Property : "push.ha.lock.implementation"</li>
     * <li>Default : null</li>
     * </ul>
     */
    public void setHighAvailabilityPushLockImplementation(String highAvailabilityPushLockImplementation) {
        this.highAvailabilityPushLockImplementation = highAvailabilityPushLockImplementation;
    }

    public boolean isNetworkClassLoaderEnabled() {
        return enableNetworkClassLoader;
    }

    /**
     * Loads a class to be used as a {@link VoldemortFilter}. Note that this is
     * not officially supported
     * 
     * <ul>
     * <li>Property :"enable.network.classloader"</li>
     * <li>Default :false</li>
     * </ul>
     */
    public void setEnableNetworkClassLoader(boolean enableNetworkClassLoader) {
        this.enableNetworkClassLoader = enableNetworkClassLoader;
    }

    /**
     * If enabled, Rebalancing is enabled on the server
     * 
     * <ul>
     * <li>Property :"enable.rebalancing"</li>
     * <li>Default : true</li>
     * </ul>
     */
    public void setEnableRebalanceService(boolean enableRebalanceService) {
        this.enableRebalanceService = enableRebalanceService;
    }

    public boolean isEnableRebalanceService() {
        return enableRebalanceService;
    }

    public int getMaxParallelStoresRebalancing() {
        return maxParallelStoresRebalancing;
    }

    /**
     * The maximum number of stores that can be rebalancing at the same time.
     * This is one of the parameters that trades off between rebalancing speed
     * and impact to online traffic
     * 
     * <ul>
     * <li>Property :"max.parallel.stores.rebalancing"</li>
     * <li>Default :3</li>
     * </ul>
     */
    public void setMaxParallelStoresRebalancing(int maxParallelStoresRebalancing) {
        this.maxParallelStoresRebalancing = maxParallelStoresRebalancing;
    }

    public boolean usePartitionScanForRebalance() {
        return usePartitionScanForRebalance;
    }

    /**
     * Total number of threads needed to issue proxy puts during rebalancing
     * 
     * <ul>
     * <li>Property :"max.proxy.put.threads"</li>
     * <li>Default : 1</li>
     * </ul>
     */
    public void setMaxProxyPutThreads(int maxProxyPutThreads) {
        this.maxProxyPutThreads = maxProxyPutThreads;
    }

    public int getMaxProxyPutThreads() {
        return this.maxProxyPutThreads;
    }

    /**
     * Enables fast, efficient range scans to be used for rebalancing
     * 
     * Note: Only valid if the storage engine supports partition scans
     * {@link StorageEngine#isPartitionScanSupported()}
     * 
     * <ul>
     * <li>Property :"use.partition.scan.for.rebalance"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setUsePartitionScanForRebalance(boolean usePartitionScanForRebalance) {
        this.usePartitionScanForRebalance = usePartitionScanForRebalance;
    }

    public boolean isEnableJmxClusterName() {
        return enableJmxClusterName;
    }

    /**
     * If enabled, the cluster name will be used as a part of the Mbeans
     * created.
     * <ul>
     * <li>Property :"enable.jmx.clustername"</li>
     * <li>Default :false</li>
     * </ul>
     */
    public void setEnableJmxClusterName(boolean enableJmxClusterName) {
        this.enableJmxClusterName = enableJmxClusterName;
    }

    public boolean isEnableQuotaLimiting() {
        return enableQuotaLimiting;
    }

    /**
     * If enabled, provides the ability to enforce quotas per operation, per
     * store on the server, via Admin tool. Also needs stat tracking enabled
     * 
     * <ul>
     * <li>Property :"enable.quota.limit"</li>
     * <li>Default :true</li>
     * </ul>
     */
    public void setEnableQuotaLimit(boolean enableQuotaLimiting) {
        this.enableQuotaLimiting = enableQuotaLimiting;
    }

    public OpTimeMap testingGetSlowQueueingDelays() {
        return this.testingSlowQueueingDelays;
    }

    public OpTimeMap testingGetSlowConcurrentDelays() {
        return this.testingSlowConcurrentDelays;
    }

    public int getGossipInterval() {
        return gossipIntervalMs;
    }

    /**
     * When Gossip is enabled, time interval to exchange gossip messages between
     * servers
     * <ul>
     * <li>Property :"gossip.interval.ms"</li>
     * <li>Default :30000</li>
     * </ul>
     */
    public void setGossipInterval(int gossipIntervalMs) {
        this.gossipIntervalMs = gossipIntervalMs;
    }

    public boolean isRestServiceEnabled() {
        return enableRestService;
    }

    /**
     * Whether or not the {@link RestService} is enabled
     * <ul>
     * <li>Property :"rest.enable"</li>
     * <li>Default :false</li>
     * </ul>
     * 
     */
    public void setEnableRestService(boolean enableRestService) {
        this.enableRestService = enableRestService;
    }

    public int getRestServiceNettyServerBacklog() {
        return numRestServiceNettyServerBacklog;
    }

    /**
     * The capacity of the REST service Netty server backlog.
     * <ul>
     * <li>Property :"num.rest.service.netty.server.backlog"</li>
     * <li>Default : 1000</li>
     * </ul>
     */
    public void setRestServiceNettyServerBacklog(int numRestServiceNettyServerBacklog) {
        this.numRestServiceNettyServerBacklog = numRestServiceNettyServerBacklog;
    }

    public int getNumRestServiceNettyBossThreads() {
        return numRestServiceNettyBossThreads;
    }

    /**
     * The number of threads in the REST server Netty Boss thread pool.
     * <ul>
     * <li>Property :"num.rest.service.boss.threads"</li>
     * <li>Default :1</li>
     * </ul>
     */

    public void setNumRestServiceNettyBossThreads(int numRestServiceNettyBossThreads) {
        this.numRestServiceNettyBossThreads = numRestServiceNettyBossThreads;
    }

    public int getNumRestServiceNettyWorkerThreads() {
        return numRestServiceNettyWorkerThreads;
    }

    /**
     * The number of threads in the REST server Netty worker thread pool.
     * <ul>
     * <li>Property :"num.rest.service.worker.threads"</li>
     * <li>Default :10</li>
     * </ul>
     */
    public void setNumRestServiceNettyWorkerThreads(int numRestServiceNettyWorkerThreads) {
        this.numRestServiceNettyWorkerThreads = numRestServiceNettyWorkerThreads;
    }

    public int getNumRestServiceStorageThreads() {
        return numRestServiceStorageThreads;
    }

    /**
     * The number of threads in the REST server storage thread pool.
     * <ul>
     * <li>Property :"num.rest.service.storage.threads"</li>
     * <li>Default :50</li>
     * </ul>
     */
    public void setNumRestServiceStorageThreads(int numRestServiceStorageThreads) {
        this.numRestServiceStorageThreads = numRestServiceStorageThreads;
    }

    public int getRestServiceStorageThreadPoolQueueSize() {
        return restServiceStorageThreadPoolQueueSize;
    }

    /**
     * The capacity of the REST server storage thread pool queue.
     * <ul>
     * <li>Property :"rest.service.storage.thread.pool.queue.size"</li>
     * <li>Default :numRestServiceStorageThreads</li>
     * </ul>
     */
    public void setRestServiceStorageThreadPoolQueueSize(int restServiceStorageThreadPoolQueueSize) {
        this.restServiceStorageThreadPoolQueueSize = restServiceStorageThreadPoolQueueSize;
    }

    @Deprecated
    public int getMaxHttpAggregatedContentLength() {
        return maxHttpAggregatedContentLength;
    }

    /**
     * The maximum length of the aggregated Http content.
     * <ul>
     * <li>Property :"max.http.content.length"</li>
     * <li>Default : 1048576</li>
     * </ul>
     */
    @Deprecated
    public void setMaxHttpAggregatedContentLength(int maxHttpAggregatedContentLength) {
        this.maxHttpAggregatedContentLength = maxHttpAggregatedContentLength;
    }

    public int getRepairJobMaxKeysScannedPerSec() {
        return repairJobMaxKeysScannedPerSec;
    }

    /**
     * Global throttle limit for repair jobs
     * 
     * <ul>
     * <li>Property :"repairjob.max.keys.scanned.per.sec"</li>
     * <li>Default : Integer.MAX_VALUE (unthrottled)</li>
     * </ul>
     */
    public void setRepairJobMaxKeysScannedPerSec(int maxKeysPerSecond) {
        this.repairJobMaxKeysScannedPerSec = maxKeysPerSecond;
    }

    public int getPruneJobMaxKeysScannedPerSec() {
        return pruneJobMaxKeysScannedPerSec;
    }

    /**
     * Global throttle limit for versioned put prune jobs
     * 
     * <ul>
     * <li>Property :"prunejob.max.keys.scanned.per.sec"</li>
     * <li>Default : Integer.MAX_VALUE (unthrottled)</li>
     * </ul>
     */
    public void setPruneJobMaxKeysScannedPerSec(int maxKeysPerSecond) {
        this.pruneJobMaxKeysScannedPerSec = maxKeysPerSecond;
    }

    public int getSlopPurgeJobMaxKeysScannedPerSec() {
        return slopPurgeJobMaxKeysScannedPerSec;
    }

    /**
     * Global throttle limit for slop purge jobs
     * 
     * <ul>
     * <li>Property :"slop.purgejob.max.keys.scanned.per.sec"</li>
     * <li>Default : 10k</li>
     * </ul>
     */
    public void setSlopPurgeJobMaxKeysScannedPerSec(int maxKeysPerSecond) {
        this.slopPurgeJobMaxKeysScannedPerSec = maxKeysPerSecond;
    }

    /**
     * Kdc for kerberized Hadoop grids
     * 
     * <ul>
     * <li>Property :"readonly.kerberos.kdc"</li>
     * <li>Default :""</li>
     * </ul>
     */
    public void setReadOnlyKerberosKdc(String kerberosKdc) {
        this.readOnlyKerberosKdc = kerberosKdc;
    }

    public String getReadOnlyKerberosKdc() {
        return this.readOnlyKerberosKdc;
    }

    /**
     * kerberized hadoop realm
     * 
     * <ul>
     * <li>Property :"readonly.kerberos.realm"</li>
     * <li>Default : ""</li>
     * </ul>
     * 
     * @return
     */
    public void setReadOnlyKerberosRealm(String kerberosRealm) {
        this.readOnlykerberosRealm = kerberosRealm;
    }

    public String getReadOnlyKerberosRealm() {
        return this.readOnlykerberosRealm;
    }

    /**
     * Read-only file fetcher class
     * 
     * <ul>
     * <li>Property :"file.fetcher.class"</li>
     * <li>Default : "voldemort.store.readonly.fetcher.HdfsFetcher"</li>
     * </ul>
     * 
     * @return
     */
    public void setFileFetcherClass(String fileFetcherClass) {
        this.fileFetcherClass = fileFetcherClass;
    }

    public String getFileFetcherClass() {
        return this.fileFetcherClass;
    }

    public boolean isReadOnlyStatsFileEnabled() {
        return readOnlyStatsFileEnabled;
    }

    public void setReadOnlyStatsFileEnabled(boolean readOnlyStatsFileEnabled) {
        this.readOnlyStatsFileEnabled = readOnlyStatsFileEnabled;
    }

    public int getReadOnlyMaxVersionsStatsFile() {
        return readOnlyMaxVersionsStatsFile;
    }

    public void setReadOnlyMaxVersionsStatsFile(int readOnlyMaxVersionsStatsFile) {
        this.readOnlyMaxVersionsStatsFile = readOnlyMaxVersionsStatsFile;
    }

    public int getReadOnlyMaxValueBufferAllocationSize() {
        return readOnlyMaxValueBufferAllocationSize;
    }

    /**
     * Internal safe guard to avoid GC (and possibly OOM) from excessive buffer allocation.
     *
     * More importantly, if the server is given corrupted (or mismatched) index/data Read-Only files,
     * it could be the case that the server accidentally attempts to read abnormally large values.
     *
     * Large value sizes are not good use cases for Voldemort so this should theoretically never come
     * into play. The default config value is already absurdly high, and should thus never need to be raised.
     *
     * <ul>
     * <li>Property : "readonly.max.value.buffer.allocation.size"</li>
     * <li>Default : 25 MB</li>
     * </ul>
     *
     * @param readOnlyMaxValueBufferAllocationSize
     */
    public void setReadOnlyMaxValueBufferAllocationSize(int readOnlyMaxValueBufferAllocationSize) {
        this.readOnlyMaxValueBufferAllocationSize = readOnlyMaxValueBufferAllocationSize;
    }

    public String getReadOnlyCompressionCodec() {
        return this.readOnlyCompressionCodec;
    }

    /**
     * Compression codec expected by Read-Only Voldemort Server
     * 
     * @param readOnlyCompressionCodec
     * 
     *        <ul>
     *        <li>Property :"readonly.compression.codec"</li>
     *        <li>Default : "NO_CODEC"</li>
     *        </ul>
     */
    public void setReadOnlyCompressionCodec(String readOnlyCompressionCodec) {
        this.readOnlyCompressionCodec = readOnlyCompressionCodec;
    }

    public String getRdbDataDirectory() {
        return rocksdbDataDirectory;
    }

    /**
     * Where RocksDB should put its data directories
     * 
     * <ul>
     * <li>Property :"rocksdb.data.dir"</li>
     * <li>Default : "/tmp/rdb_data_dir"</li>
     * </ul>
     * 
     * @param rdbDataDirectory
     */
    public void setRdbDataDirectory(String rdbDataDirectory) {
        this.rocksdbDataDirectory = rdbDataDirectory;
    }

    public boolean getRocksdbPrefixKeysWithPartitionId() {
        return rocksdbPrefixKeysWithPartitionId;
    }

    /**
     * If true, keys will be prefixed by the partition Id on disk. This can
     * possibly speed up rebalancing, restore operations, at the cost of 2 bytes
     * of extra storage per key
     * 
     * @param rocksdbPrefixKeysWithPartitionId
     */
    public void setRocksdbPrefixKeysWithPartitionId(boolean rocksdbPrefixKeysWithPartitionId) {
        this.rocksdbPrefixKeysWithPartitionId = rocksdbPrefixKeysWithPartitionId;
    }

    public boolean isRocksdbEnableReadLocks() {
        return rocksdbEnableReadLocks;
    }

    /**
     * If set to true get API will be synchronized. By default this feature is
     * disabled.
     * 
     * @param rocksdbEnableReadLocks
     */
    public void setRocksdbEnableReadLocks(boolean rocksdbEnableReadLocks) {
        this.rocksdbEnableReadLocks = rocksdbEnableReadLocks;
    }

    /**
     * If set to true client connections to the nio admin server will have SO_KEEPALIVE on,
     * to tell OS to close dead client connections
     * <ul>
     * <li>Property :"nio.admin.connector.keepalive"</li>
     * <li>Default : "false"</li>
     * </ul>
     * @param nioAdminConnectorKeepAlive
     */
    public void setNioAdminConnectorKeepAlive(boolean nioAdminConnectorKeepAlive) {
        this.nioAdminConnectorKeepAlive = nioAdminConnectorKeepAlive;
    }
    public boolean isNioAdminConnectorKeepAlive() {
        return nioAdminConnectorKeepAlive;
    }

    /**
     * If set to true client connections to the server will have SO_KEEPALIVE on,
     * to tell OS to close dead client connections
     * <ul>
     * <li>Property :"nio.connector.keepalive"</li>
     * <li>Default : "false"</li>
     * </ul>
     *
     * @param nioConnectorKeepAlive
     */
    public void setNioConnectorKeepAlive(boolean nioConnectorKeepAlive) {
        this.nioConnectorKeepAlive = nioConnectorKeepAlive;
    }

    public boolean isNioConnectorKeepAlive() {
        return nioConnectorKeepAlive;
    }

}
