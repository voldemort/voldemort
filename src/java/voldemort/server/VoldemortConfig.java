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
import com.google.common.collect.Lists;

/**
 * Configuration parameters for the voldemort server.
 */
public class VoldemortConfig implements Serializable {

    private static final long serialVersionUID = 1;

    // Config keys
    public static final String NODE_ID = "node.id";
    public static final String VOLDEMORT_HOME = "voldemort.home";
    public static final String DATA_DIRECTORY = "data.directory";
    public static final String METADATA_DIRECTORY = "metadata.directory";
    public static final String BDB_CACHE_SIZE = "bdb.cache.size";
    public static final String BDB_WRITE_TRANSACTIONS = "bdb.write.transactions";
    public static final String BDB_FLUSH_TRANSACTIONS = "bdb.flush.transactions";
    public static final String BDB_DATA_DIRECTORY = "bdb.data.directory";
    public static final String BDB_MAX_LOGFILE_SIZE = "bdb.max.logfile.size";
    public static final String BDB_BTREE_FANOUT = "bdb.btree.fanout";
    public static final String BDB_MAX_DELTA = "bdb.max.delta";
    public static final String BDB_BIN_DELTA = "bdb.bin.delta";
    public static final String BDB_CHECKPOINT_INTERVAL_BYTES = "bdb.checkpoint.interval.bytes";
    public static final String BDB_CHECKPOINT_INTERVAL_MS = "bdb.checkpoint.interval.ms";
    public static final String BDB_ONE_ENV_PER_STORE = "bdb.one.env.per.store";
    public static final String BDB_CLEANER_MIN_FILE_UTILIZATION = "bdb.cleaner.min.file.utilization";
    public static final String BDB_CLEANER_MIN_UTILIZATION = "bdb.cleaner.minUtilization";
    public static final String BDB_CLEANER_THREADS = "bdb.cleaner.threads";
    public static final String BDB_CLEANER_INTERVAL_BYTES = "bdb.cleaner.interval.bytes";
    public static final String BDB_CLEANER_LOOKAHEAD_CACHE_SIZE = "bdb.cleaner.lookahead.cache.size";
    public static final String BDB_LOCK_TIMEOUT_MS = "bdb.lock.timeout.ms";
    public static final String BDB_LOCK_N_LOCK_TABLES = "bdb.lock.nLockTables";
    public static final String BDB_LOG_FAULT_READ_SIZE = "bdb.log.fault.read.size";
    public static final String BDB_LOG_ITERATOR_READ_SIZE = "bdb.log.iterator.read.size";
    public static final String BDB_FAIR_LATCHES = "bdb.fair.latches";
    public static final String BDB_CHECKPOINTER_HIGH_PRIORITY = "bdb.checkpointer.high.priority";
    public static final String BDB_CLEANER_MAX_BATCH_FILES = "bdb.cleaner.max.batch.files";
    public static final String BDB_LOCK_READ_UNCOMMITTED = "bdb.lock.read_uncommitted";
    public static final String BDB_STATS_CACHE_TTL_MS = "bdb.stats.cache.ttl.ms";
    public static final String BDB_EXPOSE_SPACE_UTILIZATION = "bdb.expose.space.utilization";
    public static final String BDB_MINIMUM_SHARED_CACHE = "bdb.minimum.shared.cache";
    public static final String BDB_CLEANER_LAZY_MIGRATION = "bdb.cleaner.lazy.migration";
    public static final String BDB_CACHE_EVICTLN = "bdb.cache.evictln";
    public static final String BDB_MINIMIZE_SCAN_IMPACT = "bdb.minimize.scan.impact";
    public static final String BDB_PREFIX_KEYS_WITH_PARTITIONID = "bdb.prefix.keys.with.partitionid";
    public static final String BDB_EVICT_BY_LEVEL = "bdb.evict.by.level";
    public static final String BDB_CHECKPOINTER_OFF_BATCH_WRITES = "bdb.checkpointer.off.batch.writes";
    public static final String BDB_CLEANER_FETCH_OBSOLETE_SIZE = "bdb.cleaner.fetch.obsolete.size";
    public static final String BDB_CLEANER_ADJUST_UTILIZATION = "bdb.cleaner.adjust.utilization";
    public static final String BDB_RECOVERY_FORCE_CHECKPOINT = "bdb.recovery.force.checkpoint";
    public static final String BDB_RAW_PROPERTY_STRING = "bdb.raw.property.string";
    public static final String READONLY_HADOOP_CONFIG_PATH = "readonly.hadoop.config.path";
    public static final String READONLY_BACKUPS = "readonly.backups";
    public static final String READONLY_SEARCH_STRATEGY = "readonly.search.strategy";
    public static final String READONLY_DATA_DIRECTORY = "readonly.data.directory";
    public static final String READONLY_DELETE_BACKUP_MS = "readonly.delete.backup.ms";
    public static final String READONLY_KEYTAB_PATH = "readonly.keytab.path";
    public static final String READONLY_KERBEROS_USER = "readonly.kerberos.user";
    public static final String READONLY_KERBEROS_KDC = "readonly.kerberos.kdc";
    public static final String READONLY_KERBEROS_REALM = "readonly.kerberos.realm";
    public static final String FETCHER_MAX_BYTES_PER_SEC = "fetcher.max.bytes.per.sec";
    public static final String FETCHER_REPORTING_INTERVAL_BYTES = "fetcher.reporting.interval.bytes";
    public static final String FETCHER_THROTTLER_INTERVAL = "fetcher.throttler.interval";
    public static final String FETCHER_RETRY_COUNT = "fetcher.retry.count";
    public static final String FETCHER_RETRY_DELAY_MS = "fetcher.retry.delay.ms";
    public static final String FETCHER_LOGIN_INTERVAL_MS = "fetcher.login.interval.ms";
    public static final String DEFAULT_STORAGE_SPACE_QUOTA_IN_KB = "default.storage.space.quota.in.kb";
    public static final String HDFS_FETCHER_BUFFER_SIZE = "hdfs.fetcher.buffer.size";
    public static final String HDFS_FETCHER_SOCKET_TIMEOUT = "hdfs.fetcher.socket.timeout";
    public static final String FILE_FETCHER_CLASS = "file.fetcher.class";
    public static final String READONLY_STATS_FILE_ENABLED = "readonly.stats.file.enabled";
    public static final String READONLY_STATS_FILE_MAX_VERSIONS = "readonly.stats.file.max.versions";
    public static final String READONLY_MAX_VALUE_BUFFER_ALLOCATION_SIZE = "readonly.max.value.buffer.allocation.size";
    public static final String READONLY_COMPRESSION_CODEC = "readonly.compression.codec";
    public static final String READONLY_MODIFY_PROTOCOL = "readonly.modify.protocol";
    public static final String READONLY_MODIFY_PORT = "readonly.modify.port";
    public static final String READONLY_OMIT_PORT = "readonly.omit.port";
    public static final String USE_BOUNCYCASTLE_FOR_SSL = "use.bouncycastle.for.ssl";
    public static final String READONLY_BUILD_PRIMARY_REPLICAS_ONLY = "readonly.build.primary.replicas.only";
    public static final String PUSH_HA_ENABLED = "push.ha.enabled";
    public static final String PUSH_HA_CLUSTER_ID = "push.ha.cluster.id";
    public static final String PUSH_HA_LOCK_PATH = "push.ha.lock.path";
    public static final String PUSH_HA_LOCK_IMPLEMENTATION = "push.ha.lock.implementation";
    public static final String PUSH_HA_MAX_NODE_FAILURES = "push.ha.max.node.failure";
    public static final String PUSH_HA_STATE_AUTO_CLEANUP = "push.ha.state.auto.cleanup";
    public static final String MYSQL_USER = "mysql.user";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String TESTING_SLOW_QUEUEING_GET_MS = "testing.slow.queueing.get.ms";
    public static final String TESTING_SLOW_QUEUEING_GETALL_MS = "testing.slow.queueing.getall.ms";
    public static final String TESTING_SLOW_QUEUEING_GETVERSIONS_MS = "testing.slow.queueing.getversions.ms";
    public static final String TESTING_SLOW_QUEUEING_PUT_MS = "testing.slow.queueing.put.ms";
    public static final String TESTING_SLOW_QUEUEING_DELETE_MS = "testing.slow.queueing.delete.ms";
    public static final String TESTING_SLOW_CONCURRENT_GET_MS = "testing.slow.concurrent.get.ms";
    public static final String TESTING_SLOW_CONCURRENT_GETALL_MS = "testing.slow.concurrent.getall.ms";
    public static final String TESTING_SLOW_CONCURRENT_GETVERSIONS_MS = "testing.slow.concurrent.getversions.ms";
    public static final String TESTING_SLOW_CONCURRENT_PUT_MS = "testing.slow.concurrent.put.ms";
    public static final String TESTING_SLOW_CONCURRENT_DELETE_MS = "testing.slow.concurrent.delete.ms";
    public static final String MAX_THREADS = "max.threads";
    public static final String CORE_THREADS = "core.threads";
    public static final String ADMIN_MAX_THREADS = "admin.max.threads";
    public static final String ADMIN_CORE_THREADS = "admin.core.threads";
    public static final String ADMIN_STREAMS_BUFFER_SIZE = "admin.streams.buffer.size";
    public static final String ADMIN_CLIENT_CONNECTION_TIMEOUT_SEC = "admin.client.connection.timeout.sec";
    public static final String ADMIN_CLIENT_SOCKET_TIMEOUT_SEC = "admin.client.socket.timeout.sec";
    public static final String STREAM_READ_BYTE_PER_SEC = "stream.read.byte.per.sec";
    public static final String STREAM_WRITE_BYTE_PER_SEC = "stream.write.byte.per.sec";
    public static final String USE_MULTI_VERSION_STREAMING_PUTS = "use.multi.version.streaming.puts";
    public static final String SOCKET_TIMEOUT_MS = "socket.timeout.ms";
    public static final String SOCKET_BUFFER_SIZE = "socket.buffer.size";
    public static final String SOCKET_KEEPALIVE = "socket.keepalive";
    public static final String ENABLE_NIO_CONNECTOR = "enable.nio.connector";
    public static final String NIO_CONNECTOR_KEEPALIVE = "nio.connector.keepalive";
    public static final String NIO_CONNECTOR_SELECTORS = "nio.connector.selectors";
    public static final String NIO_ADMIN_CONNECTOR_SELECTORS = "nio.admin.connector.selectors";
    public static final String NIO_ADMIN_CONNECTOR_KEEPALIVE = "nio.admin.connector.keepalive";
    public static final String NIO_ACCEPTOR_BACKLOG = "nio.acceptor.backlog";
    public static final String NIO_SELECTOR_MAX_HEART_BEAT_TIME_MS = "nio.selector.max.heart.beat.time.ms";
    public static final String CLIENT_SELECTORS = "client.selectors";
    public static final String CLIENT_MAX_CONNECTIONS_PER_NODE = "client.max.connections.per.node";
    public static final String CLIENT_CONNECTION_TIMEOUT_MS = "client.connection.timeout.ms";
    public static final String CLIENT_ROUTING_TIMEOUT_MS = "client.routing.timeout.ms";
    public static final String CLIENT_ROUTING_GET_TIMEOUT_MS = "client.routing.get.timeout.ms";
    public static final String CLIENT_ROUTING_GETALL_TIMEOUT_MS = "client.routing.getall.timeout.ms";
    public static final String CLIENT_ROUTING_PUT_TIMEOUT_MS = "client.routing.put.timeout.ms";
    public static final String CLIENT_ROUTING_GETVERSIONS_TIMEOUT_MS = "client.routing.getversions.timeout.ms";
    public static final String CLIENT_ROUTING_DELETE_TIMEOUT_MS = "client.routing.delete.timeout.ms";
    public static final String CLIENT_ROUTING_ALLOW_PARTIAL_GETALL = "client.routing.allow.partial.getall";
    public static final String CLIENT_MAX_THREADS = "client.max.threads";
    public static final String CLIENT_THREAD_IDLE_MS = "client.thread.idle.ms";
    public static final String CLIENT_MAX_QUEUED_REQUESTS = "client.max.queued.requests";
    public static final String HTTP_ENABLE = "http.enable";
    public static final String SOCKET_ENABLE = "socket.enable";
    public static final String ADMIN_ENABLE = "admin.enable";
    public static final String JMX_ENABLE = "jmx.enable";
    public static final String SLOP_ENABLE = "slop.enable";
    public static final String SLOP_PUSHER_ENABLE = "slop.pusher.enable";
    public static final String SLOP_WRITE_BYTE_PER_SEC = "slop.write.byte.per.sec";
    public static final String ENABLE_VERBOSE_LOGGING = "enable.verbose.logging";
    public static final String ENABLE_STAT_TRACKING = "enable.stat.tracking";
    public static final String ENABLE_SERVER_ROUTING = "enable.server.routing";
    public static final String ENABLE_METADATA_CHECKING = "enable.metadata.checking";
    public static final String ENABLE_GOSSIP = "enable.gossip";
    public static final String ENABLE_REBALANCING = "enable.rebalancing";
    public static final String ENABLE_REPAIR = "enable.repair";
    public static final String ENABLE_PRUNEJOB = "enable.prunejob";
    public static final String ENABLE_SLOP_PURGE_JOB = "enable.slop.purge.job";
    public static final String ENABLE_JMX_CLUSTERNAME = "enable.jmx.clustername";
    public static final String ENABLE_QUOTA_LIMITING = "enable.quota.limiting";
    public static final String GOSSIP_INTERVAL_MS = "gossip.interval.ms";
    public static final String SLOP_WRITE_BYTE_PER_SEC1 = "slop.write.byte.per.sec";
    public static final String SLOP_READ_BYTE_PER_SEC = "slop.read.byte.per.sec";
    public static final String SLOP_STORE_ENGINE = "slop.store.engine";
    public static final String SLOP_FREQUENCY_MS = "slop.frequency.ms";
    public static final String SLOP_BATCH_SIZE = "slop.batch.size";
    public static final String PUSHER_TYPE = "pusher.type";
    public static final String SLOP_ZONES_TERMINATE = "slop.zones.terminate";
    public static final String AUTO_PURGE_DEAD_SLOPS = "auto.purge.dead.slops";
    public static final String SCHEDULER_THREADS = "scheduler.threads";
    public static final String SERVICE_INTERRUPTIBLE = "service.interruptible";
    public static final String NUM_SCAN_PERMITS = "num.scan.permits";
    public static final String STORAGE_CONFIGS = "storage.configs";
    public static final String RETENTION_CLEANUP_FIRST_START_HOUR = "retention.cleanup.first.start.hour";
    public static final String RETENTION_CLEANUP_FIRST_START_DAY = "retention.cleanup.first.start.day";
    public static final String RETENTION_CLEANUP_PERIOD_HOURS = "retention.cleanup.period.hours";
    public static final String RETENTION_CLEANUP_PIN_START_TIME = "retention.cleanup.pin.start.time";
    public static final String ENFORCE_RETENTION_POLICY_ON_READ = "enforce.retention.policy.on.read";
    public static final String DELETE_EXPIRED_VALUES_ON_READ = "delete.expired.values.on.read";
    public static final String REQUEST_FORMAT = "request.format";
    public static final String REBALANCING_TIMEOUT_SECONDS = "rebalancing.timeout.seconds";
    public static final String MAX_PARALLEL_STORES_REBALANCING = "max.parallel.stores.rebalancing";
    public static final String USE_PARTITION_SCAN_FOR_REBALANCE = "use.partition.scan.for.rebalance";
    public static final String MAX_PROXY_PUT_THREADS = "max.proxy.put.threads";
    public static final String FAILUREDETECTOR_IMPLEMENTATION = "failuredetector.implementation";
    public static final String CLIENT_NODE_BANNAGE_MS = "client.node.bannage.ms";
    public static final String FAILUREDETECTOR_BANNAGE_PERIOD = "failuredetector.bannage.period";
    public static final String FAILUREDETECTOR_THRESHOLD = "failuredetector.threshold";
    public static final String FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM = "failuredetector.threshold.countminimum";
    public static final String FAILUREDETECTOR_THRESHOLD_INTERVAL = "failuredetector.threshold.interval";
    public static final String FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL = "failuredetector.asyncrecovery.interval";
    public static final String FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES = "failuredetector.catastrophic.error.types";
    public static final String FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD = "failuredetector.request.length.threshold";
    public static final String ENABLE_NETWORK_CLASSLOADER = "enable.network.classloader";
    public static final String REST_ENABLE = "rest.enable";
    public static final String NUM_REST_SERVICE_NETTY_SERVER_BACKLOG = "num.rest.service.netty.server.backlog";
    public static final String NUM_REST_SERVICE_NETTY_BOSS_THREADS = "num.rest.service.netty.boss.threads";
    public static final String NUM_REST_SERVICE_NETTY_WORKER_THREADS = "num.rest.service.netty.worker.threads";
    public static final String NUM_REST_SERVICE_STORAGE_THREADS = "num.rest.service.storage.threads";
    public static final String REST_SERVICE_STORAGE_THREAD_POOL_QUEUE_SIZE = "rest.service.storage.thread.pool.queue.size";
    public static final String MAX_HTTP_AGGREGATED_CONTENT_LENGTH = "max.http.aggregated.content.length";
    public static final String REPAIRJOB_MAX_KEYS_SCANNED_PER_SEC = "repairjob.max.keys.scanned.per.sec";
    public static final String PRUNEJOB_MAX_KEYS_SCANNED_PER_SEC = "prunejob.max.keys.scanned.per.sec";
    public static final String SLOP_PURGEJOB_MAX_KEYS_SCANNED_PER_SEC = "slop.purgejob.max.keys.scanned.per.sec";
    public static final String ENABLE_NODE_ID_DETECTION = "enable.node.id.detection";
    public static final String VALIDATE_NODE_ID = "validate.node.id";
    // Options prefixed with "rocksdb.db.options." or "rocksdb.cf.options." can be used to tune RocksDB performance
    // and are passed directly to the RocksDB configuration code. See the RocksDB documentation for details:
    //   https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
    //   https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    public static final String ROCKSDB_DB_OPTIONS = "rocksdb.db.options.";
    public static final String ROCKSDB_CF_OPTIONS = "rocksdb.cf.options.";
    public static final String ROCKSDB_DATA_DIR = "rocksdb.data.dir";
    public static final String ROCKSDB_PREFIX_KEYS_WITH_PARTITIONID = "rocksdb.prefix.keys.with.partitionid";
    public static final String ROCKSDB_ENABLE_READ_LOCKS = "rocksdb.enable.read.locks";
    public static final String RESTRICTED_CONFIGS = "restricted.configs";

    // Environment variables
    public static final String VOLDEMORT_HOME_VAR_NAME = "VOLDEMORT_HOME";
    public static final String VOLDEMORT_CONFIG_DIR = "VOLDEMORT_CONFIG_DIR";
    private static final String VOLDEMORT_NODE_ID_VAR_NAME = "VOLDEMORT_NODE_ID";

    // Frequently used default values
    public static final long DEFAULT_REPORTING_INTERVAL_BYTES = 25 * 1024 * 1024;
    public static final int DEFAULT_FETCHER_BUFFER_SIZE = 64 * 1024;
    public static final int DEFAULT_FETCHER_SOCKET_TIMEOUT = 1000 * 60 * 30; // 30 minutes
    public static final int DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS = 1000;
    public static final long DEFAULT_DEFAULT_STORAGE_SPACE_QUOTA_IN_KB = -1L; // -1 represents no storage space quota constraint
    public static final int DEFAULT_RO_MAX_VALUE_BUFFER_ALLOCATION_SIZE = 25 * 1024 * 1024;

    private static final Props defaultConfig = new Props();

    static {
        defaultConfig.put(BDB_CACHE_SIZE, 200 * 1024 * 1024);
        defaultConfig.put(BDB_WRITE_TRANSACTIONS, false);
        defaultConfig.put(BDB_FLUSH_TRANSACTIONS, false);
        defaultConfig.put(BDB_MAX_LOGFILE_SIZE, 60 * 1024 * 1024);
        defaultConfig.put(BDB_BTREE_FANOUT, 512);
        defaultConfig.put(BDB_MAX_DELTA, 100);
        defaultConfig.put(BDB_BIN_DELTA, 75);
        defaultConfig.put(BDB_CHECKPOINT_INTERVAL_BYTES, 200 * 1024 * 1024);
        defaultConfig.put(BDB_CHECKPOINT_INTERVAL_MS, 30 * Time.MS_PER_SECOND);
        defaultConfig.put(BDB_ONE_ENV_PER_STORE, false);
        defaultConfig.put(BDB_CLEANER_MIN_FILE_UTILIZATION, 0);
        defaultConfig.put(BDB_CLEANER_MIN_UTILIZATION, 50);
        defaultConfig.put(BDB_CLEANER_THREADS, 1);
        // by default, wake up the cleaner everytime we write log file size *
        // utilization% bytes. So, by default 30MB
        defaultConfig.put(BDB_CLEANER_INTERVAL_BYTES, 30 * 1024 * 1024);
        defaultConfig.put(BDB_CLEANER_LOOKAHEAD_CACHE_SIZE, 8192);
        defaultConfig.put(BDB_LOCK_TIMEOUT_MS, 500);
        defaultConfig.put(BDB_LOCK_N_LOCK_TABLES, 7);
        defaultConfig.put(BDB_LOG_FAULT_READ_SIZE, 2048);
        defaultConfig.put(BDB_LOG_ITERATOR_READ_SIZE, 8192);
        defaultConfig.put(BDB_FAIR_LATCHES, false);
        defaultConfig.put(BDB_CHECKPOINTER_HIGH_PRIORITY, false);
        defaultConfig.put(BDB_CLEANER_MAX_BATCH_FILES, 0);
        defaultConfig.put(BDB_LOCK_READ_UNCOMMITTED, true);
        defaultConfig.put(BDB_STATS_CACHE_TTL_MS, 5 * Time.MS_PER_SECOND);
        defaultConfig.put(BDB_EXPOSE_SPACE_UTILIZATION, true);
        defaultConfig.put(BDB_MINIMUM_SHARED_CACHE, 0);
        defaultConfig.put(BDB_CLEANER_LAZY_MIGRATION, false);
        defaultConfig.put(BDB_CACHE_EVICTLN, true);
        defaultConfig.put(BDB_MINIMIZE_SCAN_IMPACT, true);
        defaultConfig.put(BDB_PREFIX_KEYS_WITH_PARTITIONID, true);
        defaultConfig.put(BDB_EVICT_BY_LEVEL, false);
        defaultConfig.put(BDB_CHECKPOINTER_OFF_BATCH_WRITES, false);
        defaultConfig.put(BDB_CLEANER_FETCH_OBSOLETE_SIZE, true);
        defaultConfig.put(BDB_CLEANER_ADJUST_UTILIZATION, false);
        defaultConfig.put(BDB_RECOVERY_FORCE_CHECKPOINT, false);
        defaultConfig.put(BDB_RAW_PROPERTY_STRING, (String) null);

        defaultConfig.put(READONLY_BACKUPS, 1);
        defaultConfig.put(READONLY_SEARCH_STRATEGY, BinarySearchStrategy.class.getName());
        defaultConfig.put(READONLY_DELETE_BACKUP_MS, 0);
        defaultConfig.put(FETCHER_MAX_BYTES_PER_SEC, 0);
        defaultConfig.put(FETCHER_REPORTING_INTERVAL_BYTES, DEFAULT_REPORTING_INTERVAL_BYTES);
        defaultConfig.put(FETCHER_THROTTLER_INTERVAL, DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS);
        defaultConfig.put(FETCHER_RETRY_COUNT, 5);
        defaultConfig.put(FETCHER_RETRY_DELAY_MS, 5000);
        defaultConfig.put(FETCHER_LOGIN_INTERVAL_MS, -1);
        defaultConfig.put(DEFAULT_STORAGE_SPACE_QUOTA_IN_KB, DEFAULT_DEFAULT_STORAGE_SPACE_QUOTA_IN_KB);
        defaultConfig.put(HDFS_FETCHER_BUFFER_SIZE, DEFAULT_FETCHER_BUFFER_SIZE);
        defaultConfig.put(HDFS_FETCHER_SOCKET_TIMEOUT, DEFAULT_FETCHER_SOCKET_TIMEOUT);
        defaultConfig.put(READONLY_KERBEROS_USER, "voldemrt");
        defaultConfig.put(READONLY_KERBEROS_KDC, "");
        defaultConfig.put(READONLY_KERBEROS_REALM, "");
        defaultConfig.put(FILE_FETCHER_CLASS, (String) null); // FIXME: Some unit tests fail without this default.
        defaultConfig.put(READONLY_STATS_FILE_ENABLED, true);
        defaultConfig.put(READONLY_STATS_FILE_MAX_VERSIONS, 1000);
        defaultConfig.put(READONLY_MAX_VALUE_BUFFER_ALLOCATION_SIZE, VoldemortConfig.DEFAULT_RO_MAX_VALUE_BUFFER_ALLOCATION_SIZE);
        // To enable block-level compression over the wire for Read-Only fetches, set this property to "GZIP"
        defaultConfig.put(READONLY_COMPRESSION_CODEC, "NO_CODEC");
        defaultConfig.put(READONLY_MODIFY_PROTOCOL, "");
        defaultConfig.put(READONLY_MODIFY_PORT, -1);
        defaultConfig.put(READONLY_OMIT_PORT, false);
        defaultConfig.put(USE_BOUNCYCASTLE_FOR_SSL, false);
        defaultConfig.put(READONLY_BUILD_PRIMARY_REPLICAS_ONLY, true);

        defaultConfig.put(PUSH_HA_CLUSTER_ID, (String) null);
        defaultConfig.put(PUSH_HA_LOCK_PATH, (String) null);
        defaultConfig.put(PUSH_HA_LOCK_IMPLEMENTATION, (String) null);
        defaultConfig.put(PUSH_HA_MAX_NODE_FAILURES, 0);
        defaultConfig.put(PUSH_HA_ENABLED, false);
        defaultConfig.put(PUSH_HA_STATE_AUTO_CLEANUP, false);

        defaultConfig.put(MYSQL_USER, "root");
        defaultConfig.put(MYSQL_PASSWORD, "");
        defaultConfig.put(MYSQL_HOST, "localhost");
        defaultConfig.put(MYSQL_PORT, 3306);
        defaultConfig.put(MYSQL_DATABASE, "voldemort");

        defaultConfig.put(TESTING_SLOW_QUEUEING_GET_MS, 0);
        defaultConfig.put(TESTING_SLOW_QUEUEING_GETALL_MS, 0);
        defaultConfig.put(TESTING_SLOW_QUEUEING_GETVERSIONS_MS,0);
        defaultConfig.put(TESTING_SLOW_QUEUEING_PUT_MS, 0);
        defaultConfig.put(TESTING_SLOW_QUEUEING_DELETE_MS, 0);

        defaultConfig.put(TESTING_SLOW_CONCURRENT_GET_MS, 0);
        defaultConfig.put(TESTING_SLOW_CONCURRENT_GETALL_MS,0);
        defaultConfig.put(TESTING_SLOW_CONCURRENT_GETVERSIONS_MS,0);
        defaultConfig.put(TESTING_SLOW_CONCURRENT_PUT_MS, 0);
        defaultConfig.put(TESTING_SLOW_CONCURRENT_DELETE_MS,0);

        defaultConfig.put(MAX_THREADS, 100);

        // Admin client should have less threads but very high buffer size.
        defaultConfig.put(ADMIN_MAX_THREADS, 20);
        defaultConfig.put(ADMIN_STREAMS_BUFFER_SIZE, 10 * 1000 * 1000);
        defaultConfig.put(ADMIN_CLIENT_CONNECTION_TIMEOUT_SEC, 60);
        defaultConfig.put(ADMIN_CLIENT_SOCKET_TIMEOUT_SEC, 24 * 60 * 60);

        defaultConfig.put(STREAM_READ_BYTE_PER_SEC, 10 * 1000 * 1000);
        defaultConfig.put(STREAM_WRITE_BYTE_PER_SEC, 10 * 1000 * 1000);
        defaultConfig.put(USE_MULTI_VERSION_STREAMING_PUTS, true);

        defaultConfig.put(SOCKET_TIMEOUT_MS, 5000);
        defaultConfig.put(SOCKET_BUFFER_SIZE, 64 * 1024);
        defaultConfig.put(SOCKET_KEEPALIVE, false);

        defaultConfig.put(ENABLE_NIO_CONNECTOR, true);
        defaultConfig.put(NIO_CONNECTOR_KEEPALIVE, false);
        defaultConfig.put(NIO_CONNECTOR_SELECTORS, Math.max(8, Runtime.getRuntime().availableProcessors()));
        defaultConfig.put(NIO_ADMIN_CONNECTOR_SELECTORS, Math.max(8, Runtime.getRuntime().availableProcessors()));
        defaultConfig.put(NIO_ADMIN_CONNECTOR_KEEPALIVE, false);
        // a value <= 0 forces the default to be used
        defaultConfig.put(NIO_ACCEPTOR_BACKLOG, 256);
        defaultConfig.put(NIO_SELECTOR_MAX_HEART_BEAT_TIME_MS, TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));

        defaultConfig.put(CLIENT_SELECTORS, 4);
        defaultConfig.put(CLIENT_MAX_CONNECTIONS_PER_NODE, 50);
        defaultConfig.put(CLIENT_CONNECTION_TIMEOUT_MS, 500);
        defaultConfig.put(CLIENT_ROUTING_TIMEOUT_MS, 15000);
        defaultConfig.put(CLIENT_ROUTING_ALLOW_PARTIAL_GETALL, false);
        defaultConfig.put(CLIENT_MAX_THREADS, 500);
        defaultConfig.put(CLIENT_THREAD_IDLE_MS, 100000);
        defaultConfig.put(CLIENT_MAX_QUEUED_REQUESTS, 1000);

        defaultConfig.put(HTTP_ENABLE, false);
        defaultConfig.put(SOCKET_ENABLE, true);
        defaultConfig.put(ADMIN_ENABLE, true);
        defaultConfig.put(JMX_ENABLE, true);
        defaultConfig.put(SLOP_ENABLE, true);
        defaultConfig.put(SLOP_PUSHER_ENABLE, true);
        defaultConfig.put(SLOP_WRITE_BYTE_PER_SEC, 10 * 1000 * 1000);
        defaultConfig.put(ENABLE_VERBOSE_LOGGING, true);
        defaultConfig.put(ENABLE_STAT_TRACKING, true);
        defaultConfig.put(ENABLE_SERVER_ROUTING, false);
        defaultConfig.put(ENABLE_METADATA_CHECKING, true);
        defaultConfig.put(ENABLE_GOSSIP, false);
        defaultConfig.put(ENABLE_REBALANCING, true);
        defaultConfig.put(ENABLE_REPAIR, true);
        defaultConfig.put(ENABLE_PRUNEJOB, true);
        defaultConfig.put(ENABLE_SLOP_PURGE_JOB, true);
        defaultConfig.put(ENABLE_JMX_CLUSTERNAME, false);
        defaultConfig.put(ENABLE_QUOTA_LIMITING, true);

        defaultConfig.put(GOSSIP_INTERVAL_MS, 30 * 1000);

        defaultConfig.put(SLOP_WRITE_BYTE_PER_SEC1, 10 * 1000 * 1000);
        defaultConfig.put(SLOP_READ_BYTE_PER_SEC, 10 * 1000 * 1000);
        defaultConfig.put(SLOP_STORE_ENGINE, BdbStorageConfiguration.TYPE_NAME);
        defaultConfig.put(SLOP_FREQUENCY_MS, 5 * 60 * 1000);
        defaultConfig.put(SLOP_BATCH_SIZE, 100);
        defaultConfig.put(PUSHER_TYPE, StreamingSlopPusherJob.TYPE_NAME);
        defaultConfig.put(SLOP_ZONES_TERMINATE, 0);
        defaultConfig.put(AUTO_PURGE_DEAD_SLOPS, true);

        defaultConfig.put(SCHEDULER_THREADS, 6);
        defaultConfig.put(SERVICE_INTERRUPTIBLE, true);

        defaultConfig.put(NUM_SCAN_PERMITS, 1);

        defaultConfig.put(STORAGE_CONFIGS, ImmutableList.of(BdbStorageConfiguration.class.getName(),
                                                            MysqlStorageConfiguration.class.getName(),
                                                            InMemoryStorageConfiguration.class.getName(),
                                                            CacheStorageConfiguration.class.getName(),
                                                            ReadOnlyStorageConfiguration.class.getName(),
                                                            RocksDbStorageConfiguration.class.getName()));

        // start at midnight (0-23)
        defaultConfig.put(RETENTION_CLEANUP_FIRST_START_HOUR, 0);
        // start next day by default (1=SUN, 2=MON, 3=TUE, 4=WED, 5=THU, 6=FRI, 7=SAT)
        defaultConfig.put(RETENTION_CLEANUP_FIRST_START_DAY, Utils.getDayOfTheWeekFromNow(1));
        // repeat every 24 hours
        defaultConfig.put(RETENTION_CLEANUP_PERIOD_HOURS, 24);
        // should the retention job always start at the 'start time' specified
        defaultConfig.put(RETENTION_CLEANUP_PIN_START_TIME, true);
        // should the online reads filter out stale values when reading them ?
        defaultConfig.put(ENFORCE_RETENTION_POLICY_ON_READ, false);
        // should the online reads issue deletes to clear out stale values when reading them?
        defaultConfig.put(DELETE_EXPIRED_VALUES_ON_READ, false);

        defaultConfig.put(REQUEST_FORMAT, RequestFormatType.VOLDEMORT_V1.getCode());

        // rebalancing parameters
        defaultConfig.put(REBALANCING_TIMEOUT_SECONDS, 10 * 24 * 60 * 60);
        defaultConfig.put(MAX_PARALLEL_STORES_REBALANCING, 3);
        defaultConfig.put(USE_PARTITION_SCAN_FOR_REBALANCE, true);
        defaultConfig.put(MAX_PROXY_PUT_THREADS, Math.max(8, Runtime.getRuntime().availableProcessors()));
        defaultConfig.put(FAILUREDETECTOR_IMPLEMENTATION, FailureDetectorConfig.DEFAULT_IMPLEMENTATION_CLASS_NAME);
        defaultConfig.put(FAILUREDETECTOR_BANNAGE_PERIOD, FailureDetectorConfig.DEFAULT_BANNAGE_PERIOD);
        defaultConfig.put(FAILUREDETECTOR_THRESHOLD, FailureDetectorConfig.DEFAULT_THRESHOLD);
        defaultConfig.put(FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM, FailureDetectorConfig.DEFAULT_THRESHOLD_COUNT_MINIMUM);
        defaultConfig.put(FAILUREDETECTOR_THRESHOLD_INTERVAL, FailureDetectorConfig.DEFAULT_THRESHOLD_INTERVAL);
        defaultConfig.put(FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL, FailureDetectorConfig.DEFAULT_ASYNC_RECOVERY_INTERVAL);
        defaultConfig.put(FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES, FailureDetectorConfig.DEFAULT_CATASTROPHIC_ERROR_TYPES);

        // network class loader disable by default.
        defaultConfig.put(ENABLE_NETWORK_CLASSLOADER, false);

        // TODO: REST-Server decide on the numbers
        defaultConfig.put(REST_ENABLE, false);
        defaultConfig.put(NUM_REST_SERVICE_NETTY_SERVER_BACKLOG, 1000);
        defaultConfig.put(NUM_REST_SERVICE_NETTY_BOSS_THREADS, 1);
        defaultConfig.put(NUM_REST_SERVICE_NETTY_WORKER_THREADS, 20);
        defaultConfig.put(NUM_REST_SERVICE_STORAGE_THREADS, 50);
        defaultConfig.put(MAX_HTTP_AGGREGATED_CONTENT_LENGTH, 1048576);

        defaultConfig.put(REPAIRJOB_MAX_KEYS_SCANNED_PER_SEC, Integer.MAX_VALUE);
        defaultConfig.put(PRUNEJOB_MAX_KEYS_SCANNED_PER_SEC, Integer.MAX_VALUE);
        defaultConfig.put(SLOP_PURGEJOB_MAX_KEYS_SCANNED_PER_SEC, 10000);

        // RocksDB config
        defaultConfig.put(ROCKSDB_PREFIX_KEYS_WITH_PARTITIONID, true);
        defaultConfig.put(ROCKSDB_ENABLE_READ_LOCKS, false);

        defaultConfig.put(RESTRICTED_CONFIGS, Lists.newArrayList(MYSQL_USER,
                                                                 MYSQL_PASSWORD,
                                                                 READONLY_KEYTAB_PATH,
                                                                 READONLY_KERBEROS_USER,
                                                                 READONLY_KERBEROS_KDC,
                                                                 READONLY_KERBEROS_REALM));

        defaultConfig.put(ENABLE_NODE_ID_DETECTION, false);
        defaultConfig.put(VALIDATE_NODE_ID, false);
    }

    public static final int INVALID_NODE_ID = -1;
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

    private boolean enableNodeIdDetection;
    private boolean validateNodeId;
    private List<String> nodeIdHostTypes;
    private HostMatcher nodeIdImplementation;

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
    private String readOnlyModifyProtocol;
    private int readOnlyModifyPort;
    private boolean readOnlyOmitPort;
    private boolean bouncyCastleEnabled;
    private boolean readOnlyBuildPrimaryReplicasOnly;

    private boolean highAvailabilityPushEnabled;
    private String highAvailabilityPushClusterId;
    private String highAvailabilityPushLockPath;
    private String highAvailabilityPushLockImplementation;
    private int highAvailabilityPushMaxNodeFailures;
    private boolean highAvailabilityStateAutoCleanUp;

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

    private List<String> restrictedConfigs;

    public VoldemortConfig(Properties props) {
        this(new Props(props));
    }

    public VoldemortConfig(Props props) {
        // User-supplied configs are always honored first. Then we fall back on dynamically-defined and
        // statically-defined defaults for config keys which are not found in the user-supplied config.
        this.allProps = new Props(props, getDynamicDefaults(props), defaultConfig);
        initializeStateFromProps();
        validateParams();
    }

    /**
     * This is only used by tests. Do not use in regular code...
     *
     * FIXME: Remove this from here or otherwise restrict accessibility to tests only.
     */
    public VoldemortConfig(int nodeId, String voldemortHome) {
        this(new Props().with(NODE_ID, nodeId).with(VOLDEMORT_HOME, voldemortHome));
    }

    private void initializeNodeId(Props combinedConfigs, Props dynamicDefaults) {
        boolean nodeIdExists = combinedConfigs.containsKey(NODE_ID);
        if(!nodeIdExists) {
            try {
                // If node Id is not present in the configs, look for the
                // environment variable and save it in dynamic defaults.
                int nodeId = getIntEnvVariable(VOLDEMORT_NODE_ID_VAR_NAME);
                dynamicDefaults.put(NODE_ID, nodeId);
            }catch(ConfigurationException ex) {
                // missingNodeId is handled in the server startup.
            }
        } else {
            // Make sure it is a valid integer
            combinedConfigs.getInt(NODE_ID);
        }
    }

    /**
     * This function returns a set of default configs which cannot be defined statically,
     * because they (at least potentially) depend on the config values provided by the user.
     */
    private Props getDynamicDefaults(Props userSuppliedConfig) {
        // Combined set of configs made up of user supplied configs first, while falling back
        // on statically defined defaults when the value is missing from the user supplied ones.
        Props combinedConfigs = new Props(userSuppliedConfig, defaultConfig);

        // Set of dynamic configs which depend on the combined configs in order to be determined.
        Props dynamicDefaults = new Props();

        initializeNodeId(combinedConfigs, dynamicDefaults);

        // Define various paths
        String defaultDataDirectory = combinedConfigs.getString(VOLDEMORT_HOME) + File.separator + "data";
        String dataDirectory = combinedConfigs.getString(DATA_DIRECTORY, defaultDataDirectory);
        dynamicDefaults.put(DATA_DIRECTORY, dataDirectory);
        dynamicDefaults.put(BDB_DATA_DIRECTORY, dataDirectory + File.separator + "bdb");
        dynamicDefaults.put(READONLY_DATA_DIRECTORY, dataDirectory + File.separator + "read-only");
        dynamicDefaults.put(ROCKSDB_DATA_DIR, dataDirectory + File.separator + "rocksdb");
        String metadataDirectory = combinedConfigs.getString(VOLDEMORT_HOME) + File.separator + "config";
        dynamicDefaults.put(METADATA_DIRECTORY, metadataDirectory);
        dynamicDefaults.put(READONLY_KEYTAB_PATH, metadataDirectory + File.separator + "voldemrt.headless.keytab");
        dynamicDefaults.put(READONLY_HADOOP_CONFIG_PATH, metadataDirectory + File.separator + "hadoop-conf");

        // Other "transitive" config values.
        dynamicDefaults.put(CORE_THREADS, Math.max(1, combinedConfigs.getInt(MAX_THREADS) / 2));
        dynamicDefaults.put(ADMIN_CORE_THREADS, Math.max(1, combinedConfigs.getInt(ADMIN_MAX_THREADS) / 2));
        dynamicDefaults.put(CLIENT_ROUTING_GET_TIMEOUT_MS, combinedConfigs.getInt(CLIENT_ROUTING_TIMEOUT_MS));
        dynamicDefaults.put(CLIENT_ROUTING_GETALL_TIMEOUT_MS, combinedConfigs.getInt(CLIENT_ROUTING_TIMEOUT_MS));
        int clientRoutingPutTimeoutMs = combinedConfigs.getInt(CLIENT_ROUTING_TIMEOUT_MS);
        dynamicDefaults.put(CLIENT_ROUTING_PUT_TIMEOUT_MS, clientRoutingPutTimeoutMs);
        dynamicDefaults.put(CLIENT_ROUTING_GETVERSIONS_TIMEOUT_MS, combinedConfigs.getInt(CLIENT_ROUTING_PUT_TIMEOUT_MS, clientRoutingPutTimeoutMs));
        dynamicDefaults.put(CLIENT_ROUTING_DELETE_TIMEOUT_MS, combinedConfigs.getInt(CLIENT_ROUTING_TIMEOUT_MS));
        dynamicDefaults.put(FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD, combinedConfigs.getInt(SOCKET_TIMEOUT_MS));
        dynamicDefaults.put(REST_SERVICE_STORAGE_THREAD_POOL_QUEUE_SIZE, combinedConfigs.getInt(NUM_REST_SERVICE_STORAGE_THREADS));

        // We're changing the property from "client.node.bannage.ms" to
        // "failuredetector.bannage.period" so if we have the old one, migrate it over.
        if(userSuppliedConfig.containsKey(CLIENT_NODE_BANNAGE_MS)
                && !userSuppliedConfig.containsKey(FAILUREDETECTOR_BANNAGE_PERIOD)) {
            dynamicDefaults.put(FAILUREDETECTOR_BANNAGE_PERIOD, userSuppliedConfig.getInt(CLIENT_NODE_BANNAGE_MS));
        }

        return dynamicDefaults;
    }

    /**
     * This function populates the various strongly-typed variables of this class by
     * extracting the values from {@link VoldemortConfig#allProps}.
     *
     * At this point, all defaults should have been resolved properly, so we can assume
     * that all properties are present. If that's not the case, the correct behavior is
     * to bubble up an UndefinedPropertyException.
     *
     * This code is isolated into its own function to prevent future code from trying to
     * extract configurations from anywhere else besides {@link VoldemortConfig#allProps}.
     *
     * @throws UndefinedPropertyException if any required property has not been set.
     */
    private void initializeStateFromProps() throws UndefinedPropertyException {
        this.nodeId = this.allProps.getInt(NODE_ID, INVALID_NODE_ID);
        this.voldemortHome = this.allProps.getString(VOLDEMORT_HOME);
        this.dataDirectory = this.allProps.getString(DATA_DIRECTORY);
        this.metadataDirectory = this.allProps.getString(METADATA_DIRECTORY);

        this.bdbCacheSize = this.allProps.getBytes(BDB_CACHE_SIZE);
        this.bdbWriteTransactions = this.allProps.getBoolean(BDB_WRITE_TRANSACTIONS);
        this.bdbFlushTransactions = this.allProps.getBoolean(BDB_FLUSH_TRANSACTIONS);
        this.bdbDataDirectory = this.allProps.getString(BDB_DATA_DIRECTORY);
        this.bdbMaxLogFileSize = this.allProps.getBytes(BDB_MAX_LOGFILE_SIZE);
        this.bdbBtreeFanout = this.allProps.getInt(BDB_BTREE_FANOUT);
        this.bdbMaxDelta = this.allProps.getInt(BDB_MAX_DELTA);
        this.bdbBinDelta = this.allProps.getInt(BDB_BIN_DELTA);
        this.bdbCheckpointBytes = this.allProps.getLong(BDB_CHECKPOINT_INTERVAL_BYTES);
        this.bdbCheckpointMs = this.allProps.getLong(BDB_CHECKPOINT_INTERVAL_MS);
        this.bdbOneEnvPerStore = this.allProps.getBoolean(BDB_ONE_ENV_PER_STORE);
        this.bdbCleanerMinFileUtilization = this.allProps.getInt(BDB_CLEANER_MIN_FILE_UTILIZATION);
        this.bdbCleanerMinUtilization = this.allProps.getInt(BDB_CLEANER_MIN_UTILIZATION);
        this.bdbCleanerThreads = this.allProps.getInt(BDB_CLEANER_THREADS);
        this.bdbCleanerBytesInterval = this.allProps.getLong(BDB_CLEANER_INTERVAL_BYTES);
        this.bdbCleanerLookAheadCacheSize = this.allProps.getInt(BDB_CLEANER_LOOKAHEAD_CACHE_SIZE);
        this.bdbLockTimeoutMs = this.allProps.getLong(BDB_LOCK_TIMEOUT_MS);
        this.bdbLockNLockTables = this.allProps.getInt(BDB_LOCK_N_LOCK_TABLES);
        this.bdbLogFaultReadSize = this.allProps.getInt(BDB_LOG_FAULT_READ_SIZE);
        this.bdbLogIteratorReadSize = this.allProps.getInt(BDB_LOG_ITERATOR_READ_SIZE);
        this.bdbFairLatches = this.allProps.getBoolean(BDB_FAIR_LATCHES);
        this.bdbCheckpointerHighPriority = this.allProps.getBoolean(BDB_CHECKPOINTER_HIGH_PRIORITY);
        this.bdbCleanerMaxBatchFiles = this.allProps.getInt(BDB_CLEANER_MAX_BATCH_FILES);
        this.bdbReadUncommitted = this.allProps.getBoolean(BDB_LOCK_READ_UNCOMMITTED);
        this.bdbStatsCacheTtlMs = this.allProps.getLong(BDB_STATS_CACHE_TTL_MS);
        this.bdbExposeSpaceUtilization = this.allProps.getBoolean(BDB_EXPOSE_SPACE_UTILIZATION);
        this.bdbMinimumSharedCache = this.allProps.getLong(BDB_MINIMUM_SHARED_CACHE);
        this.bdbCleanerLazyMigration = this.allProps.getBoolean(BDB_CLEANER_LAZY_MIGRATION);
        this.bdbCacheModeEvictLN = this.allProps.getBoolean(BDB_CACHE_EVICTLN);
        this.bdbMinimizeScanImpact = this.allProps.getBoolean(BDB_MINIMIZE_SCAN_IMPACT);
        this.bdbPrefixKeysWithPartitionId = this.allProps.getBoolean(BDB_PREFIX_KEYS_WITH_PARTITIONID);
        this.bdbLevelBasedEviction = this.allProps.getBoolean(BDB_EVICT_BY_LEVEL);
        this.bdbCheckpointerOffForBatchWrites = this.allProps.getBoolean(BDB_CHECKPOINTER_OFF_BATCH_WRITES);
        this.bdbCleanerFetchObsoleteSize = this.allProps.getBoolean(BDB_CLEANER_FETCH_OBSOLETE_SIZE);
        this.bdbCleanerAdjustUtilization = this.allProps.getBoolean(BDB_CLEANER_ADJUST_UTILIZATION);
        this.bdbRecoveryForceCheckpoint = this.allProps.getBoolean(BDB_RECOVERY_FORCE_CHECKPOINT);
        this.bdbRawPropertyString = this.allProps.getString(BDB_RAW_PROPERTY_STRING);

        this.numReadOnlyVersions = this.allProps.getInt(READONLY_BACKUPS);
        this.readOnlySearchStrategy = this.allProps.getString(READONLY_SEARCH_STRATEGY);
        this.readOnlyStorageDir = this.allProps.getString(READONLY_DATA_DIRECTORY);
        this.readOnlyDeleteBackupTimeMs = this.allProps.getInt(READONLY_DELETE_BACKUP_MS);
        this.readOnlyFetcherMaxBytesPerSecond = this.allProps.getBytes(FETCHER_MAX_BYTES_PER_SEC);
        this.readOnlyFetcherReportingIntervalBytes = this.allProps.getBytes(FETCHER_REPORTING_INTERVAL_BYTES);
        this.readOnlyFetcherThrottlerInterval = this.allProps.getInt(FETCHER_THROTTLER_INTERVAL);
        this.readOnlyFetchRetryCount = this.allProps.getInt(FETCHER_RETRY_COUNT);
        this.readOnlyFetchRetryDelayMs = this.allProps.getLong(FETCHER_RETRY_DELAY_MS);
        this.readOnlyLoginIntervalMs = this.allProps.getLong(FETCHER_LOGIN_INTERVAL_MS);
        this.defaultStorageSpaceQuotaInKB = this.allProps.getLong(DEFAULT_STORAGE_SPACE_QUOTA_IN_KB);
        this.fetcherBufferSize = (int) this.allProps.getBytes(HDFS_FETCHER_BUFFER_SIZE);
        this.fetcherSocketTimeout = this.allProps.getInt(HDFS_FETCHER_SOCKET_TIMEOUT);
        this.readOnlyKeytabPath = this.allProps.getString(READONLY_KEYTAB_PATH);
        this.readOnlyKerberosUser = this.allProps.getString(READONLY_KERBEROS_USER);
        this.hadoopConfigPath = this.allProps.getString(READONLY_HADOOP_CONFIG_PATH);
        this.readOnlyKerberosKdc = this.allProps.getString(READONLY_KERBEROS_KDC);
        this.readOnlykerberosRealm = this.allProps.getString(READONLY_KERBEROS_REALM);
        this.fileFetcherClass = this.allProps.getString(FILE_FETCHER_CLASS);
        this.readOnlyStatsFileEnabled = this.allProps.getBoolean(READONLY_STATS_FILE_ENABLED);
        this.readOnlyMaxVersionsStatsFile = this.allProps.getInt(READONLY_STATS_FILE_MAX_VERSIONS);
        this.readOnlyMaxValueBufferAllocationSize = this.allProps.getInt(READONLY_MAX_VALUE_BUFFER_ALLOCATION_SIZE);
        this.readOnlyCompressionCodec = this.allProps.getString(READONLY_COMPRESSION_CODEC);
        this.readOnlyModifyProtocol = this.allProps.getString(READONLY_MODIFY_PROTOCOL);
        this.readOnlyModifyPort = this.allProps.getInt(READONLY_MODIFY_PORT);
        this.readOnlyOmitPort = this.allProps.getBoolean(READONLY_OMIT_PORT);
        this.bouncyCastleEnabled = this.allProps.getBoolean(USE_BOUNCYCASTLE_FOR_SSL);
        this.readOnlyBuildPrimaryReplicasOnly = this.allProps.getBoolean(READONLY_BUILD_PRIMARY_REPLICAS_ONLY);

        this.highAvailabilityPushClusterId = this.allProps.getString(PUSH_HA_CLUSTER_ID);
        this.highAvailabilityPushLockPath = this.allProps.getString(PUSH_HA_LOCK_PATH);
        this.highAvailabilityPushLockImplementation = this.allProps.getString(PUSH_HA_LOCK_IMPLEMENTATION);
        this.highAvailabilityPushMaxNodeFailures = this.allProps.getInt(PUSH_HA_MAX_NODE_FAILURES);
        this.highAvailabilityPushEnabled = this.allProps.getBoolean(PUSH_HA_ENABLED);
        this.highAvailabilityStateAutoCleanUp = this.allProps.getBoolean(PUSH_HA_STATE_AUTO_CLEANUP);

        this.mysqlUsername = this.allProps.getString(MYSQL_USER);
        this.mysqlPassword = this.allProps.getString(MYSQL_PASSWORD);
        this.mysqlHost = this.allProps.getString(MYSQL_HOST);
        this.mysqlPort = this.allProps.getInt(MYSQL_PORT);
        this.mysqlDatabaseName = this.allProps.getString(MYSQL_DATABASE);

        this.testingSlowQueueingDelays = new OpTimeMap(0);
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.GET_OP_CODE, this.allProps.getInt(TESTING_SLOW_QUEUEING_GET_MS));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.GET_ALL_OP_CODE, this.allProps.getInt(TESTING_SLOW_QUEUEING_GETALL_MS));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.GET_VERSION_OP_CODE, this.allProps.getInt(TESTING_SLOW_QUEUEING_GETVERSIONS_MS));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.PUT_OP_CODE, this.allProps.getInt(TESTING_SLOW_QUEUEING_PUT_MS));
        this.testingSlowQueueingDelays.setOpTime(VoldemortOpCode.DELETE_OP_CODE, this.allProps.getInt(TESTING_SLOW_QUEUEING_DELETE_MS));

        this.testingSlowConcurrentDelays = new OpTimeMap(0);
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.GET_OP_CODE, this.allProps.getInt(TESTING_SLOW_CONCURRENT_GET_MS));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.GET_ALL_OP_CODE, this.allProps.getInt(TESTING_SLOW_CONCURRENT_GETALL_MS));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.GET_VERSION_OP_CODE, this.allProps.getInt(TESTING_SLOW_CONCURRENT_GETVERSIONS_MS));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.PUT_OP_CODE, this.allProps.getInt(TESTING_SLOW_CONCURRENT_PUT_MS));
        this.testingSlowConcurrentDelays.setOpTime(VoldemortOpCode.DELETE_OP_CODE, this.allProps.getInt(TESTING_SLOW_CONCURRENT_DELETE_MS));

        this.maxThreads = this.allProps.getInt(MAX_THREADS);
        this.coreThreads = this.allProps.getInt(CORE_THREADS);

        // Admin client should have less threads but very high buffer size.
        this.adminMaxThreads = this.allProps.getInt(ADMIN_MAX_THREADS);
        this.adminCoreThreads = this.allProps.getInt(ADMIN_CORE_THREADS);
        this.adminStreamBufferSize = (int) this.allProps.getBytes(ADMIN_STREAMS_BUFFER_SIZE);
        this.adminConnectionTimeout = this.allProps.getInt(ADMIN_CLIENT_CONNECTION_TIMEOUT_SEC);
        this.adminSocketTimeout = this.allProps.getInt(ADMIN_CLIENT_SOCKET_TIMEOUT_SEC);

        this.streamMaxReadBytesPerSec = this.allProps.getBytes(STREAM_READ_BYTE_PER_SEC);
        this.streamMaxWriteBytesPerSec = this.allProps.getBytes(STREAM_WRITE_BYTE_PER_SEC);
        this.multiVersionStreamingPutsEnabled = this.allProps.getBoolean(USE_MULTI_VERSION_STREAMING_PUTS);

        this.socketTimeoutMs = this.allProps.getInt(SOCKET_TIMEOUT_MS);
        this.socketBufferSize = (int) this.allProps.getBytes(SOCKET_BUFFER_SIZE);
        this.socketKeepAlive = this.allProps.getBoolean(SOCKET_KEEPALIVE);

        this.useNioConnector = this.allProps.getBoolean(ENABLE_NIO_CONNECTOR);
        this.nioConnectorKeepAlive = this.allProps.getBoolean(NIO_CONNECTOR_KEEPALIVE);
        this.nioConnectorSelectors = this.allProps.getInt(NIO_CONNECTOR_SELECTORS);
        this.nioAdminConnectorSelectors = this.allProps.getInt(NIO_ADMIN_CONNECTOR_SELECTORS);
        this.nioAdminConnectorKeepAlive = this.allProps.getBoolean(NIO_ADMIN_CONNECTOR_KEEPALIVE);
        // a value <= 0 forces the default to be used
        this.nioAcceptorBacklog = this.allProps.getInt(NIO_ACCEPTOR_BACKLOG);
        this.nioSelectorMaxHeartBeatTimeMs = this.allProps.getLong(NIO_SELECTOR_MAX_HEART_BEAT_TIME_MS);

        this.clientSelectors = this.allProps.getInt(CLIENT_SELECTORS);
        this.clientMaxConnectionsPerNode = this.allProps.getInt(CLIENT_MAX_CONNECTIONS_PER_NODE);
        this.clientConnectionTimeoutMs = this.allProps.getInt(CLIENT_CONNECTION_TIMEOUT_MS);
        this.clientRoutingTimeoutMs = this.allProps.getInt(CLIENT_ROUTING_TIMEOUT_MS);
        this.clientTimeoutConfig = new TimeoutConfig(this.clientRoutingTimeoutMs, false);
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.GET_OP_CODE, this.allProps.getInt(CLIENT_ROUTING_GET_TIMEOUT_MS));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE, this.allProps.getInt(CLIENT_ROUTING_GETALL_TIMEOUT_MS));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.PUT_OP_CODE, this.allProps.getInt(CLIENT_ROUTING_PUT_TIMEOUT_MS));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE, this.allProps.getLong(CLIENT_ROUTING_GETVERSIONS_TIMEOUT_MS));
        this.clientTimeoutConfig.setOperationTimeout(VoldemortOpCode.DELETE_OP_CODE, this.allProps.getInt(CLIENT_ROUTING_DELETE_TIMEOUT_MS));
        this.clientTimeoutConfig.setPartialGetAllAllowed(this.allProps.getBoolean(CLIENT_ROUTING_ALLOW_PARTIAL_GETALL));
        this.clientMaxThreads = this.allProps.getInt(CLIENT_MAX_THREADS);
        this.clientThreadIdleMs = this.allProps.getInt(CLIENT_THREAD_IDLE_MS);
        this.clientMaxQueuedRequests = this.allProps.getInt(CLIENT_MAX_QUEUED_REQUESTS);

        this.enableHttpServer = this.allProps.getBoolean(HTTP_ENABLE);
        this.enableSocketServer = this.allProps.getBoolean(SOCKET_ENABLE);
        this.enableAdminServer = this.allProps.getBoolean(ADMIN_ENABLE);
        this.enableJmx = this.allProps.getBoolean(JMX_ENABLE);
        this.enableSlop = this.allProps.getBoolean(SLOP_ENABLE);
        this.enableSlopPusherJob = this.allProps.getBoolean(SLOP_PUSHER_ENABLE);
        this.slopMaxWriteBytesPerSec = this.allProps.getBytes(SLOP_WRITE_BYTE_PER_SEC);
        this.enableVerboseLogging = this.allProps.getBoolean(ENABLE_VERBOSE_LOGGING);
        this.enableStatTracking = this.allProps.getBoolean(ENABLE_STAT_TRACKING);
        this.enableServerRouting = this.allProps.getBoolean(ENABLE_SERVER_ROUTING);
        this.enableMetadataChecking = this.allProps.getBoolean(ENABLE_METADATA_CHECKING);
        this.enableGossip = this.allProps.getBoolean(ENABLE_GOSSIP);
        this.enableRebalanceService = this.allProps.getBoolean(ENABLE_REBALANCING);
        this.enableRepair = this.allProps.getBoolean(ENABLE_REPAIR);
        this.enablePruneJob = this.allProps.getBoolean(ENABLE_PRUNEJOB);
        this.enableSlopPurgeJob = this.allProps.getBoolean(ENABLE_SLOP_PURGE_JOB);
        this.enableJmxClusterName = this.allProps.getBoolean(ENABLE_JMX_CLUSTERNAME);
        this.enableQuotaLimiting = this.allProps.getBoolean(ENABLE_QUOTA_LIMITING);

        this.gossipIntervalMs = this.allProps.getInt(GOSSIP_INTERVAL_MS);

        this.slopMaxWriteBytesPerSec = this.allProps.getBytes(SLOP_WRITE_BYTE_PER_SEC1);
        this.slopMaxReadBytesPerSec = this.allProps.getBytes(SLOP_READ_BYTE_PER_SEC);
        this.slopStoreType = this.allProps.getString(SLOP_STORE_ENGINE);
        this.slopFrequencyMs = this.allProps.getLong(SLOP_FREQUENCY_MS);
        this.slopBatchSize = this.allProps.getInt(SLOP_BATCH_SIZE);
        this.pusherType = this.allProps.getString(PUSHER_TYPE);
        this.slopZonesDownToTerminate = this.allProps.getInt(SLOP_ZONES_TERMINATE);
        this.autoPurgeDeadSlops = this.allProps.getBoolean(AUTO_PURGE_DEAD_SLOPS);

        this.schedulerThreads = this.allProps.getInt(SCHEDULER_THREADS);
        this.mayInterruptService = this.allProps.getBoolean(SERVICE_INTERRUPTIBLE);

        this.numScanPermits = this.allProps.getInt(NUM_SCAN_PERMITS);

        this.storageConfigurations = this.allProps.getList(STORAGE_CONFIGS);

        this.retentionCleanupFirstStartTimeInHour = this.allProps.getInt(RETENTION_CLEANUP_FIRST_START_HOUR);
        this.retentionCleanupFirstStartDayOfWeek = this.allProps.getInt(RETENTION_CLEANUP_FIRST_START_DAY);
        this.retentionCleanupScheduledPeriodInHour = this.allProps.getInt(RETENTION_CLEANUP_PERIOD_HOURS);
        this.retentionCleanupPinStartTime = this.allProps.getBoolean(RETENTION_CLEANUP_PIN_START_TIME);
        this.enforceRetentionPolicyOnRead = this.allProps.getBoolean(ENFORCE_RETENTION_POLICY_ON_READ);
        this.deleteExpiredValuesOnRead = this.allProps.getBoolean(DELETE_EXPIRED_VALUES_ON_READ);

        this.requestFormatType = RequestFormatType.fromCode(this.allProps.getString(REQUEST_FORMAT));

        // rebalancing parameters
        this.rebalancingTimeoutSec = this.allProps.getLong(REBALANCING_TIMEOUT_SECONDS);
        this.maxParallelStoresRebalancing = this.allProps.getInt(MAX_PARALLEL_STORES_REBALANCING);
        this.usePartitionScanForRebalance = this.allProps.getBoolean(USE_PARTITION_SCAN_FOR_REBALANCE);
        this.maxProxyPutThreads = this.allProps.getInt(MAX_PROXY_PUT_THREADS);
        this.failureDetectorImplementation = this.allProps.getString(FAILUREDETECTOR_IMPLEMENTATION);

        this.failureDetectorBannagePeriod = this.allProps.getLong(FAILUREDETECTOR_BANNAGE_PERIOD);
        this.failureDetectorThreshold = this.allProps.getInt(FAILUREDETECTOR_THRESHOLD);
        this.failureDetectorThresholdCountMinimum = this.allProps.getInt(FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM);
        this.failureDetectorThresholdInterval = this.allProps.getLong(FAILUREDETECTOR_THRESHOLD_INTERVAL);
        this.failureDetectorAsyncRecoveryInterval = this.allProps.getLong(FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL);
        this.failureDetectorCatastrophicErrorTypes = this.allProps.getList(FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES);
        this.failureDetectorRequestLengthThreshold = this.allProps.getLong(FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD);

        // network class loader disable by default.
        this.enableNetworkClassLoader = this.allProps.getBoolean(ENABLE_NETWORK_CLASSLOADER);

        // TODO: REST-Server decide on the numbers
        this.enableRestService = this.allProps.getBoolean(REST_ENABLE);
        this.numRestServiceNettyServerBacklog = this.allProps.getInt(NUM_REST_SERVICE_NETTY_SERVER_BACKLOG);
        this.numRestServiceNettyBossThreads = this.allProps.getInt(NUM_REST_SERVICE_NETTY_BOSS_THREADS);
        this.numRestServiceNettyWorkerThreads = this.allProps.getInt(NUM_REST_SERVICE_NETTY_WORKER_THREADS);
        this.numRestServiceStorageThreads = this.allProps.getInt(NUM_REST_SERVICE_STORAGE_THREADS);
        this.restServiceStorageThreadPoolQueueSize = this.allProps.getInt(REST_SERVICE_STORAGE_THREAD_POOL_QUEUE_SIZE);
        this.maxHttpAggregatedContentLength = this.allProps.getInt(MAX_HTTP_AGGREGATED_CONTENT_LENGTH);

        this.repairJobMaxKeysScannedPerSec = this.allProps.getInt(REPAIRJOB_MAX_KEYS_SCANNED_PER_SEC);
        this.pruneJobMaxKeysScannedPerSec = this.allProps.getInt(PRUNEJOB_MAX_KEYS_SCANNED_PER_SEC);
        this.slopPurgeJobMaxKeysScannedPerSec = this.allProps.getInt(SLOP_PURGEJOB_MAX_KEYS_SCANNED_PER_SEC);

        // RocksDB config
        this.rocksdbDataDirectory = this.allProps.getString(ROCKSDB_DATA_DIR);
        this.rocksdbPrefixKeysWithPartitionId = this.allProps.getBoolean(ROCKSDB_PREFIX_KEYS_WITH_PARTITIONID);
        this.rocksdbEnableReadLocks = this.allProps.getBoolean(ROCKSDB_ENABLE_READ_LOCKS);

        this.restrictedConfigs = this.allProps.getList(RESTRICTED_CONFIGS);
        // Node Id auto detection configs
        this.enableNodeIdDetection = this.allProps.getBoolean(ENABLE_NODE_ID_DETECTION, false);
        // validation is defaulted based on node id detection.
        this.validateNodeId = this.allProps.getBoolean(VALIDATE_NODE_ID);
        this.nodeIdImplementation = new HostMatcher();
    }

    private void validateParams() {
        if(coreThreads < 0)
            throw new IllegalArgumentException(CORE_THREADS + " cannot be less than 1");
        else if(coreThreads > maxThreads)
            throw new IllegalArgumentException(CORE_THREADS + " cannot be greater than " + MAX_THREADS);
        if(maxThreads < 1)
            throw new ConfigurationException(MAX_THREADS + " cannot be less than 1.");
        if(slopFrequencyMs < 1)
            throw new ConfigurationException(SLOP_FREQUENCY_MS + " cannot be less than 1.");
        if(socketTimeoutMs < 0)
            throw new ConfigurationException(SOCKET_TIMEOUT_MS + " must be 0 or more ms.");
        if(clientSelectors < 1)
            throw new ConfigurationException(CLIENT_SELECTORS + " must be 1 or more.");
        if(clientRoutingTimeoutMs < 0)
            throw new ConfigurationException(CLIENT_ROUTING_TIMEOUT_MS + " must be 0 or more ms.");
        if(schedulerThreads < 1)
            throw new ConfigurationException("Must have at least 1 scheduler thread, " + this.schedulerThreads + " set.");
        if(enableServerRouting && !enableSocketServer)
            throw new ConfigurationException("Server-side routing is enabled, this requires the socket server to also be enabled.");
        if(numRestServiceNettyBossThreads < 1)
            throw new ConfigurationException(NUM_REST_SERVICE_NETTY_BOSS_THREADS + " cannot be less than 1");
        if(numRestServiceNettyWorkerThreads < 1)
            throw new ConfigurationException(NUM_REST_SERVICE_NETTY_WORKER_THREADS + " cannot be less than 1");
        if(numRestServiceStorageThreads < 1)
            throw new ConfigurationException(NUM_REST_SERVICE_STORAGE_THREADS + " cannot be less than 1");
        if(numRestServiceNettyServerBacklog < 0)
            throw new ConfigurationException(NUM_REST_SERVICE_NETTY_SERVER_BACKLOG + " cannot be negative");
        if(restServiceStorageThreadPoolQueueSize < 0)
            throw new ConfigurationException(REST_SERVICE_STORAGE_THREAD_POOL_QUEUE_SIZE + " cannot be negative.");
        if(maxHttpAggregatedContentLength <= 0)
            throw new ConfigurationException(MAX_HTTP_AGGREGATED_CONTENT_LENGTH + " must be positive");
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
            properties.put(VOLDEMORT_HOME, voldemortHome);
            properties.put(METADATA_DIRECTORY, voldemortConfigDir);
        } catch(IOException e) {
            throw new ConfigurationException(e);
        }

        return new VoldemortConfig(properties);
    }

    /**
     * This is a generic function for retrieving any config value. The returned value
     * is the one the server is operating with, no matter whether it comes from defaults
     * or from the user-supplied configuration.
     *
     * This function only provides access to configs which are deemed safe to share
     * publicly (i.e.: not security-related configs). The list of configs which are
     * considered off-limit can itself be configured via '{@value #RESTRICTED_CONFIGS}'.
     *
     * @param key config key for which to retrieve the value.
     * @return the value for the requested config key, in String format.
     *         May return null if the key exists and its value is explicitly set to null.
     * @throws UndefinedPropertyException if the requested key does not exist in the config.
     * @throws ConfigurationException if the requested key is not publicly available.
     */
    public String getPublicConfigValue(String key) throws ConfigurationException {
        if (!allProps.containsKey(key)) {
            throw new UndefinedPropertyException("The requested config key does not exist.");
        }
        if (restrictedConfigs.contains(key)) {
            throw new ConfigurationException("The requested config key is not publicly available!");
        }
        return allProps.get(key);
    }

    public int getNodeId() {
        return nodeId;
    }

    /**
     * Id of the server within the cluster. The server matches up this id with
     * the information in cluster.xml to determine what partitions belong to it
     * 
     * <ul>
     * <li>Property : "{@value #NODE_ID}"</li>
     * <li>Default : {@value #VOLDEMORT_NODE_ID_VAR_NAME} env variable</li>
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
     * <li>Property : "{@value #VOLDEMORT_HOME}"</li>
     * <li>Default : {@value #VOLDEMORT_HOME_VAR_NAME} environment variable</li>
     * </ul>
     */
    public void setVoldemortHome(String voldemortHome) {
        this.voldemortHome = voldemortHome;
    }

    public String getDataDirectory() {
        return dataDirectory;
    }

    /**
     * Root directory for Voldemort's data
     * 
     * <ul>
     * <li>Property : "{@value #DATA_DIRECTORY}"</li>
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
     * Root directory for Voldemort's configuration
     * 
     * <ul>
     * <li>Property : "{@value #METADATA_DIRECTORY}"</li>
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
     * <li>Property : "{@value #BDB_CACHE_SIZE}"</li>
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
     * <li>Property : "{@value #BDB_EXPOSE_SPACE_UTILIZATION}"</li>
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
     * <li>Property : "{@value #BDB_FLUSH_TRANSACTIONS}"</li>
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
     * <li>Property : "{@value #BDB_DATA_DIRECTORY}"</li>
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
     * <li>Property : "{@value #BDB_RAW_PROPERTY_STRING}"</li>
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
     * <li>Property : "{@value #BDB_MAX_LOGFILE_SIZE}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_MIN_FILE_UTILIZATION}"</li>
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
     * <li>Property : "{@value #BDB_CHECKPOINTER_HIGH_PRIORITY}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_MAX_BATCH_FILES}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_THREADS}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_INTERVAL_BYTES}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_LOOKAHEAD_CACHE_SIZE}"</li>
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
     * <li>Property : "{@value #BDB_LOCK_TIMEOUT_MS}"</li>
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
     * <li>Property : "{@value #BDB_LOCK_N_LOCK_TABLES}"</li>
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
     * <li>Property : "{@value #BDB_LOG_FAULT_READ_SIZE}"</li>
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
     * <li>Property : "{@value #BDB_LOG_ITERATOR_READ_SIZE}"</li>
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
     * <li>Property : "{@value #BDB_FAIR_LATCHES}"</li>
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
     * <li>Property : "{@value #BDB_LOCK_READ_UNCOMMITTED}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_MIN_UTILIZATION}"</li>
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
     * <li>Property : "{@value #BDB_BTREE_FANOUT}"</li>
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
     * <li>Property : "{@value #BDB_MAX_DELTA}"</li>
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
     * <li>Property : "{@value #BDB_BIN_DELTA}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_FETCH_OBSOLETE_SIZE}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_ADJUST_UTILIZATION}"</li>
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
     * <li>Property : "{@value #BDB_RECOVERY_FORCE_CHECKPOINT}"</li>
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
     * <li>Property : "{@value #BDB_CLEANER_LAZY_MIGRATION}"</li>
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
     * <li>Property : "{@value #BDB_CACHE_EVICTLN}"</li>
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
     * <li>Property : "{@value #BDB_MINIMIZE_SCAN_IMPACT}"</li>
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
     * <li>Property : "{@value #BDB_WRITE_TRANSACTIONS}"</li>
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
     * <li>Property : "{@value #BDB_ONE_ENV_PER_STORE}"</li>
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
     * <li>Property : "{@value #BDB_PREFIX_KEYS_WITH_PARTITIONID}"</li>
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
     * <li>Property : "{@value #BDB_CHECKPOINT_INTERVAL_BYTES}"</li>
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
     * <li>Property : "{@value #BDB_CHECKPOINTER_OFF_BATCH_WRITES}"</li>
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
     * <li>Property : "{@value #BDB_CHECKPOINT_INTERVAL_MS}"</li>
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
     * <li>Property : "{@value #BDB_STATS_CACHE_TTL_MS}"</li>
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
     * <li>Property : "{@value #{@VALUE #BDB_MINIMUM_SHARED_CACHE}}"</li>
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
     * <li>Property : "{@value #BDB_EVICT_BY_LEVEL}"</li>
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
     * <li>Property : "{@value #CORE_THREADS}"</li>
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
     * <li>Property : "{@value #MAX_THREADS}"</li>
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
     * <li>Property : "{@value #ADMIN_CORE_THREADS}"</li>
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
     * <li>Property : "{@value #ADMIN_MAX_THREADS}"</li>
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
     * <li>Property : "{@value #ENABLE_NIO_CONNECTOR}"</li>
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
     * <li>Property : "{@value #NIO_CONNECTOR_SELECTORS}"</li>
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
     * <li>Property : "{@value #NIO_ADMIN_CONNECTOR_SELECTORS}"</li>
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
     * <li>Property : "{@value #HTTP_ENABLE}"</li>
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
     * <li>Property : "{@value #SOCKET_ENABLE}"</li>
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
     * <li>Property : "{@value #ADMIN_ENABLE}"</li>
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
     * <li>Property : "{@value #STREAM_READ_BYTE_PER_SEC}"</li>
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
     * <li>Property : "{@value #STREAM_WRITE_BYTE_PER_SEC}"</li>
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
     * <li>Property : "{@value #USE_MULTI_VERSION_STREAMING_PUTS}"</li>
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
     * <li>Property : "{@value #SLOP_WRITE_BYTE_PER_SEC}"</li>
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
     * <li>Property : "{@value #SLOP_READ_BYTE_PER_SEC}"</li>
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
     * <li>Property : "{@value #JMX_ENABLE}"</li>
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
     * <li>Property : "{@value #MYSQL_USER}"</li>
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
     * <li>Property : "{@value #MYSQL_PASSWORD}"</li>
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
     * <li>Property : "{@value #MYSQL_HOST}"</li>
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
     * <li>Property : "{@value #MYSQL_PORT}"</li>
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
     * <li>Property : "{@value #SLOP_STORE_ENGINE}"</li>
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
     * <li>Property : "{@value #PUSHER_TYPE}"</li>
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
     * <li>Property : "{@value #SLOP_ZONES_TERMINATE}"</li>
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
     * <li>Property : "{@value #AUTO_PURGE_DEAD_SLOPS}"</li>
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
     * <li>Property : "{@value #SLOP_BATCH_SIZE}"</li>
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
     * <li>Property : "{@value #SLOP_FREQUENCY_MS}"</li>
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
     * <li>Property : "{@value #SOCKET_TIMEOUT_MS}"</li>
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
     * <li>Property : "{@value #CLIENT_SELECTORS}"</li>
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
     * <li>Property : "{@value #CLIENT_ROUTING_TIMEOUT_MS}"</li>
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
     * <li>Property : "{@value #CLIENT_MAX_CONNECTIONS_PER_NODE}"</li>
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
     * <li>Property : "{@value #CLIENT_CONNECTION_TIMEOUT_MS}"</li>
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
     * <li>Property : "{@value #CLIENT_MAX_THREADS}"</li>
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
     * <li>Property : "{@value #CLIENT_THREAD_IDLE_MS}"</li>
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
     * <li>Property : "{@value #SLOP_ENABLE}"</li>
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
     * <li>Property : "{@value #SLOP_PUSHER_ENABLE}"</li>
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
     * <li>Property : "{@value #ENABLE_REPAIR}"</li>
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
     * <li>Property : "{@value #ENABLE_PRUNEJOB}"</li>
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
     * <li>Property : "{@value #ENABLE_SLOP_PURGE_JOB}"</li>
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
     * <li>Property : "{@value #ENABLE_VERBOSE_LOGGING}"</li>
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
     * <li>Property : "{@value #ENABLE_STAT_TRACKING}"</li>
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
     * <li>Property : "{@value #ENABLE_METADATA_CHECKING}"</li>
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
     * <li>Property : "{@value #CLIENT_MAX_QUEUED_REQUESTS}"</li>
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
     * <li>Property : "{@value #SERVICE_INTERRUPTIBLE}"</li>
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
     * <li>Property : "{@value #READONLY_DATA_DIRECTORY}"</li>
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
     * <li>Property : "{@value #READONLY_BACKUPS}"</li>
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
     * <li>Property : "{@value #READONLY_DELETE_BACKUP_MS}"</li>
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
     * <li>Property : "{@value #READONLY_KEYTAB_PATH}"</li>
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
     * <li>Property : "{@value #READONLY_KERBEROS_USER}"</li>
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
     * <li>Property : "{@value #READONLY_HADOOP_CONFIG_PATH}"</li>
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
     * <li>Property : "{@value #SOCKET_BUFFER_SIZE}"</li>
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
     * <li>Property : "{@value #SOCKET_KEEPALIVE}"</li>
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
     * <li>Property : "{@value #NIO_ACCEPTOR_BACKLOG}"</li>
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
     * <li>Property : "{@value #ADMIN_STREAMS_BUFFER_SIZE}"</li>
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
     * <li>Property : "{@value #STORAGE_CONFIGS}"</li>
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
     * <li>Property : "{@value #REQUEST_FORMAT}"</li>
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
     * <li>Property : "{@value #ENABLE_SERVER_ROUTING}"</li>
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
     * <li>Property : "{@value #NUM_SCAN_PERMITS}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_IMPLEMENTATION}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_BANNAGE_PERIOD}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_THRESHOLD}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_THRESHOLD_INTERVAL}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES}"</li>
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
     * <li>Property : "{@value #FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD}"</li>
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
     * <li>Property : "{@value #RETENTION_CLEANUP_FIRST_START_HOUR}"</li>
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
     * <li>Property : "{@value #RETENTION_CLEANUP_FIRST_START_DAY}"</li>
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
     * <li>Property : "{@value #RETENTION_CLEANUP_PIN_START_TIME}"</li>
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
     * <li>Property : "{@value #ENFORCE_RETENTION_POLICY_ON_READ}"</li>
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
     * <li>Property : "{@value #DELETE_EXPIRED_VALUES_ON_READ}"</li>
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
     * <li>Property : "{@value #ADMIN_CLIENT_SOCKET_TIMEOUT_SEC}"</li>
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
     * <li>Property : "{@value #ADMIN_CLIENT_CONNECTION_TIMEOUT_SEC}"</li>
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
     * <li>Property : "{@value #REBALANCING_TIMEOUT_SECONDS}"</li>
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
     * <li>Property : "{@value #ENABLE_GOSSIP}"</li>
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
     * <li>Property : "{@value #FETCHER_MAX_BYTES_PER_SEC}"</li>
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
     * <li>Property : "{@value #FETCHER_THROTTLER_INTERVAL}"</li>
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
     * <li>Property : "{@value #FETCHER_REPORTING_INTERVAL_BYTES}"</li>
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
     * <li>Property : "{@value #FETCHER_RETRY_COUNT}"</li>
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
     * <li>Property : "{@value #FETCHER_RETRY_DELAY_MS}"</li>
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
     * <li>Property : "{@value #FETCHER_LOGIN_INTERVAL_MS}"</li>
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
     * <li>Property : "{@value #DEFAULT_STORAGE_SPACE_QUOTA_IN_KB}"</li>
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
     * <li>Property : "{@value #HDFS_FETCHER_BUFFER_SIZE}"</li>
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
     * <li>Property : "{@value #HDFS_FETCHER_SOCKET_TIMEOUT}"</li>
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
     * <li>Property : "{@value #READONLY_SEARCH_STRATEGY}"</li>
     * <li>Default :BinarySearchStrategy.class.getName()</li>
     * </ul>
     */
    public void setReadOnlySearchStrategy(String readOnlySearchStrategy) {
        this.readOnlySearchStrategy = readOnlySearchStrategy;
    }

    public String getReadOnlyModifyProtocol() {
        return this.readOnlyModifyProtocol;
    }

    /**
     * Set modified protocol used to fetch files. If empty, protocol will
     * not be modified.
     *
     * <ul>
     * <li>Property : "{@value #READONLY_MODIFY_PROTOCOL}"</li>
     * <li>Default : ""</li>
     * </ul>
     */
    public void setReadOnlyModifyProtocol(String modifiedProtocol) {
        this.readOnlyModifyProtocol = modifiedProtocol;
    }

    public Integer getReadOnlyModifyPort() {
        return this.readOnlyModifyPort;
    }

    /**
     * Set modified port used to fetch file.
     *
     * If -1, the port will not be modified.
     *
     * N.B.: This setting is ignored if:
     * 1. "{@value #READONLY_OMIT_PORT}" is set to true, or
     * 2. "{@value #READONLY_MODIFY_PROTOCOL}" is empty or null.
     *
     * <ul>
     * <li>Property : "{@value #READONLY_MODIFY_PORT}"</li>
     * <li>Default : -1</li>
     * </ul>
     */
    public void setReadOnlyModifyPort(Integer readOnlyModifyPort) {
        this.readOnlyModifyPort = readOnlyModifyPort;
    }

    public boolean getReadOnlyOmitPort() {
      return this.readOnlyOmitPort;
    }

    /**
     * If true, the port used to fetch file will be omitted completely. For example, a URL like this:
     *
     * scheme://host:port/path
     *
     * will be changed to this:
     *
     * scheme://host/path
     *
     * N.B.: This setting is ignored if "{@value #READONLY_MODIFY_PROTOCOL}" is empty or null.
     *
     * <ul>
     * <li>Property : "{@value #READONLY_OMIT_PORT}"</li>
     * <li>Default : false</li>
     * </ul>
     */
    public void setReadOnlyOmitPort(boolean readOnlyOmitPort) {
      this.readOnlyOmitPort = readOnlyOmitPort;
    }

    public boolean isReadOnlyBuildPrimaryReplicasOnly() {
        return this.readOnlyBuildPrimaryReplicasOnly;
    }

    /**
     * Whether the server should advertise itself as capable of handling
     * the BuildAndPushJob's "build.primary.replicas.only" mode.
     *
     * <ul>
     * <li>Property : "{@value #READONLY_BUILD_PRIMARY_REPLICAS_ONLY}"</li>
     * <li>Default : true</li>
     * </ul>
     */
    public void setReadOnlyBuildPrimaryReplicasOnly(boolean readOnlyBuildPrimaryReplicasOnly) {
        this.readOnlyBuildPrimaryReplicasOnly = readOnlyBuildPrimaryReplicasOnly;
    }

    public boolean isBouncyCastleEnabled () {
        return bouncyCastleEnabled;
    }

    /**
     * Set whether use bouncy castle as JCE provider or not.
     *
     * <ul>
     * <li>Property : "use.bouncycastle.for.ssl"</li>
     * <li>Default : false</li>
     * </ul>
     */
    public void setBouncyCastleEnabled (boolean bouncyCastleEnabled) {
        this.bouncyCastleEnabled = bouncyCastleEnabled;
    }
    public boolean isHighAvailabilityPushEnabled() {
        return highAvailabilityPushEnabled;
    }

    /**
     * Sets whether a high-availability strategy is to be used while pushing whole
     * data sets (such as when bulk loading Read-Only stores).
     *
     * <ul>
     * <li>Property : "{@value #PUSH_HA_ENABLED}"</li>
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
     * <li>Property : "{@value #PUSH_HA_CLUSTER_ID}"</li>
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
     * <li>Property : "{@value #PUSH_HA_MAX_NODE_FAILURES}"</li>
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
     * <li>Property : "{@value #PUSH_HA_LOCK_PATH}"</li>
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
     * <li>Property : "{@value #PUSH_HA_LOCK_IMPLEMENTATION}"</li>
     * <li>Default : null</li>
     * </ul>
     */
    public void setHighAvailabilityPushLockImplementation(String highAvailabilityPushLockImplementation) {
        this.highAvailabilityPushLockImplementation = highAvailabilityPushLockImplementation;
    }

    public boolean getHighAvailabilityStateAutoCleanUp() {
        return highAvailabilityStateAutoCleanUp;
    }

    /**
     * When using a high-availability push strategy, there is a shared state which
     * accumulates in order to prevent race conditions. This state can become stale
     * if Voldemort has been recovered, which prevents the cluster from continuing
     * to be highly-available in the future. This configuration allows the server
     * to automatically clean up this shared state when it transitions from offline
     * to online, as well as when old store-versions are deleted, which happens
     * asynchronously after a new store-version is swapped in.
     *
     * N.B.: This is an experimental feature! Hence it is disabled by default.
     *
     * <ul>
     * <li>Property : "{@value #PUSH_HA_STATE_AUTO_CLEANUP}"</li>
     * <li>Default : false</li>
     * </ul>
     */
    public void setHighAvailabilityStateAutoCleanUp(boolean highAvailabilityStateAutoCleanUp) {
        this.highAvailabilityStateAutoCleanUp = highAvailabilityStateAutoCleanUp;
    }

    public boolean isNetworkClassLoaderEnabled() {
        return enableNetworkClassLoader;
    }

    /**
     * Loads a class to be used as a {@link VoldemortFilter}. Note that this is
     * not officially supported
     * 
     * <ul>
     * <li>Property : "{@value #ENABLE_NETWORK_CLASSLOADER}"</li>
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
     * <li>Property : "{@value #ENABLE_REBALANCING}"</li>
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
     * <li>Property : "{@value #MAX_PARALLEL_STORES_REBALANCING}"</li>
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
     * <li>Property : "{@value #MAX_PROXY_PUT_THREADS}"</li>
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
     * <li>Property : "{@value #USE_PARTITION_SCAN_FOR_REBALANCE}"</li>
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
     * <li>Property : "{@value #ENABLE_JMX_CLUSTERNAME}"</li>
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
     * <li>Property : "{@value #ENABLE_QUOTA_LIMITING}"</li>
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
     * <li>Property : "{@value #GOSSIP_INTERVAL_MS}"</li>
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
     * <li>Property : "{@value #REST_ENABLE}"</li>
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
     * <li>Property : "{@value #NUM_REST_SERVICE_NETTY_SERVER_BACKLOG}"</li>
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
     * <li>Property : "{@value #NUM_REST_SERVICE_NETTY_BOSS_THREADS}"</li>
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
     * <li>Property : "{@value #NUM_REST_SERVICE_NETTY_WORKER_THREADS}"</li>
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
     * <li>Property : "{@value #NUM_REST_SERVICE_STORAGE_THREADS}"</li>
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
     * <li>Property : "{@value #REST_SERVICE_STORAGE_THREAD_POOL_QUEUE_SIZE}"</li>
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
     * <li>Property : "{@value #MAX_HTTP_AGGREGATED_CONTENT_LENGTH}"</li>
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
     * <li>Property : "{@value #REPAIRJOB_MAX_KEYS_SCANNED_PER_SEC}"</li>
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
     * <li>Property : "{@value #PRUNEJOB_MAX_KEYS_SCANNED_PER_SEC}"</li>
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
     * <li>Property : "{@value #SLOP_PURGEJOB_MAX_KEYS_SCANNED_PER_SEC}"</li>
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
     * <li>Property : "{@value #READONLY_KERBEROS_KDC}"</li>
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
     * <li>Property : "{@value #READONLY_KERBEROS_REALM}"</li>
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
     * <li>Property : "{@value #FILE_FETCHER_CLASS}"</li>
     * <li>Default : "null"</li>
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
     * <li>Property : "{@value #READONLY_MAX_VALUE_BUFFER_ALLOCATION_SIZE}"</li>
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
     *        <li>Property : "{@value #READONLY_COMPRESSION_CODEC}"</li>
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
     * <li>Property : "{@value #ROCKSDB_DATA_DIR}"</li>
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
     * <li>Property : "{@value #NIO_ADMIN_CONNECTOR_KEEPALIVE}"</li>
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
     * <li>Property : "{@value #NIO_CONNECTOR_KEEPALIVE}"</li>
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

    public boolean isEnableNodeIdDetection() {
        return enableNodeIdDetection;
    }

    public void setEnableNodeIdDetection(boolean enableNodeIdDetection) {
        this.enableNodeIdDetection = enableNodeIdDetection;
    }

    public boolean isValidateNodeId() {
        return validateNodeId;
    }

    public void setValidateNodeId(boolean validateNodeId) {
        this.validateNodeId = validateNodeId;
    }

    public List<String> getNodeIdHostTypes() {
        return nodeIdHostTypes;
    }

    public void setNodeIdHostTypes(List<String> nodeIdHostTypes) {
        this.nodeIdHostTypes = nodeIdHostTypes;
    }

    public HostMatcher getNodeIdImplementation() {
        if(nodeIdImplementation == null) {
            throw new ConfigurationException("Node Id implementation is incorrectly configured ");
        }
        return nodeIdImplementation;
    }

    public void setNodeIdImplementation(HostMatcher nodeIdImplementation) {
        if(nodeIdImplementation == null) {
            throw new ConfigurationException("Node Id implementation can't be null");
        }
        this.nodeIdImplementation = nodeIdImplementation;
    }
}
