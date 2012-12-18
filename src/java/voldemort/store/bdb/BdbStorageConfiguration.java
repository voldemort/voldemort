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

package voldemort.store.bdb;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;

import com.google.common.collect.Maps;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.StatsConfig;

/**
 * The configuration that is shared between berkeley db instances. This includes
 * the db environment and the configuration
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class BdbStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "bdb";
    private static final String SHARED_ENV_KEY = "shared";

    private static Logger logger = Logger.getLogger(BdbStorageConfiguration.class);
    private final Object lock = new Object();
    private final Map<String, Environment> environments = Maps.newHashMap();
    private final EnvironmentConfig environmentConfig;
    private final DatabaseConfig databaseConfig;
    private final String bdbMasterDir;
    private final boolean useOneEnvPerStore;
    private final VoldemortConfig voldemortConfig;
    private long reservedCacheSize = 0;
    private Set<Environment> unreservedStores;

    public BdbStorageConfiguration(VoldemortConfig config) {
        this.voldemortConfig = config;
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTransactional(true);
        if(config.isBdbWriteTransactionsEnabled() && config.isBdbFlushTransactionsEnabled()) {
            environmentConfig.setDurability(Durability.COMMIT_SYNC);
        } else if(config.isBdbWriteTransactionsEnabled() && !config.isBdbFlushTransactionsEnabled()) {
            environmentConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
        } else {
            environmentConfig.setDurability(Durability.COMMIT_NO_SYNC);
        }
        environmentConfig.setAllowCreate(true);
        environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                         Long.toString(config.getBdbMaxLogFileSize()));
        environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
                                         Long.toString(config.getBdbCheckpointBytes()));
        environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
                                         Long.toString(config.getBdbCheckpointMs() * Time.US_PER_MS));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION,
                                         Integer.toString(config.getBdbCleanerMinFileUtilization()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                                         Integer.toString(config.getBdbCleanerMinUtilization()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS,
                                         Integer.toString(config.getBdbCleanerThreads()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
                                         Integer.toString(config.getBdbCleanerLookAheadCacheSize()));
        environmentConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES,
                                         Integer.toString(config.getBdbLockNLockTables()));
        environmentConfig.setConfigParam(EnvironmentConfig.ENV_FAIR_LATCHES,
                                         Boolean.toString(config.getBdbFairLatches()));
        environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY,
                                         Boolean.toString(config.getBdbCheckpointerHighPriority()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES,
                                         Integer.toString(config.getBdbCleanerMaxBatchFiles()));
        environmentConfig.setConfigParam(EnvironmentConfig.LOG_FAULT_READ_SIZE,
                                         Integer.toString(config.getBdbLogFaultReadSize()));
        environmentConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE,
                                         Integer.toString(config.getBdbLogIteratorReadSize()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION,
                                         Boolean.toString(config.getBdbCleanerLazyMigration()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_BACKGROUND_PROACTIVE_MIGRATION,
                                         Boolean.toString(config.getBdbProactiveBackgroundMigration()));
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_BYTES_INTERVAL,
                                         Long.toString(config.getBdbCleanerBytesInterval()));

        environmentConfig.setLockTimeout(config.getBdbLockTimeoutMs(), TimeUnit.MILLISECONDS);
        if(config.getBdbCacheModeEvictLN()) {
            environmentConfig.setCacheMode(CacheMode.EVICT_LN);
        }
        if(config.isBdbLevelBasedEviction()) {
            environmentConfig.setConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY,
                                             Boolean.toString(false));
        }

        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setSortedDuplicates(false);
        databaseConfig.setNodeMaxEntries(config.getBdbBtreeFanout());
        databaseConfig.setTransactional(true);
        bdbMasterDir = config.getBdbDataDirectory();
        useOneEnvPerStore = config.isBdbOneEnvPerStore();
        unreservedStores = new HashSet<Environment>();
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        synchronized(lock) {
            try {
                String storeName = storeDef.getName();
                Environment environment = getEnvironment(storeDef);
                Database db = environment.openDatabase(null, storeName, databaseConfig);
                BdbRuntimeConfig runtimeConfig = new BdbRuntimeConfig(voldemortConfig);
                BdbStorageEngine engine = null;
                if(voldemortConfig.getBdbPrefixKeysWithPartitionId()) {
                    engine = new PartitionPrefixedBdbStorageEngine(storeName,
                                                                   environment,
                                                                   db,
                                                                   runtimeConfig,
                                                                   strategy);
                } else {
                    engine = new BdbStorageEngine(storeName, environment, db, runtimeConfig);
                }
                if(voldemortConfig.isJmxEnabled()) {
                    // register the environment stats mbean
                    JmxUtils.registerMbean(storeName, engine.getBdbEnvironmentStats());
                }
                return engine;
            } catch(DatabaseException d) {
                throw new StorageInitializationException(d);
            }
        }
    }

    /**
     * When a reservation is made, we need to shrink the shared cache
     * accordingly to guarantee memory foot print of the new store. NOTE: This
     * is not an instantaeneous operation. Changes will take effect only when
     * traffic is thrown and eviction happens.( Won't happen until Network ports
     * are opened anyway which is rightfully done after storage service).When
     * changing this dynamically, we might want to block until the shared cache
     * shrinks enough
     * 
     */
    private void adjustCacheSizes() {
        long newSharedCacheSize = voldemortConfig.getBdbCacheSize() - this.reservedCacheSize;
        logger.info("Setting the shared cache size to " + newSharedCacheSize);
        for(Environment environment: unreservedStores) {
            EnvironmentMutableConfig mConfig = environment.getMutableConfig();
            mConfig.setCacheSize(newSharedCacheSize);
            environment.setMutableConfig(mConfig);
        }
    }

    public Environment getEnvironment(StoreDefinition storeDef) throws DatabaseException {
        String storeName = storeDef.getName();
        synchronized(lock) {
            if(useOneEnvPerStore) {
                // if we have already created this environment return a
                // reference
                if(environments.containsKey(storeName))
                    return environments.get(storeName);

                // otherwise create a new environment
                File bdbDir = new File(bdbMasterDir, storeName);
                createBdbDirIfNecessary(bdbDir);

                // configure the BDB cache
                if(storeDef.hasMemoryFootprint()) {
                    // make room for the reservation, by adjusting other stores
                    long reservedBytes = storeDef.getMemoryFootprintMB() * ByteUtils.BYTES_PER_MB;
                    long newReservedCacheSize = this.reservedCacheSize + reservedBytes;

                    // check that we leave a 'minimum' shared cache
                    if((voldemortConfig.getBdbCacheSize() - newReservedCacheSize) < voldemortConfig.getBdbMinimumSharedCache()) {
                        throw new StorageInitializationException("Reservation of "
                                                                 + storeDef.getMemoryFootprintMB()
                                                                 + " MB for store "
                                                                 + storeName
                                                                 + " violates minimum shared cache size of "
                                                                 + voldemortConfig.getBdbMinimumSharedCache());
                    }

                    this.reservedCacheSize = newReservedCacheSize;
                    adjustCacheSizes();
                    environmentConfig.setSharedCache(false);
                    environmentConfig.setCacheSize(reservedBytes);
                } else {
                    environmentConfig.setSharedCache(true);
                    environmentConfig.setCacheSize(voldemortConfig.getBdbCacheSize()
                                                   - this.reservedCacheSize);
                }

                Environment environment = new Environment(bdbDir, environmentConfig);
                logger.info("Creating environment for " + storeName + ": ");
                logEnvironmentConfig(environment.getConfig());
                environments.put(storeName, environment);

                // save this up so we can adjust later if needed
                if(!storeDef.hasMemoryFootprint())
                    this.unreservedStores.add(environment);

                return environment;
            } else {
                if(!environments.isEmpty())
                    return environments.get(SHARED_ENV_KEY);

                File bdbDir = new File(bdbMasterDir);
                createBdbDirIfNecessary(bdbDir);

                Environment environment = new Environment(bdbDir, environmentConfig);
                logger.info("Creating shared BDB environment: ");
                logEnvironmentConfig(environment.getConfig());
                environments.put(SHARED_ENV_KEY, environment);
                return environment;
            }
        }
    }

    private void createBdbDirIfNecessary(File bdbDir) {
        if(!bdbDir.exists()) {
            logger.info("Creating BDB data directory '" + bdbDir.getAbsolutePath() + ".");
            bdbDir.mkdirs();
        }
    }

    private void logEnvironmentConfig(EnvironmentConfig config) {
        logger.info("    BDB cache size = " + config.getCacheSize());
        logger.info("    BDB " + EnvironmentConfig.CLEANER_THREADS + " = "
                    + config.getConfigParam(EnvironmentConfig.CLEANER_THREADS));
        logger.info("    BDB " + EnvironmentConfig.CLEANER_MIN_UTILIZATION + " = "
                    + config.getConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION));
        logger.info("    BDB " + EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION + " = "
                    + config.getConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION));
        logger.info("    BDB " + EnvironmentConfig.LOG_FILE_MAX + " = "
                    + config.getConfigParam(EnvironmentConfig.LOG_FILE_MAX));
        logger.info("    BDB " + config.toString().replace('\n', ','));
    }

    public String getType() {
        return TYPE_NAME;
    }

    public String getStats(String storeName, boolean fast) {
        try {
            if(environments.containsKey(storeName)) {
                StatsConfig config = new StatsConfig();
                config.setFast(fast);
                Environment env = environments.get(storeName);
                return env.getStats(config).toString();
            } else {
                // return empty string if environment not created yet
                return "";
            }
        } catch(DatabaseException e) {
            throw new VoldemortException(e);
        }
    }

    @JmxOperation(description = "A variety of quickly calculated stats about one BDB environment.")
    public String getEnvStatsAsString(String storeName) throws Exception {
        return getEnvStatsAsString(storeName, true);
    }

    @JmxOperation(description = "A variety of stats about one BDB environment.")
    public String getEnvStatsAsString(String storeName, boolean fast) throws Exception {
        String envStats = getStats(storeName, fast);
        logger.debug("Bdb Environment stats:\n" + envStats);
        return envStats;
    }

    /**
     * Forceful cleanup the logs
     */
    @JmxOperation(description = "Forceful start the cleaner threads")
    public void cleanLogs() {
        synchronized(lock) {
            try {
                for(Environment environment: environments.values()) {
                    environment.cleanLog();
                }
            } catch(DatabaseException e) {
                throw new VoldemortException(e);
            }
        }
    }

    public void close() {
        synchronized(lock) {
            try {
                for(Environment environment: environments.values()) {
                    environment.sync();
                    environment.close();
                }
            } catch(DatabaseException e) {
                throw new VoldemortException(e);
            }
        }
    }

    /**
     * Detect what has changed in the store definition and rewire BDB
     * environments accordingly.
     * 
     * @param storeDef updated store definition
     */
    public void update(StoreDefinition storeDef) {
        if(!useOneEnvPerStore)
            throw new VoldemortException("Memory foot print can be set only when using different environments per store");

        String storeName = storeDef.getName();
        Environment environment = environments.get(storeName);
        // change reservation amount of reserved store
        if(!unreservedStores.contains(environment) && storeDef.hasMemoryFootprint()) {
            EnvironmentMutableConfig mConfig = environment.getMutableConfig();
            long currentCacheSize = mConfig.getCacheSize();
            long newCacheSize = storeDef.getMemoryFootprintMB() * ByteUtils.BYTES_PER_MB;
            if(currentCacheSize != newCacheSize) {
                long newReservedCacheSize = this.reservedCacheSize - currentCacheSize
                                            + newCacheSize;

                // check that we leave a 'minimum' shared cache
                if((voldemortConfig.getBdbCacheSize() - newReservedCacheSize) < voldemortConfig.getBdbMinimumSharedCache()) {
                    throw new StorageInitializationException("Reservation of "
                                                             + storeDef.getMemoryFootprintMB()
                                                             + " MB for store "
                                                             + storeName
                                                             + " violates minimum shared cache size of "
                                                             + voldemortConfig.getBdbMinimumSharedCache());
                }

                this.reservedCacheSize = newReservedCacheSize;
                adjustCacheSizes();
                mConfig.setCacheSize(newCacheSize);
                environment.setMutableConfig(mConfig);
                logger.info("Setting private cache for store " + storeDef.getName() + " to "
                            + newCacheSize);
            }
        } else {
            // we cannot support changing a reserved store to unreserved or vice
            // versa since the sharedCache param is not mutable
            throw new VoldemortException("Cannot switch between shared and private cache dynamically");
        }
    }

    public long getReservedCacheSize() {
        return this.reservedCacheSize;
    }
}
