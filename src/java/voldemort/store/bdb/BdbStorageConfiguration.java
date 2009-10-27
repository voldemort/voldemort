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
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;

import com.google.common.collect.Maps;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;

/**
 * The configuration that is shared between berkeley db instances. This includes
 * the db environment and the configuration
 * 
 * @author jay
 * 
 */
public class BdbStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "bdb";
    private static final String SHARED_ENV_KEY = "shared";

    private static Logger logger = Logger.getLogger(BdbStorageConfiguration.class);

    private final Object lock = new Object();
    private final Map<String, Environment> environments = Maps.newHashMap();
    private final EnvironmentConfig environmentConfig;
    private final DatabaseConfig databaseConfig;
    private final Map<String, BdbStorageEngine> stores = Maps.newHashMap();
    private final String bdbMasterDir;
    private final boolean useOneEnvPerStore;

    public BdbStorageConfiguration(VoldemortConfig config) {
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTransactional(true);
        environmentConfig.setCacheSize(config.getBdbCacheSize());
        if(config.isBdbWriteTransactionsEnabled() && config.isBdbFlushTransactionsEnabled()) {
            environmentConfig.setTxnNoSync(false);
            environmentConfig.setTxnWriteNoSync(false);
        } else if(config.isBdbWriteTransactionsEnabled() && !config.isBdbFlushTransactionsEnabled()) {
            environmentConfig.setTxnNoSync(false);
            environmentConfig.setTxnWriteNoSync(true);
        } else {
            environmentConfig.setTxnNoSync(true);
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
        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setSortedDuplicates(config.isBdbSortedDuplicatesEnabled());
        databaseConfig.setNodeMaxEntries(config.getBdbBtreeFanout());
        databaseConfig.setTransactional(true);
        bdbMasterDir = config.getBdbDataDirectory();
        useOneEnvPerStore = config.isBdbOneEnvPerStore();
        if(useOneEnvPerStore)
            environmentConfig.setSharedCache(true);
    }

    public StorageEngine<ByteArray, byte[]> getStore(String storeName) {
        synchronized(lock) {
            BdbStorageEngine store = stores.get(storeName);
            if(store != null)
                return stores.get(storeName);
            try {
                Environment environment = getEnvironment(storeName);
                Database db = environment.openDatabase(null, storeName, databaseConfig);
                BdbStorageEngine engine = new BdbStorageEngine(storeName, environment, db);
                stores.put(storeName, engine);
                return engine;
            } catch(DatabaseException d) {
                throw new StorageInitializationException(d);
            }
        }
    }

    private Environment getEnvironment(String storeName) throws DatabaseException {
        synchronized(lock) {
            if(useOneEnvPerStore) {

                // if we have already created this environment return a
                // reference
                if(environments.containsKey(storeName))
                    return environments.get(storeName);

                // otherwise create a new environment
                File bdbDir = new File(bdbMasterDir, storeName);
                createBdbDirIfNecessary(bdbDir);

                Environment environment = new Environment(bdbDir, environmentConfig);
                logger.info("Creating environment for " + storeName + ": ");
                logEnvironmentConfig(environment.getConfig());
                environments.put(storeName, environment);
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
    }

    public String getType() {
        return TYPE_NAME;
    }

    public EnvironmentStats getStats(String storeName) {
        StatsConfig config = new StatsConfig();
        config.setFast(false);
        try {
            Environment env = getEnvironment(storeName);
            return env.getStats(config);
        } catch(DatabaseException e) {
            throw new VoldemortException(e);
        }
    }

    @JmxOperation(description = "A variety of stats about one BDB environment.")
    public String getEnvStatsAsString(String storeName) throws Exception {
        return getStats(storeName).toString();
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

}
