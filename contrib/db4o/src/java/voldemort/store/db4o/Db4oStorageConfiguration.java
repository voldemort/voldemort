/*
 * Copyright 2010 Versant Corporation
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

package voldemort.store.db4o;

import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.utils.ByteArray;

import com.db4o.Db4oEmbedded;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.config.QueryEvaluationMode;
import com.db4o.ext.Db4oException;
import com.google.common.collect.Maps;

/**
 * The configuration that is shared between db4o db instances. This includes the
 * configuration
 * 
 * 
 */
public class Db4oStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "db4o";

    private static Logger logger = Logger.getLogger(Db4oStorageConfiguration.class);

    private final Object lock = new Object();
    private final Map<String, Db4oByteArrayStorageEngine> stores = Maps.newHashMap();
    private final String db4oMasterDir;

    private final EmbeddedConfiguration db4oConfig;
    private final VoldemortConfig voldemortConfig;

    public Db4oStorageConfiguration(VoldemortConfig config) {
        this.voldemortConfig = config;
        this.db4oConfig = getDb4oConfig(Db4oKeyValuePair.class, "key");
        db4oMasterDir = "./";
        // TODO Via voldemortConfig so the following:
        // set transactional operation mode
        // set cache size: config.getDb4oCacheSize()
        // set option to write transactions:
        // config.isDb4oWriteTransactionsEnabled()
        // set option to flush transactions:
        // config.isDb4oFlushTransactionsEnabled()
        // turn off read only mode if necessary
        // set log file size: config.getDb4oMaxLogFileSize()
        // set: config.getDb4oCheckpointBytes()
        // set: config.getDb4oCheckpointMs()
        // set: config.getDb4oCleanerMinFileUtilization()
        // set: config.getDb4oCleanerMinUtilization()
        // set if duplicate key entries are sorted:
        // config.isDb4oSortedDuplicatesEnabled()
        // set limit of node entries: config.getDb4oBtreeFanout()
        // set database directory: config.getDb4oDataDirectory()
        // set if shared cache will be used: config.isDb4oOneEnvPerStore()
        // set if cursor preloading is used: config.getDb4oCursorPreload()
        /*
         * TODO bdb creates and set an environment:
         * if(environments.containsKey(storeName)) return
         * environments.get(storeName); environments.put(storeName,
         * environment);
         */
    }

    public StorageEngine<ByteArray, byte[]> getStore(String storeName) {
        synchronized(lock) {
            Db4oByteArrayStorageEngine store = stores.get(storeName);
            if(store != null)
                return stores.get(storeName);
            try {
                logger.info("Creating db4o store: ");
                // Enable cursor preload if configured
                Db4oByteArrayStorageEngine engine = new Db4oByteArrayStorageEngine(db4oMasterDir + storeName,
                                                                 db4oConfig);
                stores.put(storeName, engine);
                return engine;
            } catch(Db4oException d) {
                throw new StorageInitializationException(d);
            }
        }
    }

    private EmbeddedConfiguration getDb4oConfig(Class keyValuePairClass, String keyFieldName) {
        logger.info("Creating db4o configuration instance: ");
        EmbeddedConfiguration config = Db4oEmbedded.newConfiguration();
        // Use lazy mode
        config.common().queries().evaluationMode(QueryEvaluationMode.LAZY);
        // Set activation depth to 0
        config.common().activationDepth(0);
        // Set index by Key
        config.common().objectClass(keyValuePairClass).objectField(keyFieldName).indexed(true);
        // Cascade on delete
        config.common().objectClass(keyValuePairClass).cascadeOnDelete(true);
        return config;
    }

    public String getType() {
        return TYPE_NAME;
    }

    @JmxOperation(description = "A variety of stats about one db4o environment.")
    public String getEnvStatsAsString(String storeName) throws Exception {
        throw new VoldemortException("Db4o stats not implemented yet");
    }

    public void close() {
        synchronized(lock) {
            try {
                for(Db4oByteArrayStorageEngine engine: stores.values()) {
                    engine.close();
                }
            } catch(Db4oException e) {
                throw new VoldemortException(e);
            }
        }
    }

}
