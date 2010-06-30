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

import com.db4o.config.QueryEvaluationMode;
import com.db4o.cs.Db4oClientServer;
import com.db4o.cs.config.ServerConfiguration;
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

    private final ServerConfiguration db4oConfig;
    private final VoldemortConfig voldemortConfig;

    public Db4oStorageConfiguration(VoldemortConfig config) {
        this.voldemortConfig = config;
        this.db4oConfig = newDb4oConfig();
        db4oMasterDir = "config/single_node_cluster_db4o/data/";
        // TODO Via voldemortConfig get the db4o configuration parameters
    }

    public StorageEngine<ByteArray, byte[]> getStore(String storeName) {
        synchronized(lock) {
            Db4oByteArrayStorageEngine store = stores.get(storeName);
            if(store != null)
                return stores.get(storeName);
            try {
                logger.info("Creating db4o store named '" + storeName + "' in file "
                            + db4oMasterDir + storeName);
                Db4oByteArrayStorageEngine engine = new Db4oByteArrayStorageEngine(db4oMasterDir
                                                                                           + storeName,
                                                                                   newDb4oConfig());
                stores.put(storeName, engine);
                return engine;
            } catch(Db4oException d) {
                throw new StorageInitializationException(d);
            }
        }
    }

    private ServerConfiguration newDb4oConfig() {
        return getDb4oConfig(Db4oKeyValueProvider.KEY_VALUE_PAIR_CLASS,
                             Db4oKeyValueProvider.KEY_FIELD_NAME);
    }

    @SuppressWarnings("unchecked")
    private ServerConfiguration getDb4oConfig(Class keyValuePairClass, String keyFieldName) {
        logger.info("Creating db4o configuration instance.");
        ServerConfiguration config = Db4oClientServer.newServerConfiguration();
        // Use lazy mode
        config.common().queries().evaluationMode(QueryEvaluationMode.LAZY);
        // Set activation depth to 3
        config.common().activationDepth(3);
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
        throw new VoldemortException("Db4o stats not implemented yetfor store " + storeName);
    }

    public void close() {
        logger.info("Closing db4o store.");
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
