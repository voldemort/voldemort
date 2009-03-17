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

package voldemort.server.storage;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.cluster.Cluster;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.AbstractService;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.logging.LoggingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.RandomAccessFileStorageConfiguration;
import voldemort.store.readonly.RandomAccessFileStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopDetectingStore;
import voldemort.store.stats.StatTrackingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ConfigurationException;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * The service responsible for managing all storage types
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "Start and stop all stores.")
public class StorageService extends AbstractService {

    private static final Logger logger = Logger.getLogger(StorageService.class.getName());

    private final VoldemortConfig voldemortConfig;
    private final ConcurrentMap<String, Store<ByteArray, byte[]>> localStoreMap;
    private final Map<String, StorageEngine<ByteArray, byte[]>> rawEngines;
    private final ConcurrentMap<String, StorageConfiguration> storageConfigurations;
    private final SchedulerService scheduler;
    private final Map<String, RandomAccessFileStore> readOnlyStores;
    private MetadataStore metadataStore;
    private Store<ByteArray, Slop> slopStore;

    public StorageService(String name,
                          ConcurrentMap<String, Store<ByteArray, byte[]>> storeMap,
                          SchedulerService scheduler,
                          VoldemortConfig config) {
        super(name);
        this.voldemortConfig = config;
        this.localStoreMap = storeMap;
        this.rawEngines = new ConcurrentHashMap<String, StorageEngine<ByteArray, byte[]>>();
        this.scheduler = scheduler;
        this.storageConfigurations = initStorageConfigurations(config);
        this.metadataStore = new MetadataStore(new File(config.getMetadataDirectory()));
        this.readOnlyStores = new ConcurrentHashMap<String, RandomAccessFileStore>();
    }

    private ConcurrentMap<String, StorageConfiguration> initStorageConfigurations(VoldemortConfig config) {
        ConcurrentMap<String, StorageConfiguration> configs = new ConcurrentHashMap<String, StorageConfiguration>();
        for(String configClassName: config.getStorageConfigurations()) {
            try {
                Class<?> configClass = Utils.loadClass(configClassName);
                StorageConfiguration configuration = (StorageConfiguration) Utils.callConstructor(configClass,
                                                                                                  new Class<?>[] { VoldemortConfig.class },
                                                                                                  new Object[] { config });
                logger.info("Initializing " + configuration.getType() + " storage engine.");
                configs.put(configuration.getType(), configuration);
            } catch(IllegalStateException e) {
                logger.error("Error loading storage configuration '" + configClassName + "'.", e);
            }
        }

        if(configs.size() == 0)
            throw new ConfigurationException("No storage engine has been enabled!");

        return configs;
    }

    @Override
    protected void startInner() {
        this.localStoreMap.clear();
        this.localStoreMap.put(MetadataStore.METADATA_STORE_NAME, metadataStore);
        Store<ByteArray, byte[]> slopStorage = getStore("slop", voldemortConfig.getSlopStoreType());
        this.slopStore = new SerializingStore<ByteArray, Slop>(slopStorage,
                                                               new ByteArraySerializer(),
                                                               new SlopSerializer());
        Cluster cluster = this.metadataStore.getCluster();
        List<StoreDefinition> storeDefs = this.metadataStore.getStores();
        logger.info("Initializing stores:");
        Time time = new SystemTime();
        for(StoreDefinition def: storeDefs) {
            if(!def.getName().equals(MetadataStore.METADATA_STORE_NAME)) {
                logger.info("Opening store '" + def.getName() + "'.");
                StorageEngine<ByteArray, byte[]> engine = getStore(def.getName(), def.getType());
                rawEngines.put(engine.getName(), engine);

                if(def.getType().equals(RandomAccessFileStorageConfiguration.TYPE_NAME))
                    this.readOnlyStores.put(engine.getName(), (RandomAccessFileStore) engine);

                /* Now add any store wrappers that are enabled */
                Store<ByteArray, byte[]> store = engine;
                if(voldemortConfig.isSlopDetectionEnabled()) {
                    RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(),
                                                                                    def.getReplicationFactor());
                    store = new SlopDetectingStore(store,
                                                   this.slopStore,
                                                   def.getReplicationFactor(),
                                                   cluster.getNodeById(this.voldemortConfig.getNodeId()),
                                                   routingStrategy);
                }
                if(voldemortConfig.isVerboseLoggingEnabled())
                    store = new LoggingStore<ByteArray, byte[]>(store, cluster.getName(), time);
                if(voldemortConfig.isStatTrackingEnabled())
                    store = new StatTrackingStore<ByteArray, byte[]>(store);
                this.localStoreMap.put(def.getName(), store);
            }
        }
        logger.info("All stores initialized.");

        scheduleCleanupJobs(storeDefs, rawEngines);
    }

    private void scheduleCleanupJobs(List<StoreDefinition> storeDefs,
                                     Map<String, StorageEngine<ByteArray, byte[]>> engines) {
        // Schedule data retention cleanup jobs
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // allow only one cleanup job at a time
        Date startTime = cal.getTime();
        Semaphore cleanupPermits = new Semaphore(1);
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.hasRetentionPeriod()) {
                logger.info("Scheduling data retention cleanup job for store '"
                            + storeDef.getName() + "' at " + startTime + ".");
                StorageEngine<ByteArray, byte[]> engine = engines.get(storeDef.getName());
                Runnable cleanupJob = new DataCleanupJob<ByteArray, byte[]>(engine,
                                                                            cleanupPermits,
                                                                            storeDef.getRetentionDays()
                                                                                    * Time.MS_PER_DAY,
                                                                            SystemTime.INSTANCE);
                this.scheduler.schedule(cleanupJob, startTime, Time.MS_PER_DAY);
            }
        }
    }

    private StorageEngine<ByteArray, byte[]> getStore(String name, String type) {
        StorageConfiguration config = storageConfigurations.get(type);
        if(config == null)
            throw new ConfigurationException("Attempt to open store " + name + " but " + type
                                             + " storage engine has not been enabled.");
        return config.getStore(name);
    }

    @Override
    protected void stopInner() {
        try {
            if(metadataStore != null)
                metadataStore.close();
        } catch(VoldemortException e) {
            logger.error("Error while closing metadata store:", e);
        }
        try {
            if(slopStore != null)
                slopStore.close();
        } catch(VoldemortException e) {
            logger.error("Error while closing metadata store:", e);
        }
        VoldemortException exception = null;
        logger.info("Closing stores:");
        for(Store<ByteArray, byte[]> s: this.localStoreMap.values()) {
            try {
                logger.info("Closing store '" + s.getName() + "'.");
                s.close();
            } catch(VoldemortException e) {
                // in the event of a failure still attempt to close other stores
                logger.error(e);
                exception = e;
            }
        }
        this.localStoreMap.clear();

        logger.info("Closing storage configurations:");
        for(StorageConfiguration config: storageConfigurations.values()) {
            try {
                logger.info("Closing storage configuration for " + config.getType());
                config.close();
            } catch(VoldemortException e) {
                logger.error("Error when shutting down storage configuration: ", e);
            }
        }

        // propagate the first exception
        if(exception != null)
            throw exception;
        logger.info("All stores closed.");
    }

    public ConcurrentMap<String, Store<ByteArray, byte[]>> getLocalStoreMap() {
        return localStoreMap;
    }

    @JmxGetter(name = "storeNames", description = "Get the names of all open stores.")
    public Set<String> getStoreNames() {
        return new HashSet<String>(localStoreMap.keySet());
    }

    @JmxOperation(impact = MBeanOperationInfo.ACTION, description = "Push all keys that do not belong to this store out to the correct store.")
    public void rebalance() {
    // this.scheduler.scheduleNow(new
    // RebalancingJob(voldemortConfig.getNodeId(), this.rawEngines));
    }

    public StorageConfiguration getStorageConfiguration(String type) {
        return storageConfigurations.get(type);
    }

    public MetadataStore getMetadataStore() {
        return this.metadataStore;
    }

    public Map<String, RandomAccessFileStore> getReadOnlyStores() {
        return this.readOnlyStores;
    }

    public Store<ByteArray, Slop> getSlopStore() {
        return this.slopStore;
    }

}
