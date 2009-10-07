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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.ClientThreadPool;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortMetadata;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.logging.LoggingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.store.slop.Slop;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ConfigurationException;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.ReflectUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.VectorClockInconsistencyResolver;

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
    private final StoreRepository storeRepository;
    private final SchedulerService scheduler;
    private final VoldemortMetadata metadata;
    private final Semaphore cleanupPermits;
    private final SocketPool socketPool;
    private final ConcurrentMap<String, StorageConfiguration> storageConfigs;
    private final ClientThreadPool clientThreadPool;

    public StorageService(StoreRepository storeRepository,
                          VoldemortMetadata metadata,
                          SchedulerService scheduler,
                          VoldemortConfig config) {
        super(ServiceType.STORAGE);
        this.voldemortConfig = config;
        this.scheduler = scheduler;
        this.storeRepository = storeRepository;
        this.metadata = metadata;
        this.cleanupPermits = new Semaphore(1);
        this.storageConfigs = new ConcurrentHashMap<String, StorageConfiguration>();
        this.clientThreadPool = new ClientThreadPool(config.getClientMaxThreads(),
                                                     config.getClientThreadIdleMs(),
                                                     config.getClientMaxQueuedRequests());
        this.socketPool = new SocketPool(config.getClientMaxConnectionsPerNode(),
                                         config.getClientConnectionTimeoutMs(),
                                         config.getSocketTimeoutMs(),
                                         config.getSocketBufferSize());
    }

    private void initStorageConfig(String configClassName) {
        try {
            Class<?> configClass = ReflectUtils.loadClass(configClassName);
            StorageConfiguration configuration = (StorageConfiguration) ReflectUtils.callConstructor(configClass,
                                                                                                     new Class<?>[] { VoldemortConfig.class },
                                                                                                     new Object[] { voldemortConfig });
            logger.info("Initializing " + configuration.getType() + " storage engine.");
            storageConfigs.put(configuration.getType(), configuration);

            if(voldemortConfig.isJmxEnabled())
                JmxUtils.registerMbean(configuration.getType() + "StorageConfiguration",
                                       configuration);
        } catch(IllegalStateException e) {
            logger.error("Error loading storage configuration '" + configClassName + "'.", e);
        }

        if(storageConfigs.size() == 0)
            throw new ConfigurationException("No storage engine has been enabled!");
    }

    @Override
    protected void startInner() {
        MetadataStore metadataStore = MetadataStore.readFromDirectory(new File(voldemortConfig.getMetadataDirectory()));
        registerEngine(metadataStore);

        /* Initialize storage configurations */
        for(String configClassName: voldemortConfig.getStorageConfigurations())
            initStorageConfig(configClassName);

        /* Register slop stores */
        if(voldemortConfig.isSlopEnabled()) {
            StorageEngine<ByteArray, byte[]> slopEngine = getStorageEngine("slop",
                                                                           voldemortConfig.getSlopStoreType());
            registerEngine(slopEngine);
            storeRepository.setSlopStore(new SerializingStorageEngine<ByteArray, Slop>(slopEngine,
                                                                                       new ByteArraySerializer(),
                                                                                       new SlopSerializer()));
        }
        List<StoreDefinition> storeDefs = new ArrayList<StoreDefinition>(this.metadata.getStoreDefs()
                                                                                      .values());
        logger.info("Initializing stores:");
        for(StoreDefinition def: storeDefs) {
            openStore(def);
        }
        logger.info("All stores initialized.");
    }

    public void openStore(StoreDefinition storeDef) {
        logger.info("Opening store '" + storeDef.getName() + "' (" + storeDef.getType() + ").");
        StorageEngine<ByteArray, byte[]> engine = getStorageEngine(storeDef.getName(),
                                                                   storeDef.getType());
        registerEngine(engine);

        if(voldemortConfig.isServerRoutingEnabled())
            registerNodeStores(storeDef, metadata.getCurrentCluster(), voldemortConfig.getNodeId());

        if(storeDef.hasRetentionPeriod())
            scheduleCleanupJob(storeDef, engine);
    }

    /**
     * Register the given engine with the storage repository
     * 
     * @param engine Register the storage engine
     */
    public void registerEngine(StorageEngine<ByteArray, byte[]> engine) {
        Cluster cluster = this.metadata.getCurrentCluster();
        storeRepository.addStorageEngine(engine);

        /* Now add any store wrappers that are enabled */
        Store<ByteArray, byte[]> store = engine;
        if(voldemortConfig.isVerboseLoggingEnabled())
            store = new LoggingStore<ByteArray, byte[]>(store,
                                                        cluster.getName(),
                                                        SystemTime.INSTANCE);
        if(voldemortConfig.isStatTrackingEnabled()) {
            store = new StatTrackingStore<ByteArray, byte[]>(store);
            if(voldemortConfig.isJmxEnabled())
                JmxUtils.registerMbean(store.getName(), store);
        }

        storeRepository.addLocalStore(store);
    }

    public void registerNodeStores(StoreDefinition def, Cluster cluster, int localNode) {
        Map<Integer, Store<ByteArray, byte[]>> nodeStores = new HashMap<Integer, Store<ByteArray, byte[]>>(cluster.getNumberOfNodes());
        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> store;
            if(node.getId() == localNode) {
                store = this.storeRepository.getLocalStore(def.getName());
            } else {
                store = new SocketStore(def.getName(),
                                        new SocketDestination(node.getHost(),
                                                              node.getSocketPort(),
                                                              voldemortConfig.getRequestFormatType()),
                                        socketPool,
                                        false);
            }
            this.storeRepository.addNodeStore(node.getId(), store);
            nodeStores.put(node.getId(), store);
        }

        Store<ByteArray, byte[]> routedStore = new RoutedStore(def.getName(),
                                                               nodeStores,
                                                               cluster,
                                                               def,
                                                               true,
                                                               this.clientThreadPool,
                                                               voldemortConfig.getRoutingTimeoutMs(),
                                                               voldemortConfig.getClientNodeBannageMs(),
                                                               SystemTime.INSTANCE);
        routedStore = new InconsistencyResolvingStore<ByteArray, byte[]>(routedStore,
                                                                         new VectorClockInconsistencyResolver<byte[]>());
        this.storeRepository.addRoutedStore(routedStore);
    }

    /**
     * Schedule a data retention cleanup job for the given store
     * 
     * @param storeDef The store definition
     * @param engine The storage engine to do cleanup on
     */
    private void scheduleCleanupJob(StoreDefinition storeDef,
                                    StorageEngine<ByteArray, byte[]> engine) {
        // Schedule data retention cleanup job if applicable
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // allow only one cleanup job at a time
        Date startTime = cal.getTime();

        int maxReadRate = storeDef.hasRetentionScanThrottleRate() ? storeDef.getRetentionScanThrottleRate()
                                                                 : Integer.MAX_VALUE;

        logger.info("Scheduling data retention cleanup job for store '" + storeDef.getName()
                    + "' at " + startTime + " with retention scan throttle rate:" + maxReadRate
                    + " Entries/second.");

        EventThrottler throttler = new EventThrottler(maxReadRate);

        Runnable cleanupJob = new DataCleanupJob<ByteArray, byte[]>(engine,
                                                                    cleanupPermits,
                                                                    storeDef.getRetentionDays()
                                                                            * Time.MS_PER_DAY,
                                                                    SystemTime.INSTANCE,
                                                                    throttler);
        this.scheduler.schedule(cleanupJob, startTime, Time.MS_PER_DAY);
    }

    private StorageEngine<ByteArray, byte[]> getStorageEngine(String name, String type) {
        StorageConfiguration config = storageConfigs.get(type);
        if(config == null)
            throw new ConfigurationException("Attempt to open store " + name + " but " + type
                                             + " storage engine of type " + type
                                             + " has not been enabled.");
        return config.getStore(name);
    }

    @Override
    protected void stopInner() {
        /*
         * We may end up closing a given store more than once, but that is cool
         * because close() is idempotent
         */

        Exception lastException = null;
        logger.info("Closing all stores.");
        /* This will also close the node stores including local stores */
        for(Store<ByteArray, byte[]> store: this.storeRepository.getAllRoutedStores()) {
            logger.info("Closing routed store for " + store.getName());
            try {
                store.close();
            } catch(Exception e) {
                lastException = e;
            }
        }
        /* This will also close the storage engines */
        for(Store<ByteArray, byte[]> store: this.storeRepository.getAllStorageEngines()) {
            logger.info("Closing storage engine for " + store.getName());
            try {
                store.close();
            } catch(Exception e) {
                lastException = e;
            }
        }
        logger.info("All stores closed.");

        /* Close slop store if necessary */
        if(this.storeRepository.hasSlopStore()) {
            try {
                this.storeRepository.getSlopStore().close();
            } catch(Exception e) {
                lastException = e;
            }
        }

        /* Close all storage configs */
        logger.info("Closing storage configurations.");
        for(StorageConfiguration config: storageConfigs.values()) {
            logger.info("Closing " + config.getType() + " storage config.");
            try {
                config.close();
            } catch(Exception e) {
                lastException = e;
            }
        }

        this.clientThreadPool.shutdownNow();
        logger.info("Closed client threadpool.");

        /* If there is an exception, throw it */
        if(lastException instanceof VoldemortException)
            throw (VoldemortException) lastException;
        else if(lastException != null)
            throw new VoldemortException(lastException);
    }

    public VoldemortMetadata getMetadataStore() {
        return this.metadata;
    }

    public StoreRepository getStoreRepository() {
        return this.storeRepository;
    }

    @JmxOperation(description = "Force cleanup of old data based on retention policy, allows override of throttle-rate", impact = MBeanOperationInfo.ACTION)
    public void forceCleanupOldData(String storeName) {
        StoreDefinition storeDef = getMetadataStore().getStoreDef(storeName);
        int throttleRate = storeDef.hasRetentionScanThrottleRate() ? storeDef.getRetentionScanThrottleRate()
                                                                  : Integer.MAX_VALUE;

        forceCleanupOldDataThrottled(storeName, throttleRate);
    }

    @JmxOperation(description = "Force cleanup of old data based on retention policy.", impact = MBeanOperationInfo.ACTION)
    public void forceCleanupOldDataThrottled(String storeName, int entryScanThrottleRate) {
        logger.info("forceCleanupOldData() called for store " + storeName
                    + " with retention scan throttle rate:" + entryScanThrottleRate
                    + " Entries/second.");

        try {
            StoreDefinition storeDef = getMetadataStore().getStoreDef(storeName);
            StorageEngine<ByteArray, byte[]> engine = storeRepository.getStorageEngine(storeName);

            if(null != engine) {
                if(storeDef.hasRetentionPeriod()) {
                    ExecutorService executor = Executors.newFixedThreadPool(1);
                    try {
                        if(cleanupPermits.availablePermits() >= 1) {

                            executor.execute(new DataCleanupJob<ByteArray, byte[]>(engine,
                                                                                   cleanupPermits,
                                                                                   storeDef.getRetentionDays()
                                                                                           * Time.MS_PER_DAY,
                                                                                   SystemTime.INSTANCE,
                                                                                   new EventThrottler(entryScanThrottleRate)));
                        } else {
                            logger.error("forceCleanupOldData() No permit available to run cleanJob already running multiple instance."
                                         + engine.getName());
                        }
                    } finally {
                        executor.shutdown();
                    }
                } else {
                    logger.error("forceCleanupOldData() No retention policy found for " + storeName);
                }
            }
        } catch(Exception e) {
            logger.error("Error while running forceCleanupOldData()", e);
            throw new VoldemortException(e);
        }
    }
}
