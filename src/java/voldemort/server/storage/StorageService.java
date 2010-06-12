/*
 * Copyright 2008-2010 LinkedIn, Inc
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

import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.lang.management.ManagementFactory;
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
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.ClientThreadPool;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.ServerStoreVerifier;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.invalidmetadata.InvalidMetadataCheckingStore;
import voldemort.store.logging.LoggingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.rebalancing.RebootstrappingStore;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.store.stats.DataSetStats;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.StoreStatsJmx;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.store.views.ViewStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.ConfigurationException;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Pair;
import voldemort.utils.ReflectUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * The service responsible for managing all storage types
 * 
 * 
 */
@JmxManaged(description = "Start and stop all stores.")
public class StorageService extends AbstractService {

    private static final Logger logger = Logger.getLogger(StorageService.class.getName());

    private final VoldemortConfig voldemortConfig;
    private final StoreRepository storeRepository;
    private final SchedulerService scheduler;
    private final MetadataStore metadata;
    private final Semaphore cleanupPermits;
    private final SocketPool socketPool;
    private final ConcurrentMap<String, StorageConfiguration> storageConfigs;
    private final ClientThreadPool clientThreadPool;
    private final FailureDetector failureDetector;
    private final StoreStats storeStats;

    public StorageService(StoreRepository storeRepository,
                          MetadataStore metadata,
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
                                         config.getSocketBufferSize(),
                                         config.getSocketKeepAlive());

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig(voldemortConfig).setNodes(metadata.getCluster()
                                                                                                                  .getNodes())
                                                                                                .setStoreVerifier(new ServerStoreVerifier(socketPool,
                                                                                                                                          metadata,
                                                                                                                                          config));
        this.failureDetector = create(failureDetectorConfig, config.isJmxEnabled());
        this.storeStats = new StoreStats();
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
        registerEngine(metadata);

        /* Initialize storage configurations */
        for(String configClassName: voldemortConfig.getStorageConfigurations())
            initStorageConfig(configClassName);

        /* Initialize view storage configuration */
        storageConfigs.put(ViewStorageConfiguration.TYPE_NAME,
                           new ViewStorageConfiguration(voldemortConfig,
                                                        metadata.getStoreDefList(),
                                                        storeRepository));

        /* Register slop store */
        if(voldemortConfig.isSlopEnabled()) {
            StorageEngine<ByteArray, byte[], byte[]> slopEngine = getStorageEngine("slop",
                                                                                   voldemortConfig.getSlopStoreType());
            registerEngine(slopEngine);
            storeRepository.setSlopStore(SerializingStorageEngine.wrap(slopEngine,
                                                                       new ByteArraySerializer(),
                                                                       new SlopSerializer(),
                                                                       new IdentitySerializer()));
        }
        List<StoreDefinition> storeDefs = new ArrayList<StoreDefinition>(this.metadata.getStoreDefList());
        logger.info("Initializing stores:");

        // first initialize non-view stores
        for(StoreDefinition def: storeDefs)
            if(!def.isView())
                openStore(def);

        // now that we have all our stores, we can initialize views pointing at
        // those stores
        for(StoreDefinition def: storeDefs)
            if(def.isView())
                openStore(def);

        // enable aggregate jmx statistics
        if(voldemortConfig.isStatTrackingEnabled())
            JmxUtils.registerMbean(new StoreStatsJmx(this.storeStats),
                                   JmxUtils.createObjectName("voldemort.store.stats.aggregate",
                                                             "aggregate-perf"));

        logger.info("All stores initialized.");
    }

    public void openStore(StoreDefinition storeDef) {
        logger.info("Opening store '" + storeDef.getName() + "' (" + storeDef.getType() + ").");
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine(storeDef.getName(),
                                                                           storeDef.getType());
        registerEngine(engine);

        if(voldemortConfig.isServerRoutingEnabled())
            registerNodeStores(storeDef, metadata.getCluster(), voldemortConfig.getNodeId());

        if(storeDef.hasRetentionPeriod())
            scheduleCleanupJob(storeDef, engine);
    }

    /**
     * Register the given engine with the storage repository
     * 
     * @param engine Register the storage engine
     */
    public void registerEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        Cluster cluster = this.metadata.getCluster();
        storeRepository.addStorageEngine(engine);

        /* Now add any store wrappers that are enabled */
        Store<ByteArray, byte[], byte[]> store = engine;
        if(voldemortConfig.isVerboseLoggingEnabled())
            store = new LoggingStore<ByteArray, byte[], byte[]>(store,
                                                                cluster.getName(),
                                                                SystemTime.INSTANCE);

        if(voldemortConfig.isRedirectRoutingEnabled())
            store = new RedirectingStore(store,
                                         metadata,
                                         storeRepository,
                                         failureDetector,
                                         socketPool);

        if(voldemortConfig.isMetadataCheckingEnabled())
            store = new InvalidMetadataCheckingStore(metadata.getNodeId(), store, metadata);

        if(voldemortConfig.isStatTrackingEnabled()) {
            StatTrackingStore<ByteArray, byte[], byte[]> statStore = new StatTrackingStore<ByteArray, byte[], byte[]>(store,
                                                                                                                      this.storeStats);
            store = statStore;
            if(voldemortConfig.isJmxEnabled()) {

                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                ObjectName name = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                            store.getName());

                synchronized(mbeanServer) {
                    if(mbeanServer.isRegistered(name))
                        JmxUtils.unregisterMbean(mbeanServer, name);
                    JmxUtils.registerMbean(mbeanServer,
                                           JmxUtils.createModelMBean(new StoreStatsJmx(statStore.getStats())),
                                           name);
                }
            }
        }

        storeRepository.addLocalStore(store);
    }

    /**
     * For server side routing create NodeStore (socketstore) and pass it on to
     * a {@link RebootstrappingStore}.
     * <p>
     * 
     * The {@link RebootstrappingStore} handles invalid-metadata exceptions
     * introduced due to changes in cluster.xml at different nodes.
     * 
     * @param def
     * @param cluster
     * @param localNode
     */
    public void registerNodeStores(StoreDefinition def, Cluster cluster, int localNode) {
        Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>(cluster.getNumberOfNodes());

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> store = getNodeStore(def.getName(), node, localNode);
            this.storeRepository.addNodeStore(node.getId(), store);
            nodeStores.put(node.getId(), store);
        }

        Store<ByteArray, byte[], byte[]> routedStore = new RoutedStore(def.getName(),
                                                                       nodeStores,
                                                                       metadata.getCluster(),
                                                                       def,
                                                                       true,
                                                                       this.clientThreadPool,
                                                                       voldemortConfig.getRoutingTimeoutMs(),
                                                                       failureDetector,
                                                                       SystemTime.INSTANCE);

        routedStore = new RebootstrappingStore(metadata,
                                               storeRepository,
                                               voldemortConfig,
                                               socketPool,
                                               (RoutedStore) routedStore);

        routedStore = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                 new VectorClockInconsistencyResolver<byte[]>());
        this.storeRepository.addRoutedStore(routedStore);
    }

    private Store<ByteArray, byte[], byte[]> getNodeStore(String storeName, Node node, int localNode) {
        Store<ByteArray, byte[], byte[]> store;
        if(node.getId() == localNode) {
            store = this.storeRepository.getLocalStore(storeName);
        } else {
            store = createNodeStore(storeName, node);
        }
        return store;
    }

    private Store<ByteArray, byte[], byte[]> createNodeStore(String storeName, Node node) {
        return new SocketStore(storeName,
                               new SocketDestination(node.getHost(),
                                                     node.getSocketPort(),
                                                     voldemortConfig.getRequestFormatType()),
                               socketPool,
                               false);
    }

    /**
     * Schedule a data retention cleanup job for the given store
     * 
     * @param storeDef The store definition
     * @param engine The storage engine to do cleanup on
     */
    private void scheduleCleanupJob(StoreDefinition storeDef,
                                    StorageEngine<ByteArray, byte[], byte[]> engine) {
        // Schedule data retention cleanup job starting next day.
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        cal.set(Calendar.HOUR_OF_DAY, voldemortConfig.getRetentionCleanupFirstStartTimeInHour());
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

        Runnable cleanupJob = new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                            cleanupPermits,
                                                                            storeDef.getRetentionDays()
                                                                                    * Time.MS_PER_DAY,
                                                                            SystemTime.INSTANCE,
                                                                            throttler);

        this.scheduler.schedule(cleanupJob,
                                startTime,
                                voldemortConfig.getRetentionCleanupScheduledPeriodInHour()
                                        * Time.MS_PER_HOUR);

    }

    private StorageEngine<ByteArray, byte[], byte[]> getStorageEngine(String name, String type) {
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
        for(Store<ByteArray, byte[], byte[]> store: this.storeRepository.getAllRoutedStores()) {
            logger.info("Closing routed store for " + store.getName());
            try {
                store.close();
            } catch(Exception e) {
                logger.error(e);
                lastException = e;
            }
        }
        /* This will also close the storage engines */
        for(Store<ByteArray, byte[], byte[]> store: this.storeRepository.getAllStorageEngines()) {
            logger.info("Closing storage engine for " + store.getName());
            try {
                store.close();
            } catch(Exception e) {
                logger.error(e);
                lastException = e;
            }
        }
        logger.info("All stores closed.");

        /* Close slop store if necessary */
        if(this.storeRepository.hasSlopStore()) {
            try {
                this.storeRepository.getSlopStore().close();
            } catch(Exception e) {
                logger.error(e);
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
                logger.error(e);
                lastException = e;
            }
        }

        this.clientThreadPool.shutdownNow();
        logger.info("Closed client threadpool.");

        if(this.failureDetector != null) {
            try {
                this.failureDetector.destroy();
            } catch(Exception e) {
                lastException = e;
            }
        }

        logger.info("Closed failure detector.");

        /* If there is an exception, throw it */
        if(lastException instanceof VoldemortException)
            throw (VoldemortException) lastException;
        else if(lastException != null)
            throw new VoldemortException(lastException);
    }

    public MetadataStore getMetadataStore() {
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
            StorageEngine<ByteArray, byte[], byte[]> engine = storeRepository.getStorageEngine(storeName);

            if(null != engine) {
                if(storeDef.hasRetentionPeriod()) {
                    ExecutorService executor = Executors.newFixedThreadPool(1);
                    try {
                        if(cleanupPermits.availablePermits() >= 1) {

                            executor.execute(new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
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

    @JmxOperation(description = "Print stats on a given store", impact = MBeanOperationInfo.ACTION)
    public void logStoreStats(final String storeName) {
        this.scheduler.scheduleNow(new Runnable() {

            public void run() {
                StorageEngine<ByteArray, byte[], byte[]> store = storeRepository.getStorageEngine(storeName);
                if(store == null) {
                    logger.error("Invalid store name '" + storeName + "'.");
                    return;
                }
                logger.info("Data statistics for store '" + store.getName() + "':\n\n"
                            + calculateStats(store) + "\n\n");
            }
        });

    }

    @JmxOperation(description = "Print stats on a given store", impact = MBeanOperationInfo.ACTION)
    public void logStoreStats() {
        this.scheduler.scheduleNow(new Runnable() {

            public void run() {
                try {
                    DataSetStats totals = new DataSetStats();
                    List<String> names = new ArrayList<String>();
                    List<DataSetStats> stats = new ArrayList<DataSetStats>();
                    for(StorageEngine<ByteArray, byte[], byte[]> store: storeRepository.getAllStorageEngines()) {
                        if(store instanceof ReadOnlyStorageEngine
                           || store instanceof ViewStorageEngine || store instanceof MetadataStore)
                            continue;
                        logger.info(store.getClass());
                        logger.info("Calculating stats for '" + store.getName() + "'...");
                        DataSetStats curr = calculateStats(store);
                        names.add(store.getName());
                        stats.add(curr);
                        totals.add(curr);
                    }
                    for(int i = 0; i < names.size(); i++)
                        logger.info("\n\nData statistics for store '" + names.get(i) + "':\n"
                                    + stats.get(i) + "\n\n");
                    logger.info("Totals: \n " + totals + "\n\n");
                } catch(Exception e) {
                    logger.error("Error in thread: ", e);
                }
            }
        });

    }

    private DataSetStats calculateStats(StorageEngine<ByteArray, byte[], byte[]> store) {
        DataSetStats stats = new DataSetStats();
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter = store.entries();
        try {
            int count = 0;
            while(iter.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> pair = iter.next();
                VectorClock clock = (VectorClock) pair.getSecond().getVersion();
                stats.countEntry(pair.getFirst().length(), pair.getSecond().getValue().length
                                                           + clock.sizeInBytes());
                if(count % 10000 == 0)
                    logger.debug("Processing key " + count);
                count++;
            }
        } finally {
            iter.close();
        }
        return stats;
    }

    public SocketPool getStorageSocketPool() {
        return socketPool;
    }

}
