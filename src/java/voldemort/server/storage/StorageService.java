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

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.ClientThreadPool;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.ServerStoreVerifier;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.avro.versioned.SchemaEvolutionValidator;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.slop.BlockingSlopPusherJob;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.configuration.FileBackedCachingStorageConfiguration;
import voldemort.store.invalidmetadata.InvalidMetadataCheckingStore;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStoreListener;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.rebalancing.RebootstrappingStore;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.store.retention.RetentionEnforcingStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.routed.RoutedStoreFactory;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.stats.DataSetStats;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.StoreStatsJmx;
import voldemort.store.system.SystemStoreConstants;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.store.views.ViewStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.ConfigurationException;
import voldemort.utils.DynamicThrottleLimit;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Pair;
import voldemort.utils.ReflectUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;
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
    public static final String VERSIONS_METADATA_STORE = "metadata-versions";
    public static final String CLUSTER_VERSION_KEY = "cluster.xml";
    public static final String STORES_VERSION_KEY = "stores.xml";

    private final VoldemortConfig voldemortConfig;
    private final StoreRepository storeRepository;
    private final SchedulerService scheduler;
    private final MetadataStore metadata;

    /* Dynamic throttle limit required for read-only stores */
    private final DynamicThrottleLimit dynThrottleLimit;

    // Common permit shared by all job which do a disk scan
    private final ScanPermitWrapper scanPermitWrapper;
    private final SocketStoreFactory storeFactory;
    private final ConcurrentMap<String, StorageConfiguration> storageConfigs;
    private final ClientThreadPool clientThreadPool;
    private final FailureDetector failureDetector;
    private final StoreStats storeStats;
    private final RoutedStoreFactory routedStoreFactory;

    public StorageService(StoreRepository storeRepository,
                          MetadataStore metadata,
                          SchedulerService scheduler,
                          VoldemortConfig config) {
        super(ServiceType.STORAGE);
        this.voldemortConfig = config;
        this.scheduler = scheduler;
        this.storeRepository = storeRepository;
        this.metadata = metadata;
        this.scanPermitWrapper = new ScanPermitWrapper(voldemortConfig.getNumScanPermits());
        this.storageConfigs = new ConcurrentHashMap<String, StorageConfiguration>();
        this.clientThreadPool = new ClientThreadPool(config.getClientMaxThreads(),
                                                     config.getClientThreadIdleMs(),
                                                     config.getClientMaxQueuedRequests());
        this.storeFactory = new ClientRequestExecutorPool(config.getClientSelectors(),
                                                          config.getClientMaxConnectionsPerNode(),
                                                          config.getClientConnectionTimeoutMs(),
                                                          config.getSocketTimeoutMs(),
                                                          config.getSocketBufferSize(),
                                                          config.getSocketKeepAlive());

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig(voldemortConfig).setCluster(metadata.getCluster())
                                                                                                .setStoreVerifier(new ServerStoreVerifier(storeFactory,
                                                                                                                                          metadata,
                                                                                                                                          config));
        this.failureDetector = create(failureDetectorConfig, config.isJmxEnabled());
        this.storeStats = new StoreStats();
        this.routedStoreFactory = new RoutedStoreFactory(voldemortConfig.isPipelineRoutedStoreEnabled(),
                                                         this.clientThreadPool,
                                                         voldemortConfig.getTimeoutConfig());

        /*
         * Initialize the dynamic throttle limit based on the per node limit
         * config only if read-only engine is being used.
         */
        if(this.voldemortConfig.getStorageConfigurations()
                               .contains(ReadOnlyStorageConfiguration.class.getName())) {
            long rate = this.voldemortConfig.getReadOnlyFetcherMaxBytesPerSecond();
            this.dynThrottleLimit = new DynamicThrottleLimit(rate);
        } else
            this.dynThrottleLimit = null;
    }

    private void initStorageConfig(String configClassName) {
        // add the configurations of the storage engines needed by user stores
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

        // now, add the configurations of the storage engines needed by system
        // stores, if not yet exist
        initSystemStorageConfig();
    }

    private void initSystemStorageConfig() {
        // add InMemoryStorage used by voldsys$_client_registry
        if(!storageConfigs.containsKey(InMemoryStorageConfiguration.TYPE_NAME)) {
            storageConfigs.put(InMemoryStorageConfiguration.TYPE_NAME,
                               new InMemoryStorageConfiguration());
        }

        // add FileStorage config here
        if(!storageConfigs.containsKey(FileBackedCachingStorageConfiguration.TYPE_NAME)) {
            storageConfigs.put(FileBackedCachingStorageConfiguration.TYPE_NAME,
                               new FileBackedCachingStorageConfiguration(voldemortConfig));
        }
    }

    private void initSystemStores() {
        List<StoreDefinition> storesDefs = SystemStoreConstants.getAllSystemStoreDefs();

        // TODO: replication factor can't now be determined unless the
        // cluster.xml is made available to the server at runtime. So we need to
        // set them here after load they are loaded
        updateRepFactor(storesDefs);

        for(StoreDefinition storeDef: storesDefs) {
            openSystemStore(storeDef);
        }
    }

    private void updateRepFactor(List<StoreDefinition> storesDefs) {
        // need impl
    }

    @Override
    protected void startInner() {
        registerInternalEngine(metadata, false, "metadata");

        /* Initialize storage configurations */
        for(String configClassName: voldemortConfig.getStorageConfigurations())
            initStorageConfig(configClassName);

        /* Initialize view storage configuration */
        storageConfigs.put(ViewStorageConfiguration.TYPE_NAME,
                           new ViewStorageConfiguration(voldemortConfig,
                                                        metadata.getStoreDefList(),
                                                        storeRepository));

        /* Initialize system stores */
        initSystemStores();

        /* Register slop store */
        if(voldemortConfig.isSlopEnabled()) {

            logger.info("Initializing the slop store using " + voldemortConfig.getSlopStoreType());
            StorageConfiguration config = storageConfigs.get(voldemortConfig.getSlopStoreType());
            if(config == null)
                throw new ConfigurationException("Attempt to open store "
                                                 + SlopStorageEngine.SLOP_STORE_NAME + " but "
                                                 + voldemortConfig.getSlopStoreType()
                                                 + " storage engine has not been enabled.");

            // make a dummy store definition object
            StoreDefinition slopStoreDefinition = new StoreDefinition(SlopStorageEngine.SLOP_STORE_NAME,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      RoutingStrategyType.CONSISTENT_STRATEGY,
                                                                      0,
                                                                      null,
                                                                      0,
                                                                      null,
                                                                      0,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      0);
            SlopStorageEngine slopEngine = new SlopStorageEngine(config.getStore(slopStoreDefinition,
                                                                                 new RoutingStrategyFactory().updateRoutingStrategy(slopStoreDefinition,
                                                                                                                                    metadata.getCluster())),
                                                                 metadata.getCluster());
            registerInternalEngine(slopEngine, false, "slop");
            storeRepository.setSlopStore(slopEngine);

            if(voldemortConfig.isSlopPusherJobEnabled()) {
                // Now initialize the pusher job after some time
                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.SECOND,
                        (int) (voldemortConfig.getSlopFrequencyMs() / Time.MS_PER_SECOND));
                Date nextRun = cal.getTime();
                logger.info("Initializing slop pusher job type " + voldemortConfig.getPusherType()
                            + " at " + nextRun);

                scheduler.schedule("slop",
                                   (voldemortConfig.getPusherType()
                                                   .compareTo(BlockingSlopPusherJob.TYPE_NAME) == 0) ? new BlockingSlopPusherJob(storeRepository,
                                                                                                                                 metadata,
                                                                                                                                 failureDetector,
                                                                                                                                 voldemortConfig,
                                                                                                                                 scanPermitWrapper)
                                                                                                    : new StreamingSlopPusherJob(storeRepository,
                                                                                                                                 metadata,
                                                                                                                                 failureDetector,
                                                                                                                                 voldemortConfig,
                                                                                                                                 scanPermitWrapper),
                                   nextRun,
                                   voldemortConfig.getSlopFrequencyMs());
            }

            // Create a repair job object and register it with Store repository
            if(voldemortConfig.isRepairEnabled()) {
                logger.info("Initializing repair job.");
                RepairJob job = new RepairJob(storeRepository, metadata, scanPermitWrapper);
                JmxUtils.registerMbean(job, JmxUtils.createObjectName(job.getClass()));
                storeRepository.registerRepairJob(job);
            }
        }

        List<StoreDefinition> storeDefs = new ArrayList<StoreDefinition>(this.metadata.getStoreDefList());
        logger.info("Initializing stores:");

        logger.info("Validating schemas:");
        String AVRO_GENERIC_VERSIONED_TYPE_NAME = "avro-generic-versioned";

        for(StoreDefinition storeDef: storeDefs) {
            SerializerDefinition keySerDef = storeDef.getKeySerializer();
            SerializerDefinition valueSerDef = storeDef.getValueSerializer();

            if(keySerDef.getName().equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)) {

                SchemaEvolutionValidator.checkSchemaCompatibility(keySerDef);

            }

            if(valueSerDef.getName().equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)) {

                SchemaEvolutionValidator.checkSchemaCompatibility(valueSerDef);

            }
        }
        // first initialize non-view stores
        for(StoreDefinition def: storeDefs)
            if(!def.isView())
                openStore(def);

        // now that we have all our stores, we can initialize views pointing at
        // those stores
        for(StoreDefinition def: storeDefs) {
            if(def.isView())
                openStore(def);
        }

        initializeMetadataVersions(storeDefs);

        // enable aggregate jmx statistics
        if(voldemortConfig.isStatTrackingEnabled())
            if(this.voldemortConfig.isEnableJmxClusterName())
                JmxUtils.registerMbean(new StoreStatsJmx(this.storeStats),
                                       JmxUtils.createObjectName(metadata.getCluster().getName()
                                                                         + ".voldemort.store.stats.aggregate",
                                                                 "aggregate-perf"));
            else
                JmxUtils.registerMbean(new StoreStatsJmx(this.storeStats),
                                       JmxUtils.createObjectName("voldemort.store.stats.aggregate",
                                                                 "aggregate-perf"));

        logger.info("All stores initialized.");
    }

    protected void initializeMetadataVersions(List<StoreDefinition> storeDefs) {
        Store<ByteArray, byte[], byte[]> versionStore = storeRepository.getLocalStore(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        Properties props = new Properties();

        try {
            ByteArray metadataVersionsKey = new ByteArray(VERSIONS_METADATA_STORE.getBytes());
            List<Versioned<byte[]>> versionList = versionStore.get(metadataVersionsKey, null);
            VectorClock newClock = null;

            if(versionList != null && versionList.size() > 0) {
                byte[] versionsByteArray = versionList.get(0).getValue();
                if(versionsByteArray != null) {
                    props.load(new ByteArrayInputStream(versionsByteArray));
                }
                newClock = (VectorClock) versionList.get(0).getVersion();
                newClock = newClock.incremented(0, System.currentTimeMillis());
            } else {
                newClock = new VectorClock();
            }

            // Check if version exists for cluster.xml
            if(!props.containsKey(CLUSTER_VERSION_KEY)) {
                props.setProperty(CLUSTER_VERSION_KEY, "0");
            }

            // Check if version exists for stores.xml
            if(!props.containsKey(STORES_VERSION_KEY)) {
                props.setProperty(STORES_VERSION_KEY, "0");
            }

            // Check if version exists for each store
            for(StoreDefinition def: storeDefs) {
                if(!props.containsKey(def.getName())) {
                    props.setProperty(def.getName(), "0");
                }
            }

            StringBuilder finalVersionList = new StringBuilder();
            for(String propName: props.stringPropertyNames()) {
                finalVersionList.append(propName + "=" + props.getProperty(propName) + "\n");
            }
            versionStore.put(metadataVersionsKey,
                             new Versioned<byte[]>(finalVersionList.toString().getBytes(), newClock),
                             null);

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void openSystemStore(StoreDefinition storeDef) {

        logger.info("Opening system store '" + storeDef.getName() + "' (" + storeDef.getType()
                    + ").");

        StorageConfiguration config = storageConfigs.get(storeDef.getType());
        if(config == null)
            throw new ConfigurationException("Attempt to open system store " + storeDef.getName()
                                             + " but " + storeDef.getType()
                                             + " storage engine has not been enabled.");

        final StorageEngine<ByteArray, byte[], byte[]> engine = config.getStore(storeDef, null);

        // Noted that there is no read-only processing as for user stores.

        // openStore() should have atomic semantics
        try {
            registerSystemEngine(engine);

            if(voldemortConfig.isServerRoutingEnabled())
                registerNodeStores(storeDef, metadata.getCluster(), voldemortConfig.getNodeId());

            if(storeDef.hasRetentionPeriod())
                scheduleCleanupJob(storeDef, engine);
        } catch(Exception e) {
            unregisterSystemEngine(engine);
            throw new VoldemortException(e);
        }
    }

    public void registerSystemEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {

        Cluster cluster = this.metadata.getCluster();
        storeRepository.addStorageEngine(engine);

        /* Now add any store wrappers that are enabled */
        Store<ByteArray, byte[], byte[]> store = engine;

        if(voldemortConfig.isVerboseLoggingEnabled())
            store = new LoggingStore<ByteArray, byte[], byte[]>(store,
                                                                cluster.getName(),
                                                                SystemTime.INSTANCE);

        if(voldemortConfig.isMetadataCheckingEnabled())
            store = new InvalidMetadataCheckingStore(metadata.getNodeId(), store, metadata);

        if(voldemortConfig.isStatTrackingEnabled()) {
            StatTrackingStore statStore = new StatTrackingStore(store, this.storeStats);
            store = statStore;
            if(voldemortConfig.isJmxEnabled()) {

                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                ObjectName name = null;
                if(this.voldemortConfig.isEnableJmxClusterName())
                    name = JmxUtils.createObjectName(metadata.getCluster().getName()
                                                             + "."
                                                             + JmxUtils.getPackageName(store.getClass()),
                                                     store.getName());
                else
                    name = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
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

    public void unregisterSystemEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        String storeName = engine.getName();
        Store<ByteArray, byte[], byte[]> store = storeRepository.removeLocalStore(storeName);

        if(store != null) {
            if(voldemortConfig.isJmxEnabled()) {
                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

                if(voldemortConfig.isEnableRebalanceService()) {

                    ObjectName name = null;
                    if(this.voldemortConfig.isEnableJmxClusterName())
                        name = JmxUtils.createObjectName(metadata.getCluster().getName()
                                                                 + "."
                                                                 + JmxUtils.getPackageName(RedirectingStore.class),
                                                         store.getName());
                    else
                        name = JmxUtils.createObjectName(JmxUtils.getPackageName(RedirectingStore.class),
                                                         store.getName());

                    synchronized(mbeanServer) {
                        if(mbeanServer.isRegistered(name))
                            JmxUtils.unregisterMbean(mbeanServer, name);
                    }

                }

                if(voldemortConfig.isStatTrackingEnabled()) {
                    ObjectName name = null;
                    if(this.voldemortConfig.isEnableJmxClusterName())
                        name = JmxUtils.createObjectName(metadata.getCluster().getName()
                                                                 + "."
                                                                 + JmxUtils.getPackageName(store.getClass()),
                                                         store.getName());
                    else
                        name = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                         store.getName());

                    synchronized(mbeanServer) {
                        if(mbeanServer.isRegistered(name))
                            JmxUtils.unregisterMbean(mbeanServer, name);
                    }

                }
            }
            if(voldemortConfig.isServerRoutingEnabled()) {
                this.storeRepository.removeRoutedStore(storeName);
                for(Node node: metadata.getCluster().getNodes())
                    this.storeRepository.removeNodeStore(storeName, node.getId());
            }
        }

        storeRepository.removeStorageEngine(storeName);
        // engine.truncate(); why truncate here when unregister? Isn't close
        // good enough?
        engine.close();
    }

    public void updateStore(StoreDefinition storeDef) {
        logger.info("Updating store '" + storeDef.getName() + "' (" + storeDef.getType() + ").");
        StorageConfiguration config = storageConfigs.get(storeDef.getType());
        if(config == null)
            throw new ConfigurationException("Attempt to open store " + storeDef.getName()
                                             + " but " + storeDef.getType()
                                             + " storage engine has not been enabled.");
        config.update(storeDef);
    }

    public void openStore(StoreDefinition storeDef) {

        logger.info("Opening store '" + storeDef.getName() + "' (" + storeDef.getType() + ").");

        StorageConfiguration config = storageConfigs.get(storeDef.getType());
        if(config == null)
            throw new ConfigurationException("Attempt to open store " + storeDef.getName()
                                             + " but " + storeDef.getType()
                                             + " storage engine has not been enabled.");

        boolean isReadOnly = storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
        final RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                   metadata.getCluster());

        final StorageEngine<ByteArray, byte[], byte[]> engine = config.getStore(storeDef,
                                                                                routingStrategy);
        // Update the routing strategy + add listener to metadata
        if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
            metadata.addMetadataStoreListener(storeDef.getName(), new MetadataStoreListener() {

                public void updateRoutingStrategy(RoutingStrategy updatedRoutingStrategy) {
                    ((ReadOnlyStorageEngine) engine).setRoutingStrategy(updatedRoutingStrategy);
                }

                public void updateStoreDefinition(StoreDefinition storeDef) {
                    return;
                }
            });
        }

        // openStore() should have atomic semantics
        try {
            registerEngine(engine, isReadOnly, storeDef.getType(), storeDef);

            if(voldemortConfig.isServerRoutingEnabled())
                registerNodeStores(storeDef, metadata.getCluster(), voldemortConfig.getNodeId());

            if(storeDef.hasRetentionPeriod())
                scheduleCleanupJob(storeDef, engine);
        } catch(Exception e) {
            removeEngine(engine, isReadOnly, storeDef.getType(), false);
            throw new VoldemortException(e);
        }
    }

    /**
     * Unregister and remove the engine from the storage repository. This is
     * called during deletion of stores and if there are exceptions
     * adding/opening stores
     * 
     * @param engine The actual engine to remove
     * @param isReadOnly Is this read-only?
     * @param storeType The storage type of the store
     * @param truncate Should the store be truncated?
     */
    public void removeEngine(StorageEngine<ByteArray, byte[], byte[]> engine,
                             boolean isReadOnly,
                             String storeType,
                             boolean truncate) {
        String storeName = engine.getName();
        Store<ByteArray, byte[], byte[]> store = storeRepository.removeLocalStore(storeName);

        boolean isSlop = storeType.compareTo("slop") == 0;
        boolean isView = storeType.compareTo(ViewStorageConfiguration.TYPE_NAME) == 0;
        boolean isMetadata = storeName.compareTo(MetadataStore.METADATA_STORE_NAME) == 0;

        if(store != null) {
            if(voldemortConfig.isJmxEnabled()) {
                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

                if(!isSlop && voldemortConfig.isEnableRebalanceService() && !isReadOnly
                   && !isMetadata && !isView) {

                    ObjectName name = null;
                    if(this.voldemortConfig.isEnableJmxClusterName())
                        name = JmxUtils.createObjectName(metadata.getCluster().getName()
                                                                 + "."
                                                                 + JmxUtils.getPackageName(RedirectingStore.class),
                                                         store.getName());
                    else
                        name = JmxUtils.createObjectName(JmxUtils.getPackageName(RedirectingStore.class),
                                                         store.getName());

                    synchronized(mbeanServer) {
                        if(mbeanServer.isRegistered(name))
                            JmxUtils.unregisterMbean(mbeanServer, name);
                    }

                }

                if(voldemortConfig.isStatTrackingEnabled()) {
                    ObjectName name = null;
                    if(this.voldemortConfig.isEnableJmxClusterName())
                        name = JmxUtils.createObjectName(metadata.getCluster().getName()
                                                                 + "."
                                                                 + JmxUtils.getPackageName(store.getClass()),
                                                         store.getName());
                    else
                        name = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                         store.getName());

                    synchronized(mbeanServer) {
                        if(mbeanServer.isRegistered(name))
                            JmxUtils.unregisterMbean(mbeanServer, name);
                    }

                }
            }
            if(voldemortConfig.isServerRoutingEnabled() && !isSlop) {
                this.storeRepository.removeRoutedStore(storeName);
                for(Node node: metadata.getCluster().getNodes())
                    this.storeRepository.removeNodeStore(storeName, node.getId());
            }
        }

        storeRepository.removeStorageEngine(storeName);
        if(truncate)
            engine.truncate();
        engine.close();
    }

    /**
     * Register the given internal engine (slop and metadata) with the storage
     * repository
     * 
     * @param engine Register the storage engine
     * @param isReadOnly Boolean indicating if this store is read-only
     * @param storeType The type of the store
     */
    public void registerInternalEngine(StorageEngine<ByteArray, byte[], byte[]> engine,
                                       boolean isReadOnly,
                                       String storeType) {
        registerEngine(engine, isReadOnly, storeType, null);
    }

    /**
     * Register the given engine with the storage repository
     * 
     * @param engine Register the storage engine
     * @param isReadOnly Boolean indicating if this store is read-only
     * @param storeType The type of the store
     * @param storeDef store definition for the store to be registered
     */
    public void registerEngine(StorageEngine<ByteArray, byte[], byte[]> engine,
                               boolean isReadOnly,
                               String storeType,
                               StoreDefinition storeDef) {
        Cluster cluster = this.metadata.getCluster();
        storeRepository.addStorageEngine(engine);

        /* Now add any store wrappers that are enabled */
        Store<ByteArray, byte[], byte[]> store = engine;

        boolean isMetadata = store.getName().compareTo(MetadataStore.METADATA_STORE_NAME) == 0;
        boolean isSlop = storeType.compareTo("slop") == 0;
        boolean isView = storeType.compareTo(ViewStorageConfiguration.TYPE_NAME) == 0;

        if(voldemortConfig.isVerboseLoggingEnabled())
            store = new LoggingStore<ByteArray, byte[], byte[]>(store,
                                                                cluster.getName(),
                                                                SystemTime.INSTANCE);
        if(!isSlop) {
            if(!isReadOnly && !isMetadata && !isView) {
                // wrap store to enforce retention policy
                if(voldemortConfig.isEnforceRetentionPolicyOnRead() && storeDef != null) {
                    RetentionEnforcingStore retentionEnforcingStore = new RetentionEnforcingStore(store,
                                                                                                  storeDef,
                                                                                                  voldemortConfig.isDeleteExpiredValuesOnRead(),
                                                                                                  SystemTime.INSTANCE);
                    metadata.addMetadataStoreListener(store.getName(), retentionEnforcingStore);
                    store = retentionEnforcingStore;
                }

                if(voldemortConfig.isEnableRebalanceService()) {
                    store = new RedirectingStore(store,
                                                 metadata,
                                                 storeRepository,
                                                 failureDetector,
                                                 storeFactory);
                    if(voldemortConfig.isJmxEnabled()) {
                        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                        ObjectName name = null;
                        if(this.voldemortConfig.isEnableJmxClusterName())
                            name = JmxUtils.createObjectName(cluster.getName()
                                                                     + "."
                                                                     + JmxUtils.getPackageName(RedirectingStore.class),
                                                             store.getName());
                        else
                            name = JmxUtils.createObjectName(JmxUtils.getPackageName(RedirectingStore.class),
                                                             store.getName());

                        synchronized(mbeanServer) {
                            if(mbeanServer.isRegistered(name))
                                JmxUtils.unregisterMbean(mbeanServer, name);

                            JmxUtils.registerMbean(mbeanServer,
                                                   JmxUtils.createModelMBean(store),
                                                   name);
                        }

                    }
                }
            }

            if(voldemortConfig.isMetadataCheckingEnabled() && !isMetadata)
                store = new InvalidMetadataCheckingStore(metadata.getNodeId(), store, metadata);
        }

        if(voldemortConfig.isStatTrackingEnabled()) {
            StatTrackingStore statStore = new StatTrackingStore(store, this.storeStats);
            store = statStore;
            if(voldemortConfig.isJmxEnabled()) {

                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                ObjectName name = null;
                if(this.voldemortConfig.isEnableJmxClusterName())
                    name = JmxUtils.createObjectName(metadata.getCluster().getName()
                                                             + "."
                                                             + JmxUtils.getPackageName(store.getClass()),
                                                     store.getName());
                else
                    name = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
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
        Map<Integer, NonblockingStore> nonblockingStores = new HashMap<Integer, NonblockingStore>(cluster.getNumberOfNodes());
        try {
            for(Node node: cluster.getNodes()) {
                Store<ByteArray, byte[], byte[]> store = getNodeStore(def.getName(),
                                                                      node,
                                                                      localNode);
                this.storeRepository.addNodeStore(node.getId(), store);
                nodeStores.put(node.getId(), store);

                NonblockingStore nonblockingStore = routedStoreFactory.toNonblockingStore(store);
                nonblockingStores.put(node.getId(), nonblockingStore);
            }

            Store<ByteArray, byte[], byte[]> store = routedStoreFactory.create(cluster,
                                                                               def,
                                                                               nodeStores,
                                                                               nonblockingStores,
                                                                               null,
                                                                               null,
                                                                               true,
                                                                               cluster.getNodeById(localNode)
                                                                                      .getZoneId(),
                                                                               failureDetector);

            store = new RebootstrappingStore(metadata,
                                             storeRepository,
                                             voldemortConfig,
                                             (RoutedStore) store,
                                             storeFactory);

            store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(store,
                                                                               new VectorClockInconsistencyResolver<byte[]>());
            this.storeRepository.addRoutedStore(store);
        } catch(Exception e) {
            // Roll back
            for(Node node: cluster.getNodes())
                this.storeRepository.removeNodeStore(def.getName(), node.getId());
            throw new VoldemortException(e);
        }
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
        return storeFactory.create(storeName,
                                   node.getHost(),
                                   node.getSocketPort(),
                                   voldemortConfig.getRequestFormatType(),
                                   RequestRoutingType.NORMAL);
    }

    /**
     * Schedule a data retention cleanup job for the given store
     * 
     * @param storeDef The store definition
     * @param engine The storage engine to do cleanup on
     */
    private void scheduleCleanupJob(StoreDefinition storeDef,
                                    StorageEngine<ByteArray, byte[], byte[]> engine) {
        // Compute the start time of the job, based on current time
        GregorianCalendar cal = Utils.getCalendarForNextRun(new GregorianCalendar(),
                                                            voldemortConfig.getRetentionCleanupFirstStartDayOfWeek(),
                                                            voldemortConfig.getRetentionCleanupFirstStartTimeInHour());

        // allow only one cleanup job at a time
        Date startTime = cal.getTime();

        int maxReadRate = storeDef.hasRetentionScanThrottleRate() ? storeDef.getRetentionScanThrottleRate()
                                                                 : Integer.MAX_VALUE;

        logger.info("Scheduling data retention cleanup job for store '" + storeDef.getName()
                    + "' at " + startTime + " with retention scan throttle rate:" + maxReadRate
                    + " Entries/second.");

        EventThrottler throttler = new EventThrottler(maxReadRate);

        Runnable cleanupJob = new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                            scanPermitWrapper,
                                                                            storeDef.getRetentionDays()
                                                                                    * Time.MS_PER_DAY,
                                                                            SystemTime.INSTANCE,
                                                                            throttler);
        if(voldemortConfig.isJmxEnabled()) {
            JmxUtils.registerMbean("DataCleanupJob-" + engine.getName(), cleanupJob);
        }

        long retentionFreqHours = storeDef.hasRetentionFrequencyDays() ? (storeDef.getRetentionFrequencyDays() * Time.HOURS_PER_DAY)
                                                                      : voldemortConfig.getRetentionCleanupScheduledPeriodInHour();

        this.scheduler.schedule("cleanup-" + storeDef.getName(),
                                cleanupJob,
                                startTime,
                                retentionFreqHours * Time.MS_PER_HOUR,
                                voldemortConfig.getRetentionCleanupPinStartTime());
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

        this.clientThreadPool.shutdown();

        try {
            if(!this.clientThreadPool.awaitTermination(10, TimeUnit.SECONDS))
                this.clientThreadPool.shutdownNow();
        } catch(InterruptedException e) {
            // okay, fine, playing nice didn't work
            this.clientThreadPool.shutdownNow();
        }

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
                        if(scanPermitWrapper.availablePermits() >= 1) {

                            executor.execute(new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                                           scanPermitWrapper,
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

    public SocketStoreFactory getSocketStoreFactory() {
        return storeFactory;
    }

    public DynamicThrottleLimit getDynThrottleLimit() {
        return dynThrottleLimit;
    }

    @JmxGetter(name = "getScanPermitOwners", description = "Returns class names of services holding the scan permit")
    public List<String> getPermitOwners() {
        return this.scanPermitWrapper.getPermitOwners();
    }

    @JmxGetter(name = "numGrantedScanPermits", description = "Returns number of scan permits granted at the moment")
    public long getGrantedPermits() {
        return this.scanPermitWrapper.getGrantedPermits();
    }

    @JmxGetter(name = "numEntriesScanned", description = "Returns number of entries scanned since last call")
    public long getEntriesScanned() {
        return this.scanPermitWrapper.getEntriesScanned();
    }
}
