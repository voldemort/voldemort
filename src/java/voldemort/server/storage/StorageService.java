package voldemort.server.storage;

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
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.AbstractService;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.filesystem.FilesystemStorageEngine;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.CacheStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.readonly.RandomAccessFileStorageConfiguration;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopDetectingStore;
import voldemort.store.stats.StatTrackingStore;
import voldemort.utils.ConfigurationException;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.xml.StoreDefinitionsMapper;

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
    private final ConcurrentMap<String, Store<byte[], byte[]>> localStoreMap;
    private final Map<String, StorageEngine<byte[], byte[]>> rawEngines;
    private final ConcurrentMap<StorageEngineType, StorageConfiguration> storageConfigurations;
    private final StoreDefinitionsMapper storeMapper;
    private final SchedulerService scheduler;
    private MetadataStore metadataStore;
    private Store<byte[], Slop> slopStore;

    public StorageService(String name,
                          ConcurrentMap<String, Store<byte[], byte[]>> storeMap,
                          SchedulerService scheduler,
                          VoldemortConfig config) {
        super(name);
        this.voldemortConfig = config;
        this.storeMapper = new StoreDefinitionsMapper();
        this.localStoreMap = storeMap;
        this.rawEngines = new ConcurrentHashMap<String, StorageEngine<byte[], byte[]>>();
        this.scheduler = scheduler;
        this.storageConfigurations = initStorageConfigurations(config);
        this.metadataStore = new MetadataStore(new FilesystemStorageEngine(MetadataStore.METADATA_STORE_NAME,
                                                                           config.getMetadataDirectory()),
                                               storeMap);
    }

    private ConcurrentMap<StorageEngineType, StorageConfiguration> initStorageConfigurations(VoldemortConfig config) {
        ConcurrentMap<StorageEngineType, StorageConfiguration> configs = new ConcurrentHashMap<StorageEngineType, StorageConfiguration>();
        if(config.iseBdbEngineEnabled())
            configs.put(StorageEngineType.BDB, new BdbStorageConfiguration(config));
        if(config.isMysqlEngineEnabled())
            configs.put(StorageEngineType.MYSQL, new MysqlStorageConfiguration(config));
        if(config.isMemoryEngineEnabled())
            configs.put(StorageEngineType.MEMORY, new InMemoryStorageConfiguration());
        if(config.isCacheEngineEnabled())
            configs.put(StorageEngineType.CACHE, new CacheStorageConfiguration());
        if(config.isReadOnlyEngineEnabled()) 
          configs.put(StorageEngineType.READONLY, new RandomAccessFileStorageConfiguration(config));
        
        if(configs.size() == 0)
            throw new ConfigurationException("No storage engine has been enabled!");

        return configs;
    }

    @Override
    protected void startInner() {
        this.localStoreMap.clear();
        this.localStoreMap.put(MetadataStore.METADATA_STORE_NAME, metadataStore);
        Store<byte[], byte[]> slopStorage = getStore("slop", voldemortConfig.getSlopStoreType());
        this.slopStore = new SerializingStore<byte[], Slop>(slopStorage,
                                                            new IdentitySerializer(),
                                                            new SlopSerializer());
        Cluster cluster = this.metadataStore.getCluster();
        List<StoreDefinition> storeDefs = this.metadataStore.getStores();
        logger.info("Initializing stores:");
        for(StoreDefinition def: storeDefs) {
            if(!def.getName().equals(MetadataStore.METADATA_STORE_NAME)) {
                logger.info("Opening " + def.getName() + ".");
                StorageEngine<byte[], byte[]> engine = getStore(def.getName(), def.getType());
                rawEngines.put(engine.getName(), engine);

                /* Now add any store wrappers that are enabled */
                Store<byte[], byte[]> store = engine;
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
                    store = new LoggingStore<byte[], byte[]>(store);
                if(voldemortConfig.isStatTrackingEnabled())
                    store = new StatTrackingStore<byte[], byte[]>(store);
                this.localStoreMap.put(def.getName(), store);
            }
        }
        logger.info("All stores initialized.");

        scheduleCleanupJobs(storeDefs, rawEngines);
    }

    private void scheduleCleanupJobs(List<StoreDefinition> storeDefs,
                                     Map<String, StorageEngine<byte[], byte[]>> engines) {
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
                StorageEngine<byte[], byte[]> engine = engines.get(storeDef.getName());
                Runnable cleanupJob = new DataCleanupJob<byte[], byte[]>(engine,
                                                                         cleanupPermits,
                                                                         storeDef.getRetentionDays()
                                                                                 * Time.MS_PER_DAY,
                                                                         SystemTime.INSTANCE);
                this.scheduler.schedule(cleanupJob, startTime, Time.MS_PER_DAY);
            }
        }
    }

    private StorageEngine<byte[], byte[]> getStore(String name, StorageEngineType type) {
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
        for(Store<byte[], byte[]> s: this.localStoreMap.values()) {
            try {
                logger.info("Closing " + s.getName() + ".");
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

    public ConcurrentMap<String, Store<byte[], byte[]>> getLocalStoreMap() {
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

    public StorageConfiguration getStorageConfiguration(StorageEngineType type) {
        return storageConfigurations.get(type);
    }

    public MetadataStore getMetadataStore() {
        return this.metadataStore;
    }

    public Store<byte[], Slop> getSlopStore() {
        return this.slopStore;
    }

}
