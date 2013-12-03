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

package voldemort.store.metadata;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.MBeanOperationInfo;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.cluster.Cluster;
import voldemort.routing.RouteToAllStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.configuration.ConfigurationStorageEngine;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * MetadataStore maintains metadata for Voldemort Server. <br>
 * Metadata is persisted as strings in inner store for ease of readability.<br>
 * Metadata Store keeps an in memory write-through-cache for performance.
 */
public class MetadataStore extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    public static final String METADATA_STORE_NAME = "metadata";
    public static final String STORE_DEFINITIONS_STORE_NAME = "STORES";

    public static final String CLUSTER_KEY = "cluster.xml";
    public static final String STORES_KEY = "stores.xml";
    public static final String SYSTEM_STORES_KEY = "system.stores";
    public static final String SERVER_STATE_KEY = "server.state";
    public static final String NODE_ID_KEY = "node.id";
    public static final String REBALANCING_STEAL_INFO = "rebalancing.steal.info.key";
    public static final String REBALANCING_SOURCE_CLUSTER_XML = "rebalancing.source.cluster.xml";
    public static final String REBALANCING_SOURCE_STORES_XML = "rebalancing.source.stores.xml";

    public static final Set<String> GOSSIP_KEYS = ImmutableSet.of(CLUSTER_KEY, STORES_KEY);

    public static final Set<String> REQUIRED_KEYS = ImmutableSet.of(CLUSTER_KEY, STORES_KEY);

    public static final Set<String> OPTIONAL_KEYS = ImmutableSet.of(SERVER_STATE_KEY,
                                                                    NODE_ID_KEY,
                                                                    REBALANCING_STEAL_INFO,
                                                                    REBALANCING_SOURCE_CLUSTER_XML,
                                                                    REBALANCING_SOURCE_STORES_XML);

    public static final Set<Object> METADATA_KEYS = ImmutableSet.builder()
                                                                .addAll(REQUIRED_KEYS)
                                                                .addAll(OPTIONAL_KEYS)
                                                                .build();

    // helper keys for metadataCacheOnly
    private static final String ROUTING_STRATEGY_KEY = "routing.strategy";
    private static final String SYSTEM_ROUTING_STRATEGY_KEY = "system.routing.strategy";

    public static enum VoldemortState {
        NORMAL_SERVER,
        REBALANCING_MASTER_SERVER
    }

    private final Store<String, String, String> innerStore;
    private final Map<String, Versioned<Object>> metadataCache;
    private final StorageEngine<String, String, String> storeDefinitionsManager;
    private final List<String> storeNames;

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    private static final RoutingStrategyFactory routingFactory = new RoutingStrategyFactory();

    // Guards mutations made to non-scalar objects e.g., lists stored in
    // innerStore
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    public final Lock readLock = lock.readLock();
    public final Lock writeLock = lock.writeLock();

    private final ConcurrentHashMap<String, List<MetadataStoreListener>> storeNameTolisteners;

    private static final Logger logger = Logger.getLogger(MetadataStore.class);

    public MetadataStore(Store<String, String, String> innerStore,
                         StorageEngine<String, String, String> storeDefinitionsManager,
                         int nodeId) {
        super(innerStore.getName());
        this.innerStore = innerStore;
        this.metadataCache = new HashMap<String, Versioned<Object>>();
        this.storeNameTolisteners = new ConcurrentHashMap<String, List<MetadataStoreListener>>();
        this.storeDefinitionsManager = storeDefinitionsManager;
        this.storeNames = new ArrayList<String>();

        init(nodeId);
    }

    // This constructor is used exclusively by tests
    public MetadataStore(Store<String, String, String> innerStore, int nodeId) {
        super(innerStore.getName());
        this.innerStore = innerStore;
        this.metadataCache = new HashMap<String, Versioned<Object>>();
        this.storeNameTolisteners = new ConcurrentHashMap<String, List<MetadataStoreListener>>();
        this.storeNames = new ArrayList<String>();
        StorageEngine<String, String, String> storesRepo = new InMemoryStorageEngine<String, String, String>("stores-repo");

        List<Versioned<String>> versionedStoreList = innerStore.get(STORES_KEY, "");

        if(versionedStoreList != null) {
            String stores = versionedStoreList.get(0).getValue();
            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefinitions = mapper.readStoreList(new StringReader(stores));
            for(StoreDefinition storeDef: storeDefinitions) {
                Versioned<String> versionedStoreValue = new Versioned<String>(mapper.writeStore(storeDef));
                storesRepo.put(storeDef.getName(), versionedStoreValue, null);
            }
        }
        this.storeDefinitionsManager = storesRepo;

        init(nodeId);
    }

    public void addMetadataStoreListener(String storeName, MetadataStoreListener listener) {
        if(this.storeNameTolisteners == null)
            throw new VoldemortException("MetadataStoreListener must be non-null");

        if(!this.storeNameTolisteners.containsKey(storeName))
            this.storeNameTolisteners.put(storeName, new ArrayList<MetadataStoreListener>(2));
        this.storeNameTolisteners.get(storeName).add(listener);
    }

    public void removeMetadataStoreListener(String storeName) {
        if(this.storeNameTolisteners == null)
            throw new VoldemortException("MetadataStoreListener must be non-null");

        this.storeNameTolisteners.remove(storeName);
    }

    public static MetadataStore readFromDirectory(File dir, int nodeId) {
        if(!Utils.isReadableDir(dir))
            throw new IllegalArgumentException("Metadata directory " + dir.getAbsolutePath()
                                               + " does not exist or can not be read.");

        String storeDefDirPath = dir.getAbsolutePath() + File.separator
                                 + MetadataStore.STORE_DEFINITIONS_STORE_NAME;

        // If config directory does not contain STORES sub directory, then
        // create one by parsing the stores.xml file
        List<String> configurationFiles = Arrays.asList(dir.list());
        if(configurationFiles == null)
            throw new IllegalArgumentException("No configuration found in " + dir.getAbsolutePath()
                                               + ".");

        if(!configurationFiles.contains(STORE_DEFINITIONS_STORE_NAME)) {
            // parse stores.xml and create STORES sub-dir
            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefinitions = null;
            try {
                storeDefinitions = mapper.readStoreList(new File(dir.getAbsolutePath()
                                                                 + File.separator + STORES_KEY));
            } catch(IOException e) {
                throw new VoldemortException("Cannot parse the store definitions from "
                                             + STORES_KEY + " file ", e);
            }

            if(storeDefinitions == null) {
                throw new VoldemortException("Neither STORES nor stores.xml exist in the config directory");
            }

            // Create the STORES sub directory
            File storeDefinitionsDir = new File(storeDefDirPath);
            if(!storeDefinitionsDir.mkdir()) {
                throw new VoldemortException("Unable to create " + STORE_DEFINITIONS_STORE_NAME
                                             + " sub directory");
            }
            for(StoreDefinition storeDef: storeDefinitions) {
                try {
                    FileUtils.writeStringToFile(new File(storeDefDirPath + File.separator
                                                         + storeDef.getName()),
                                                mapper.writeStore(storeDef));
                } catch(IOException e) {
                    throw new VoldemortException("Cannot write store definition to file: "
                                                 + storeDef.getName(), e);
                }
            }
        }

        // Create a STORES configuration engine for STORES sub-directory
        StorageEngine<String, String, String> storesEngine = new ConfigurationStorageEngine(MetadataStore.STORE_DEFINITIONS_STORE_NAME,
                                                                                            storeDefDirPath);

        Store<String, String, String> innerStore = new ConfigurationStorageEngine(MetadataStore.METADATA_STORE_NAME,
                                                                                  dir.getAbsolutePath());
        return new MetadataStore(innerStore, storesEngine, nodeId);
    }

    @Override
    public String getName() {
        return METADATA_STORE_NAME;
    }

    /**
     * helper function to convert strings to bytes as needed.
     * 
     * @param key
     * @param value
     */
    @SuppressWarnings("unchecked")
    public void put(String key, Versioned<Object> value) {
        // acquire write lock
        writeLock.lock();

        try {
            if(this.storeNames.contains(key) || key.equals(STORES_KEY)) {

                // Check for backwards compatibility
                List<StoreDefinition> storeDefs = (List<StoreDefinition>) value.getValue();
                StoreDefinitionUtils.validateSchemasAsNeeded(storeDefs);

                // If the put is on the entire stores.xml key, delete the
                // additional stores which do not exist in the specified
                // stores.xml
                Set<String> storeNamesToDelete = new HashSet<String>();
                for(String storeName: this.storeNames) {
                    storeNamesToDelete.add(storeName);
                }

                // Add / update the list of store definitions specified in the
                // value
                StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
                List<StoreDefinition> storeDefinitions = (List<StoreDefinition>) value.getValue();

                // Update the STORES directory and the corresponding entry in
                // metadata cache
                Set<String> specifiedStoreNames = new HashSet<String>();
                for(StoreDefinition storeDef: storeDefinitions) {
                    specifiedStoreNames.add(storeDef.getName());
                    String storeDefStr = mapper.writeStore(storeDef);
                    Versioned<String> versionedValueStr = new Versioned<String>(storeDefStr,
                                                                                value.getVersion());
                    this.storeDefinitionsManager.put(storeDef.getName(), versionedValueStr, "");

                    // Update the metadata cache
                    this.metadataCache.put(storeDef.getName(),
                                           new Versioned<Object>(storeDefStr, value.getVersion()));
                }

                if(key.equals(STORES_KEY)) {
                    storeNamesToDelete.removeAll(specifiedStoreNames);
                    resetStoreDefinitions(storeNamesToDelete);
                }

                // Re-initialize the store definitions
                initStoreDefinitions(value.getVersion());

                // Update routing strategies
                updateRoutingStrategies(getCluster(), storeDefinitions);

            } else if(METADATA_KEYS.contains(key)) {
                // try inserting into inner store first
                putInner(key, convertObjectToString(key, value));

                // cache all keys if innerStore put succeeded
                metadataCache.put(key, value);

                // do special stuff if needed
                if(CLUSTER_KEY.equals(key)) {
                    updateRoutingStrategies((Cluster) value.getValue(), getStoreDefList());
                } else if(STORES_KEY.equals(key)) {
                    updateRoutingStrategies(getCluster(), (List<StoreDefinition>) value.getValue());
                } else if(SYSTEM_STORES_KEY.equals(key))
                    throw new VoldemortException("Cannot overwrite system store definitions");

            } else {
                throw new VoldemortException("Unhandled Key:" + key + " for MetadataStore put()");
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Function to update store definitions. Unlike the put method, this
     * function does not delete any existing state. It only updates the state of
     * the stores specified in the given stores.xml
     * 
     * @param valueBytes specifies the bytes of the stores.xml containing
     *        updates for the specified stores
     */
    @SuppressWarnings("unchecked")
    public void updateStoreDefinitions(Versioned<byte[]> valueBytes) {
        // acquire write lock
        writeLock.lock();

        try {
            Versioned<String> value = new Versioned<String>(ByteUtils.getString(valueBytes.getValue(),
                                                                                "UTF-8"),
                                                            valueBytes.getVersion());
            Versioned<Object> valueObject = convertStringToObject(STORES_KEY, value);
            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefinitions = (List<StoreDefinition>) valueObject.getValue();

            // Go through each store definition and do a corresponding put
            for(StoreDefinition storeDef: storeDefinitions) {
                if(!this.storeNames.contains(storeDef.getName())) {
                    throw new VoldemortException("Cannot update a store which does not exist !");
                }

                String storeDefStr = mapper.writeStore(storeDef);
                Versioned<String> versionedValueStr = new Versioned<String>(storeDefStr,
                                                                            value.getVersion());
                this.storeDefinitionsManager.put(storeDef.getName(), versionedValueStr, "");

                // Update the metadata cache
                this.metadataCache.put(storeDef.getName(),
                                       new Versioned<Object>(storeDefStr, value.getVersion()));
            }

            // Re-initialize the store definitions
            initStoreDefinitions(value.getVersion());

            // Update routing strategies
            updateRoutingStrategies(getCluster(), storeDefinitions);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * helper function to read current version and put() after incrementing it
     * for local node.
     * 
     * @param key
     * @param value
     */
    public void put(String key, Object value) {
        // acquire write lock
        writeLock.lock();
        try {
            if(METADATA_KEYS.contains(key)) {
                VectorClock version = (VectorClock) get(key, null).get(0).getVersion();
                put(key,
                    new Versioned<Object>(value, version.incremented(getNodeId(),
                                                                     System.currentTimeMillis())));
            } else {
                throw new VoldemortException("Unhandled Key:" + key + " for MetadataStore put()");
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * A write through put to inner-store.
     * 
     * @param keyBytes : keyName strings serialized as bytes eg. 'cluster.xml'
     * @param valueBytes : versioned byte[] eg. UTF bytes for cluster xml
     *        definitions
     * @throws VoldemortException
     */
    @Override
    public void put(ByteArray keyBytes, Versioned<byte[]> valueBytes, byte[] transforms)
            throws VoldemortException {
        // acquire write lock
        writeLock.lock();
        try {
            String key = ByteUtils.getString(keyBytes.get(), "UTF-8");
            Versioned<String> value = new Versioned<String>(ByteUtils.getString(valueBytes.getValue(),
                                                                                "UTF-8"),
                                                            valueBytes.getVersion());

            Versioned<Object> valueObject = convertStringToObject(key, value);

            this.put(key, valueObject);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws VoldemortException {
        innerStore.close();
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        return innerStore.getCapability(capability);
    }

    /**
     * @param keyBytes : keyName strings serialized as bytes eg. 'cluster.xml'
     * @return List of values (only 1 for Metadata) versioned byte[] eg. UTF
     *         bytes for cluster xml definitions
     * @throws VoldemortException
     */
    @Override
    public List<Versioned<byte[]>> get(ByteArray keyBytes, byte[] transforms)
            throws VoldemortException {
        // acquire read lock

        readLock.lock();
        try {
            // get a read lock this prevents any sort of interleaving\
            // especially critical during reebalance when we set the new cluster
            // and store xml

            String key = ByteUtils.getString(keyBytes.get(), "UTF-8");

            if(METADATA_KEYS.contains(key) || this.storeNames.contains(key)) {
                List<Versioned<byte[]>> values = Lists.newArrayList();

                // Get the cached value and convert to string
                Versioned<String> value = convertObjectToString(key, metadataCache.get(key));

                // Metadata debugging information
                if(logger.isTraceEnabled())
                    logger.trace("Key " + key + " requested, returning: " + value.getValue());

                values.add(new Versioned<byte[]>(ByteUtils.getBytes(value.getValue(), "UTF-8"),
                                                 value.getVersion()));

                return values;
            } else {
                throw new VoldemortException("Unhandled Key:" + key + " for MetadataStore get()");
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to read metadata key:"
                                                 + ByteUtils.getString(keyBytes.get(), "UTF-8")
                                                 + " delete config/.temp config/.version directories and restart.",
                                         e);
        } finally {
            readLock.unlock();
        }

    }

    public List<Versioned<byte[]>> get(String key, String transforms) throws VoldemortException {
        // acquire read lock
        readLock.lock();
        try {
            return get(new ByteArray(ByteUtils.getBytes(key, "UTF-8")),
                       transforms == null ? null : ByteUtils.getBytes(transforms, "UTF-8"));
        } finally {
            readLock.unlock();
        }
    }

    @JmxOperation(description = "Clean all rebalancing server/cluster states from this node.", impact = MBeanOperationInfo.ACTION)
    public void cleanAllRebalancingState() {
        // acquire write lock
        writeLock.lock();
        try {
            for(String key: OPTIONAL_KEYS) {
                if(!key.equals(NODE_ID_KEY))
                    innerStore.delete(key,
                                      getVersions(new ByteArray(ByteUtils.getBytes(key, "UTF-8"))).get(0));
            }

            init(getNodeId());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        // acquire read lock
        readLock.lock();
        try {
            List<Versioned<byte[]>> values = get(key, null);
            List<Version> versions = new ArrayList<Version>(values.size());
            for(Versioned<?> value: values) {
                versions.add(value.getVersion());
            }
            return versions;
        } finally {
            readLock.unlock();
        }
    }

    public Cluster getCluster() {
        // acquire read lock
        readLock.lock();
        try {
            return (Cluster) metadataCache.get(CLUSTER_KEY).getValue();
        } finally {
            readLock.unlock();

        }
    }

    @SuppressWarnings("unchecked")
    public List<StoreDefinition> getStoreDefList() {
        // acquire read lock
        readLock.lock();
        try {
            return (List<StoreDefinition>) metadataCache.get(STORES_KEY).getValue();
        } finally {
            readLock.unlock();

        }
    }

    @SuppressWarnings("unchecked")
    public List<StoreDefinition> getSystemStoreDefList() {
        // acquire read lock
        readLock.lock();
        try {
            return (List<StoreDefinition>) metadataCache.get(SYSTEM_STORES_KEY).getValue();
        } finally {
            readLock.unlock();
        }
    }

    public int getNodeId() {
        // acquire read lock
        readLock.lock();
        try {
            return (Integer) (metadataCache.get(NODE_ID_KEY).getValue());
        } finally {
            readLock.unlock();
        }
    }

    public StoreDefinition getStoreDef(String storeName) {
        // acquire read lock
        readLock.lock();
        try {

            List<StoreDefinition> storeDefs = getStoreDefList();
            for(StoreDefinition storeDef: storeDefs) {
                if(storeDef.getName().equals(storeName))
                    return storeDef;
            }

            throw new VoldemortException("Store " + storeName + " not found in MetadataStore");
        } finally {
            readLock.unlock();
        }
    }

    public VoldemortState getServerStateLocked() {
        // acquire read lock
        readLock.lock();
        try {
            return VoldemortState.valueOf(metadataCache.get(SERVER_STATE_KEY).getValue().toString());
        } finally {
            readLock.unlock();

        }
    }

    public VoldemortState getServerStateUnlocked() {

        return VoldemortState.valueOf(metadataCache.get(SERVER_STATE_KEY).getValue().toString());

    }

    public RebalancerState getRebalancerState() {
        // acquire read lock
        readLock.lock();
        try {
            return (RebalancerState) metadataCache.get(REBALANCING_STEAL_INFO).getValue();
        } finally {
            readLock.unlock();
        }
    }

    public Cluster getRebalancingSourceCluster() {
        // acquire read lock
        readLock.lock();
        try {
            return (Cluster) metadataCache.get(REBALANCING_SOURCE_CLUSTER_XML).getValue();
        } finally {
            readLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public List<StoreDefinition> getRebalancingSourceStores() {
        // acquire read lock
        readLock.lock();
        try {
            return (List<StoreDefinition>) metadataCache.get(REBALANCING_SOURCE_STORES_XML)
                                                        .getValue();
        } finally {
            readLock.unlock();
        }
    }

    /*
     * First check in the map of regular stores. If not present, check in the
     * system stores map.
     */
    @SuppressWarnings("unchecked")
    public RoutingStrategy getRoutingStrategy(String storeName) {
        // acquire read lock
        readLock.lock();
        try {
            Map<String, RoutingStrategy> routingStrategyMap = (Map<String, RoutingStrategy>) metadataCache.get(ROUTING_STRATEGY_KEY)
                                                                                                          .getValue();
            RoutingStrategy strategy = routingStrategyMap.get(storeName);
            if(strategy == null) {
                Map<String, RoutingStrategy> systemRoutingStrategyMap = (Map<String, RoutingStrategy>) metadataCache.get(SYSTEM_ROUTING_STRATEGY_KEY)
                                                                                                                    .getValue();
                strategy = systemRoutingStrategyMap.get(storeName);
            }
            return strategy;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the list of store defs as a map
     * 
     * @param storeDefs
     * @return
     */
    private HashMap<String, StoreDefinition> makeStoreDefinitionMap(List<StoreDefinition> storeDefs) {
        HashMap<String, StoreDefinition> storeDefMap = new HashMap<String, StoreDefinition>();
        for(StoreDefinition storeDef: storeDefs)
            storeDefMap.put(storeDef.getName(), storeDef);
        return storeDefMap;
    }

    /**
     * Changes to cluster OR store definition metadata results in routing
     * strategies changing. These changes need to be propagated to all the
     * listeners.
     * 
     * @param cluster The updated cluster metadata
     * @param storeDefs The updated list of store definition
     */
    private void updateRoutingStrategies(Cluster cluster, List<StoreDefinition> storeDefs) {
        // acquire write lock
        writeLock.lock();
        try {
            VectorClock clock = new VectorClock();
            if(metadataCache.containsKey(ROUTING_STRATEGY_KEY))
                clock = (VectorClock) metadataCache.get(ROUTING_STRATEGY_KEY).getVersion();

            logger.info("Updating routing strategy for all stores");
            HashMap<String, StoreDefinition> storeDefMap = makeStoreDefinitionMap(storeDefs);
            HashMap<String, RoutingStrategy> routingStrategyMap = createRoutingStrategyMap(cluster,
                                                                                           storeDefMap);
            this.metadataCache.put(ROUTING_STRATEGY_KEY,
                                   new Versioned<Object>(routingStrategyMap,
                                                         clock.incremented(getNodeId(),
                                                                           System.currentTimeMillis())));

            for(String storeName: storeNameTolisteners.keySet()) {
                RoutingStrategy updatedRoutingStrategy = routingStrategyMap.get(storeName);
                if(updatedRoutingStrategy != null) {
                    try {
                        for(MetadataStoreListener listener: storeNameTolisteners.get(storeName)) {
                            listener.updateRoutingStrategy(updatedRoutingStrategy);
                            listener.updateStoreDefinition(storeDefMap.get(storeName));
                        }
                    } catch(Exception e) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn(e, e);
                    }
                }

            }
        } finally {
            writeLock.unlock();
        }
    }

    /*
     * Initialize the routing strategy map for system stores. This is used
     * during get / put on system stores.
     */
    private void initSystemRoutingStrategies(Cluster cluster) {
        HashMap<String, RoutingStrategy> routingStrategyMap = createRoutingStrategyMap(cluster,
                                                                                       makeStoreDefinitionMap(getSystemStoreDefList()));
        this.metadataCache.put(SYSTEM_ROUTING_STRATEGY_KEY,
                               new Versioned<Object>(routingStrategyMap));
    }

    /**
     * Add the steal information to the rebalancer state
     * 
     * @param stealInfo The steal information to add
     */
    public void addRebalancingState(final RebalanceTaskInfo stealInfo) {
        // acquire write lock
        writeLock.lock();
        try {
            // Move into rebalancing state
            if(ByteUtils.getString(get(SERVER_STATE_KEY, null).get(0).getValue(), "UTF-8")
                        .compareTo(VoldemortState.NORMAL_SERVER.toString()) == 0) {
                put(SERVER_STATE_KEY, VoldemortState.REBALANCING_MASTER_SERVER);
                initCache(SERVER_STATE_KEY);
            }

            // Add the steal information
            RebalancerState rebalancerState = getRebalancerState();
            if(!rebalancerState.update(stealInfo)) {
                throw new VoldemortException("Could not add steal information " + stealInfo
                                             + " since a plan for the same donor node "
                                             + stealInfo.getDonorId() + " ( "
                                             + rebalancerState.find(stealInfo.getDonorId())
                                             + " ) already exists");
            }
            put(MetadataStore.REBALANCING_STEAL_INFO, rebalancerState);
            initCache(REBALANCING_STEAL_INFO);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Delete the partition steal information from the rebalancer state
     * 
     * @param stealInfo The steal information to delete
     */
    public void deleteRebalancingState(RebalanceTaskInfo stealInfo) {
        // acquire write lock
        writeLock.lock();
        try {
            RebalancerState rebalancerState = getRebalancerState();

            if(!rebalancerState.remove(stealInfo))
                throw new IllegalArgumentException("Couldn't find " + stealInfo + " in "
                                                   + rebalancerState + " while deleting");

            if(rebalancerState.isEmpty()) {
                logger.debug("Cleaning all rebalancing state");
                cleanAllRebalancingState();
            } else {
                put(REBALANCING_STEAL_INFO, rebalancerState);
                initCache(REBALANCING_STEAL_INFO);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Function to add a new Store to the Metadata store. This involves
     * 
     * 1. Create a new entry in the ConfigurationStorageEngine for STORES.
     * 
     * 2. Update the metadata cache.
     * 
     * 3. Re-create the 'stores.xml' key
     * 
     * @param storeDef defines the new store to be created
     */
    public void addStoreDefinition(StoreDefinition storeDef) {
        // acquire write lock
        writeLock.lock();

        try {
            // Check if store already exists
            if(this.storeNames.contains(storeDef.getName())) {
                throw new VoldemortException("Store already exists !");
            }

            // Otherwise add to the STORES directory
            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            String storeDefStr = mapper.writeStore(storeDef);
            Versioned<String> versionedValueStr = new Versioned<String>(storeDefStr);
            this.storeDefinitionsManager.put(storeDef.getName(), versionedValueStr, null);

            // Update the metadata cache
            this.metadataCache.put(storeDef.getName(), new Versioned<Object>(storeDefStr));

            // Re-initialize the store definitions. This is primarily required
            // to re-create the value for key: 'stores.xml'. This is necessary
            // for backwards compatibility.
            initStoreDefinitions(null);

            updateRoutingStrategies(getCluster(), getStoreDefList());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Function to delete the specified store from Metadata store. This involves
     * 
     * 1. Remove entry from the ConfigurationStorageEngine for STORES.
     * 
     * 2. Update the metadata cache.
     * 
     * 3. Re-create the 'stores.xml' key
     * 
     * @param storeName specifies name of the store to be deleted.
     */
    public void deleteStoreDefinition(String storeName) {
        // acquire write lock
        writeLock.lock();

        try {
            // Check if store exists
            if(!this.storeNames.contains(storeName)) {
                throw new VoldemortException("Requested store to be deleted does not exist !");
            }

            // Otherwise remove from the STORES directory. Note: The version
            // argument is not required here since the
            // ConfigurationStorageEngine simply ignores this.
            this.storeDefinitionsManager.delete(storeName, null);

            // Update the metadata cache
            this.metadataCache.remove(storeName);

            // Re-initialize the store definitions. This is primarily required
            // to re-create the value for key: 'stores.xml'. This is necessary
            // for backwards compatibility.
            initStoreDefinitions(null);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new VoldemortException("You cannot iterate over all entries in Metadata");
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        throw new VoldemortException("You cannot iterate over all keys in Metadata");
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public void truncate() {
        throw new VoldemortException("You cannot truncate entries in Metadata");
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        throw new VoldemortException("You cannot delete your metadata fool !!");
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        // acquire read lock
        readLock.lock();
        try {
            StoreUtils.assertValidKeys(keys);
            return StoreUtils.getAll(this, keys, transforms);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Initializes the metadataCache for MetadataStore
     */
    private void init(int nodeId) {
        logger.info("metadata init().");

        writeLock.lock();
        // Required keys
        initCache(CLUSTER_KEY);

        // If stores definition storage engine is not null, initialize metadata
        // Add the mapping from key to the storage engine used
        if(this.storeDefinitionsManager != null) {
            initStoreDefinitions(null);
        } else {
            initCache(STORES_KEY);
        }

        // Initialize system store in the metadata cache
        initSystemCache();
        initSystemRoutingStrategies(getCluster());

        initCache(NODE_ID_KEY, nodeId);
        if(getNodeId() != nodeId)
            throw new RuntimeException("Attempt to start previous node:"
                                       + getNodeId()
                                       + " as node:"
                                       + nodeId
                                       + " (Did you copy config directory ? try deleting .temp .version in config dir to force clean) aborting ...");

        // Initialize with default if not present
        initCache(REBALANCING_STEAL_INFO, new RebalancerState(new ArrayList<RebalanceTaskInfo>()));
        initCache(SERVER_STATE_KEY, VoldemortState.NORMAL_SERVER.toString());
        initCache(REBALANCING_SOURCE_CLUSTER_XML, null);
        initCache(REBALANCING_SOURCE_STORES_XML, null);

        // set transient values
        updateRoutingStrategies(getCluster(), getStoreDefList());

        writeLock.unlock();
    }

    /**
     * Function to go through all the store definitions contained in the STORES
     * directory and
     * 
     * 1. Update metadata cache.
     * 
     * 2. Update STORES_KEY by stitching together all these keys.
     * 
     * 3. Update 'storeNames' list.
     * 
     * This method is not thread safe. It is expected that the caller of this
     * method will correctly handle concurrency issues. Currently this is not an
     * issue since its invoked by init, put, add and delete store all of which
     * use locks to deal with any concurrency related issues.
     */
    private void initStoreDefinitions(Version storesXmlVersion) {
        if(this.storeDefinitionsManager == null) {
            throw new VoldemortException("The store definitions directory is empty");
        }

        String allStoreDefinitions = "<stores>";
        Version finalStoresXmlVersion = null;
        if(storesXmlVersion != null) {
            finalStoresXmlVersion = storesXmlVersion;
        }
        this.storeNames.clear();

        ClosableIterator<Pair<String, Versioned<String>>> storesIterator = this.storeDefinitionsManager.entries();

        // Some test setups may result in duplicate entries for 'store' element.
        // Do the de-dup here
        Map<String, Versioned<String>> storeNameToDefMap = new HashMap<String, Versioned<String>>();
        Version maxVersion = null;
        while(storesIterator.hasNext()) {
            Pair<String, Versioned<String>> storeDetail = storesIterator.next();
            String storeName = storeDetail.getFirst();
            Versioned<String> versionedStoreDef = storeDetail.getSecond();
            storeNameToDefMap.put(storeName, versionedStoreDef);
            Version curVersion = versionedStoreDef.getVersion();

            // Get the highest version from all the store entries
            if(maxVersion == null) {
                maxVersion = curVersion;
            } else if(maxVersion.compare(curVersion) == Occurred.BEFORE) {
                maxVersion = curVersion;
            }
        }

        // If the specified version is null, assign highest Version to
        // 'stores.xml' key
        if(finalStoresXmlVersion == null) {
            finalStoresXmlVersion = maxVersion;
        }

        // Go through all the individual stores and update metadata
        for(Entry<String, Versioned<String>> storeEntry: storeNameToDefMap.entrySet()) {
            String storeName = storeEntry.getKey();
            Versioned<String> versionedStoreDef = storeEntry.getValue();

            // Add all the store names to the list of storeNames
            this.storeNames.add(storeName);

            this.metadataCache.put(storeName, new Versioned<Object>(versionedStoreDef.getValue(),
                                                                    versionedStoreDef.getVersion()));

            // Stitch together to form the complete store definition list.
            allStoreDefinitions += versionedStoreDef.getValue();
        }
        allStoreDefinitions += "</stores>";

        // Update cache with the composite store definition list.
        metadataCache.put(STORES_KEY,
                          convertStringToObject(STORES_KEY,
                                                new Versioned<String>(allStoreDefinitions,
                                                                      finalStoresXmlVersion)));
    }

    /**
     * Function to clear all the metadata related to the given store
     * definitions. This is needed when a put on 'stores.xml' is called, thus
     * replacing the existing state.
     * 
     * This method is not thread safe. It is expected that the caller of this
     * method will handle concurrency related issues.
     * 
     * @param storeNamesToDelete
     */
    private void resetStoreDefinitions(Set<String> storeNamesToDelete) {
        // Clear entries in the metadata cache
        for(String storeName: storeNamesToDelete) {
            this.metadataCache.remove(storeName);
            this.storeDefinitionsManager.delete(storeName, null);
            this.storeNames.remove(storeName);
        }
    }

    private synchronized void initCache(String key) {
        metadataCache.put(key, convertStringToObject(key, getInnerValue(key)));
    }

    // Initialize the metadata cache with system store list
    private synchronized void initSystemCache() {
        List<StoreDefinition> value = storeMapper.readStoreList(new StringReader(SystemStoreConstants.SYSTEM_STORE_SCHEMA));
        metadataCache.put(SYSTEM_STORES_KEY, new Versioned<Object>(value));
    }

    private void initCache(String key, Object defaultValue) {
        try {
            initCache(key);
        } catch(Exception e) {
            // put default value if failed to init
            this.put(key, new Versioned<Object>(defaultValue));
        }
    }

    private HashMap<String, RoutingStrategy> createRoutingStrategyMap(Cluster cluster,
                                                                      HashMap<String, StoreDefinition> storeDefs) {
        HashMap<String, RoutingStrategy> map = new HashMap<String, RoutingStrategy>();

        for(StoreDefinition store: storeDefs.values()) {
            map.put(store.getName(), routingFactory.updateRoutingStrategy(store, cluster));
        }

        // add metadata Store route to ALL routing strategy.
        map.put(METADATA_STORE_NAME, new RouteToAllStrategy(getCluster().getNodes()));

        return map;
    }

    /**
     * Converts Object to byte[] depending on the key
     * <p>
     * StoreRepository takes only StorageEngine<ByteArray,byte[]> and for
     * persistence on disk we need to convert them to String.<br>
     * 
     * @param key
     * @param value
     * @return
     */
    @SuppressWarnings("unchecked")
    private Versioned<String> convertObjectToString(String key, Versioned<Object> value) {
        String valueStr = "";

        if(CLUSTER_KEY.equals(key)) {
            valueStr = clusterMapper.writeCluster((Cluster) value.getValue());
        } else if(STORES_KEY.equals(key)) {
            valueStr = storeMapper.writeStoreList((List<StoreDefinition>) value.getValue());
        } else if(REBALANCING_STEAL_INFO.equals(key)) {
            RebalancerState rebalancerState = (RebalancerState) value.getValue();
            valueStr = rebalancerState.toJsonString();
        } else if(SERVER_STATE_KEY.equals(key) || NODE_ID_KEY.equals(key)) {
            valueStr = value.getValue().toString();
        } else if(REBALANCING_SOURCE_CLUSTER_XML.equals(key)) {
            if(value.getValue() != null) {
                valueStr = clusterMapper.writeCluster((Cluster) value.getValue());
            }
        } else if(REBALANCING_SOURCE_STORES_XML.equals(key)) {
            if(value.getValue() != null) {
                valueStr = storeMapper.writeStoreList((List<StoreDefinition>) value.getValue());
            }
        } else if(this.storeNames.contains(key)) {
            valueStr = "<stores>";
            if(value.getValue() != null) {
                valueStr += value.getValue();
            }
            valueStr += "</stores>";
        } else {
            throw new VoldemortException("Unhandled key:'" + key
                                         + "' for Object to String serialization.");
        }

        return new Versioned<String>(valueStr, value.getVersion());
    }

    /**
     * convert Object to String depending on key.
     * <p>
     * StoreRepository takes only StorageEngine<ByteArray,byte[]> and for
     * persistence on disk we need to convert them to String.<br>
     * 
     * @param key
     * @param value
     * @return
     */
    private Versioned<Object> convertStringToObject(String key, Versioned<String> value) {
        Object valueObject = null;

        if(CLUSTER_KEY.equals(key)) {
            valueObject = clusterMapper.readCluster(new StringReader(value.getValue()));
        } else if(STORES_KEY.equals(key)) {
            valueObject = storeMapper.readStoreList(new StringReader(value.getValue()));
        } else if(SERVER_STATE_KEY.equals(key)) {
            valueObject = VoldemortState.valueOf(value.getValue());
        } else if(NODE_ID_KEY.equals(key)) {
            valueObject = Integer.parseInt(value.getValue());
        } else if(REBALANCING_STEAL_INFO.equals(key)) {
            String valueString = value.getValue();
            if(valueString.startsWith("[")) {
                valueObject = RebalancerState.create(valueString);
            } else {
                valueObject = new RebalancerState(Arrays.asList(RebalanceTaskInfo.create(valueString)));
            }
        } else if(REBALANCING_SOURCE_CLUSTER_XML.equals(key)) {
            if(value.getValue() != null && value.getValue().length() > 0) {
                valueObject = clusterMapper.readCluster(new StringReader(value.getValue()));
            }
        } else if(REBALANCING_SOURCE_STORES_XML.equals(key)) {
            if(value.getValue() != null && value.getValue().length() > 0) {
                valueObject = storeMapper.readStoreList(new StringReader(value.getValue()));
            }
        } else {
            throw new VoldemortException("Unhandled key:'" + key
                                         + "' for String to Object serialization.");
        }

        return new Versioned<Object>(valueObject, value.getVersion());
    }

    private void putInner(String key, Versioned<String> value) {
        innerStore.put(key, value, null);
    }

    private Versioned<String> getInnerValue(String key) throws VoldemortException {
        List<Versioned<String>> values = innerStore.get(key, null);

        if(values.size() > 1)
            throw new VoldemortException("Inconsistent metadata found: expected 1 version but found "
                                         + values.size() + " for key:" + key);
        if(values.size() > 0)
            return values.get(0);

        throw new VoldemortException("No metadata found for required key:" + key);
    }
}
