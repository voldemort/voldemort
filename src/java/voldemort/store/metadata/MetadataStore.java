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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.routing.RouteToAllStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.configuration.ConfigurationStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
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
 * Metadata Store keeps a in memory write-through-cache for performance.
 * 
 * 
 * @author bbansal
 * 
 */
public class MetadataStore implements StorageEngine<ByteArray, byte[]> {

    public static final String METADATA_STORE_NAME = "metadata";

    public static final String CLUSTER_KEY = "cluster.xml";
    public static final String STORES_KEY = "stores.xml";
    public static final String CLUSTER_STATE_KEY = "cluster.state";
    public static final String SERVER_STATE_KEY = "server.state";
    public static final String NODE_ID_KEY = "node.id";
    public static final String REBALANCING_SLAVES_LIST_KEY = "rebalancing.slave.list";
    public static final String REBALANCING_PARTITIONS_LIST_KEY = "rebalancing.partitions.list";
    public static final String REBALANCING_SLAVE_NODE_ID = "rebalancing.node.id";

    public static final Set<String> REQUIRED_KEYS = ImmutableSet.of(CLUSTER_KEY, STORES_KEY);

    public static final Set<String> OPTIONAL_KEYS = ImmutableSet.of(SERVER_STATE_KEY,
                                                                    NODE_ID_KEY,
                                                                    REBALANCING_SLAVES_LIST_KEY,
                                                                    REBALANCING_PARTITIONS_LIST_KEY,
                                                                    REBALANCING_SLAVE_NODE_ID,
                                                                    CLUSTER_STATE_KEY);

    public static final Set<Object> METADATA_KEYS = ImmutableSet.builder()
                                                                .addAll(REQUIRED_KEYS)
                                                                .addAll(OPTIONAL_KEYS)
                                                                .build();

    // helper keys for metadataCacheOnly
    private static final String ROUTING_STRATEGY_KEY = "routing.strategy";

    public static enum VoldemortState {
        NORMAL_SERVER,
        REBALANCING_MASTER_SERVER,
        REBALANCING_CLUSTER,
        NORMAL_CLUSTER
    }

    private final Store<String, String> innerStore;
    private final HashMap<String, Versioned<Object>> metadataCache;

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    private static final RoutingStrategyFactory routingFactory = new RoutingStrategyFactory();

    private static final Logger logger = Logger.getLogger(MetadataStore.class);

    public MetadataStore(Store<String, String> innerStore, int nodeId) {
        this.innerStore = innerStore;
        this.metadataCache = new HashMap<String, Versioned<Object>>();

        init(nodeId);
    }

    public static MetadataStore readFromDirectory(File dir, int nodeId) {
        if(!Utils.isReadableDir(dir))
            throw new IllegalArgumentException("Metadata directory " + dir.getAbsolutePath()
                                               + " does not exist or can not be read.");
        if(dir.listFiles() == null)
            throw new IllegalArgumentException("No configuration found in " + dir.getAbsolutePath()
                                               + ".");
        Store<String, String> innerStore = new ConfigurationStorageEngine(MetadataStore.METADATA_STORE_NAME,
                                                                          dir.getAbsolutePath());
        return new MetadataStore(innerStore, nodeId);
    }

    public String getName() {
        return METADATA_STORE_NAME;
    }

    /**
     * helper function to auto update version and put()
     * 
     * @param key
     * @param value
     */
    public void put(String keyString, Object value) {
        ByteArray key = new ByteArray(ByteUtils.getBytes(keyString, "UTF-8"));
        VectorClock current = (VectorClock) getVersions(key).get(0);

        put(keyString,
            new Versioned<Object>(value, current.incremented(getNodeId(),
                                                             System.currentTimeMillis())));
    }

    /**
     * helper function to convert strings to bytes as needed.
     * 
     * @param key
     * @param value
     */
    public void put(String key, Versioned<Object> value) {
        if(METADATA_KEYS.contains(key)) {

            // try inserting into inner store first
            putInner(key, convertObjectToString(key, value));

            // cache all keys if innerStore put succeeded
            metadataCache.put(key, value);

        } else {
            throw new VoldemortException("Unhandled Key:" + key + " for MetadataStore put()");
        }
    }

    /**
     * A write through put to inner-store.
     * 
     * @param key : keyName strings serialized as bytes eg. 'cluster.xml'
     * @param value: versioned byte[] eg. UTF bytes for cluster xml definitions
     * @return void
     * @throws VoldemortException
     */
    public void put(ByteArray keyBytes, Versioned<byte[]> valueBytes) throws VoldemortException {
        String key = ByteUtils.getString(keyBytes.get(), "UTF-8");
        Versioned<String> value = new Versioned<String>(ByteUtils.getString(valueBytes.getValue(),
                                                                            "UTF-8"),
                                                        valueBytes.getVersion());
        Versioned<Object> valueObject = convertStringToObject(key, value);

        this.put(key, valueObject);
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        return innerStore.getCapability(capability);
    }

    /**
     * @param key : keyName strings serialized as bytes eg. 'cluster.xml'
     * @return List of values (only 1 for Metadata) versioned byte[] eg. UTF
     *         bytes for cluster xml definitions
     * @throws VoldemortException
     */
    public List<Versioned<byte[]>> get(ByteArray keyBytes) throws VoldemortException {
        try {
            String key = ByteUtils.getString(keyBytes.get(), "UTF-8");

            if(METADATA_KEYS.contains(key)) {
                List<Versioned<byte[]>> values = Lists.newArrayList();

                // Get the cached value and convert to string
                Versioned<String> value = convertObjectToString(key, metadataCache.get(key));

                values.add(new Versioned<byte[]>(ByteUtils.getBytes(value.getValue(), "UTF-8"),
                                                 value.getVersion()));

                return values;
            } else {
                throw new VoldemortException("Unhandled Key:" + key + " for MetadataStore get()");
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to get() for key:"
                                         + ByteUtils.getString(keyBytes.get(), "UTF-8"), e);
        }

    }

    public List<Version> getVersions(ByteArray key) {
        List<Versioned<byte[]>> values = get(key);
        List<Version> versions = new ArrayList<Version>(values.size());
        for(Versioned<?> value: values) {
            versions.add(value.getVersion());
        }
        return versions;
    }

    public Cluster getCluster() {
        return (Cluster) metadataCache.get(CLUSTER_KEY).getValue();
    }

    @SuppressWarnings("unchecked")
    public List<StoreDefinition> getStoreDefList() {
        return (List<StoreDefinition>) metadataCache.get(STORES_KEY).getValue();
    }

    public int getNodeId() {
        return (Integer) (metadataCache.get(NODE_ID_KEY).getValue());
    }

    public StoreDefinition getStoreDef(String storeName) {
        List<StoreDefinition> storeDefs = getStoreDefList();
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().equals(storeName)) {
                return storeDef;
            }
        }

        throw new VoldemortException("Store " + storeName + " not found in MetadataStore");
    }

    public VoldemortState getServerState() {
        return VoldemortState.valueOf(metadataCache.get(SERVER_STATE_KEY).getValue().toString());
    }

    public List<Integer> getRebalancingProxySlaveList() {
        return (List<Integer>) metadataCache.get(REBALANCING_PARTITIONS_LIST_KEY).getValue();
    }

    public Integer getRebalancingSlaveNodeId() {
        return (Integer) metadataCache.get(REBALANCING_SLAVE_NODE_ID).getValue();
    }

    @SuppressWarnings("unchecked")
    public List<Integer> getRebalancingPartitionList() {
        return (List<Integer>) metadataCache.get(REBALANCING_PARTITIONS_LIST_KEY).getValue();
    }

    @SuppressWarnings("unchecked")
    public RoutingStrategy getRoutingStrategy(String storeName) {
        Map<String, RoutingStrategy> routingStrategyMap = (Map<String, RoutingStrategy>) metadataCache.get(ROUTING_STRATEGY_KEY)
                                                                                                      .getValue();
        return routingStrategyMap.get(storeName);
    }

    public void updateRoutingStrategies() {
        VectorClock clock = new VectorClock();
        if(metadataCache.containsKey(ROUTING_STRATEGY_KEY))
            clock = (VectorClock) metadataCache.get(ROUTING_STRATEGY_KEY).getVersion();

        this.metadataCache.put(ROUTING_STRATEGY_KEY,
                               new Versioned<Object>(createRoutingStrategMap(),
                                                     clock.incremented(getNodeId(),
                                                                       System.currentTimeMillis())));
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new VoldemortException("You cannot iterate over all entries in Metadata");
    }

    public ClosableIterator<ByteArray> keys() {
        throw new VoldemortException("You cannot iterate over all keys in Metadata");
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        throw new VoldemortException("You cannot delete your metadata fool !!");
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    /**
     * Initializes the metadataCache for MetadataStore
     */
    private void init(int nodeId) {
        logger.info("metadata init().");

        // Required keys
        initCache(CLUSTER_KEY);
        initCache(STORES_KEY);

        // Initialize with default if not present
        initCache(NODE_ID_KEY, nodeId);
        initCache(REBALANCING_SLAVES_LIST_KEY, new ArrayList<Integer>(0));
        initCache(REBALANCING_PARTITIONS_LIST_KEY, new ArrayList<Integer>(0));
        initCache(SERVER_STATE_KEY, VoldemortState.NORMAL_SERVER.toString());
        initCache(REBALANCING_SLAVE_NODE_ID, -1);

        // set transient values
        updateRoutingStrategies();
    }

    private void initCache(String key) {
        metadataCache.put(key, convertStringToObject(key, getInnerValue(key)));
    }

    private void initCache(String key, Object defaultValue) {
        try {
            initCache(key);
        } catch(Exception e) {
            // put default value if failed to init
            this.put(key, new Versioned<Object>(defaultValue));
        }
    }

    private Object createRoutingStrategMap() {
        HashMap<String, RoutingStrategy> map = new HashMap<String, RoutingStrategy>();

        for(StoreDefinition store: getStoreDefList()) {
            map.put(store.getName(), routingFactory.updateRoutingStrategy(store, getCluster()));
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
        String valueStr = value.getValue().toString();

        if(CLUSTER_KEY.equals(key)) {
            valueStr = clusterMapper.writeCluster((Cluster) value.getValue());
        } else if(STORES_KEY.equals(key)) {
            valueStr = storeMapper.writeStoreList((List<StoreDefinition>) value.getValue());
        } else if(REBALANCING_SLAVES_LIST_KEY.equals(key)
                  || REBALANCING_PARTITIONS_LIST_KEY.equals(key)) {
            // save the list as comma separate string.
            StringBuilder builder = new StringBuilder();
            List<Integer> list = (List<Integer>) value.getValue();
            for(int i = 0; i < list.size(); i++) {
                Integer partition = list.get(i);
                builder.append(partition);
                if(i < list.size() - 1) {
                    builder.append(",");
                }
            }
            valueStr = builder.toString();
        } else if(SERVER_STATE_KEY.equals(key) || CLUSTER_STATE_KEY.equals(key)
                  || NODE_ID_KEY.equals(key) || REBALANCING_SLAVE_NODE_ID.equals(key)) {
            valueStr = value.getValue().toString();
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
        } else if(SERVER_STATE_KEY.equals(key) || CLUSTER_STATE_KEY.equals(key)) {
            valueObject = VoldemortState.valueOf(value.getValue());
        } else if(NODE_ID_KEY.equals(key) || REBALANCING_SLAVE_NODE_ID.equals(key)) {
            valueObject = Integer.parseInt(value.getValue());
        } else if(REBALANCING_SLAVES_LIST_KEY.equals(key)
                  || REBALANCING_PARTITIONS_LIST_KEY.equals(key)) {
            List<Integer> list = new ArrayList<Integer>();
            if(value.getValue().trim().length() > 0) {
                String[] partitions = value.getValue().split(",");
                if(partitions.length > 0) {
                    for(String partition: partitions) {
                        list.add(Integer.parseInt(partition));
                    }
                }
            }
            valueObject = list;
        } else {
            throw new VoldemortException("Unhandled key:'" + key
                                         + "' for String to Object serialization.");
        }

        return new Versioned<Object>(valueObject, value.getVersion());
    }

    private void putInner(String key, Versioned<String> value) {
        innerStore.put(key, value);
    }

    private Versioned<String> getInnerValue(String key) throws VoldemortException {
        List<Versioned<String>> values = innerStore.get(key);

        if(values.size() > 1)
            throw new VoldemortException("Inconsistent metadata found: expected 1 version but found "
                                         + values.size() + " for key:" + key);
        if(values.size() > 0) {
            return values.get(0);
        }

        throw new VoldemortException("No metadata found for required key:" + key);
    }
}
