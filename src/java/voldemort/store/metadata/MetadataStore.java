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
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.filesystem.ConfiguratinStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.utils.WriteThroughCache;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * MetadataStore maintains metadata for Voldemort Server. <br>
 * Metadata is persisted as string for ease of readability.Metadata Store keeps
 * a write-through-cache for performance.
 * 
 * @author bbansal
 * 
 */
public class MetadataStore implements StorageEngine<ByteArray, byte[]> {

    public static final String METADATA_STORE_NAME = "metadata";
    public static final String CLUSTER_KEY = "cluster.xml";
    public static final String STORES_KEY = "stores.xml";
    public static final String SERVER_STATE_KEY = "server.state";
    public static final String REBALANCING_PROXY_DEST = "rebalancing.proxy.dest";
    public static final String ROLLBACK_CLUSTER_KEY = null;

    public static final Set<String> KNOWN_KEYS = ImmutableSet.of(CLUSTER_KEY,
                                                                 STORES_KEY,
                                                                 SERVER_STATE_KEY,
                                                                 REBALANCING_PROXY_DEST);

    private final Store<String, String> innerStore;
    public final MetadataCache metadataCache = new MetadataCache();

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    public MetadataStore(Store<String, String> innerStore) {
        this.innerStore = innerStore;
        init();
    }

    public static MetadataStore readFromDirectory(File dir) {
        if(!Utils.isReadableDir(dir))
            throw new IllegalArgumentException("Metadata directory " + dir.getAbsolutePath()
                                               + " does not exist or can not be read.");
        if(dir.listFiles() == null)
            throw new IllegalArgumentException("No configuration found in " + dir.getAbsolutePath()
                                               + ".");
        Store<String, String> innerStore = new ConfiguratinStorageEngine(MetadataStore.METADATA_STORE_NAME,
                                                                         dir.getAbsolutePath());
        return new MetadataStore(innerStore);
    }

    public String getName() {
        return METADATA_STORE_NAME;
    }

    /**
     * Initializes the metadataCache for MetadataStore
     */
    private void init() {
        for(String key: KNOWN_KEYS) {
            metadataCache.get(key);
        }
    }

    /**
     * @param key : keyName strings serialized as bytes eg. 'cluster.xml'
     * @param value: versioned byte[] eg. UTF bytes for cluster xml definitions
     * @return void
     * @throws VoldemortException
     */
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        String keyStr = ByteUtils.getString(key.get(), "UTF-8");
        String valueStr = ByteUtils.getString(value.getValue(), "UTF-8");
        Version version = value.getVersion();

        if(KNOWN_KEYS.contains(keyStr)) {
            if(CLUSTER_KEY.equals(keyStr)) {
                metadataCache.put(keyStr,
                                  new Versioned<Object>(clusterMapper.readCluster(new StringReader(valueStr)),
                                                        version));
            } else if(STORES_KEY.equals(keyStr)) {
                metadataCache.put(keyStr,
                                  new Versioned<Object>(storeMapper.readStoreList(new StringReader(valueStr)),
                                                        version));
            } else if(SERVER_STATE_KEY.equals(keyStr) || REBALANCING_PROXY_DEST.equals(keyStr)) {
                metadataCache.put(keyStr, new Versioned<Object>(valueStr, version));
            } else {
                throw new VoldemortException("Unhandled Key:" + keyStr + " for MetadataStore put()");
            }
        }
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    /**
     * @param key : keyName strings serialized as bytes eg. 'cluster.xml'
     * @return List of values (only 1 for Metadata) versioned byte[] eg. UTF
     *         bytes for cluster xml definitions
     * @throws VoldemortException
     */
    public List<Versioned<byte[]>> get(byte[] key) throws VoldemortException {
        String keyString = ByteUtils.getString(key, "UTF-8");
        String valueString;

        if(CLUSTER_KEY.equals(keyString)) {
            valueString = clusterMapper.writeCluster(getCluster());
        } else if(STORES_KEY.equals(keyString)) {
            valueString = storeMapper.writeStoreList(getStores());
        } else if(SERVER_STATE_KEY.equals(keyString) || REBALANCING_PROXY_DEST.equals(keyString)) {
            valueString = metadataCache.get(keyString).getValue().toString();
        } else {
            throw new VoldemortException("Unhandled key:" + keyString + " for get() method.");
        }

        List<Versioned<byte[]>> values = Lists.newArrayList();
        values.add(new Versioned<byte[]>(ByteUtils.getBytes(valueString, "UTF-8"),
                                         metadataCache.get(keyString).getVersion()));
        return values;
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        return get(key.get());
    }

    public Cluster getCluster() {
        return (Cluster) metadataCache.get(CLUSTER_KEY).getValue();
    }

    public List<StoreDefinition> getStores() {
        return (List<StoreDefinition>) metadataCache.get(STORES_KEY).getValue();
    }

    public StoreDefinition getStore(String storeName) {
        List<StoreDefinition> storeDefs = getStores();
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().equals(storeName)) {
                return storeDef;
            }
        }

        throw new VoldemortException("Store " + storeName + " not found in MetadataStore");
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new VoldemortException("You cannot iterate over all entries in Metadata");
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        throw new VoldemortException("You cannot delete your metadata fool !!");
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public class MetadataCache extends WriteThroughCache<String, Versioned<Object>> {

        /**
         * ReadBack function to initialize from innerStore
         */
        @Override
        public Versioned<Object> readBack(String key) {
            if(CLUSTER_KEY.equals(key)) {
                Versioned<String> versionClusterString = getInnerValue(CLUSTER_KEY, true);
                Cluster cluster = clusterMapper.readCluster(new StringReader(versionClusterString.getValue()));
                return new Versioned<Object>(cluster, versionClusterString.getVersion());
            } else if(STORES_KEY.equals(key)) {
                Versioned<String> versionStoresString = getInnerValue(STORES_KEY, true);
                List<StoreDefinition> stores = storeMapper.readStoreList(new StringReader(versionStoresString.getValue()));
                return new Versioned<Object>(stores, versionStoresString.getVersion());
            } else if(SERVER_STATE_KEY.equals(key) || REBALANCING_PROXY_DEST.equals(key)) {
                Versioned<String> value = getInnerValue(key, false);
                return new Versioned<Object>(value.getValue(), value.getVersion());
            }

            throw new VoldemortException("Unsupported key in readBack:" + key);
        }

        /**
         * WriteBack function to update the innerStore.
         */
        @Override
        public void writeBack(String key, Versioned<Object> value) {
            if(CLUSTER_KEY.equals(key)) {
                String clusterString = clusterMapper.writeCluster((Cluster) value.getValue());
                innerStore.put(key, new Versioned<String>(clusterString, value.getVersion()));
            } else if(STORES_KEY.equals(key)) {
                String storesString = storeMapper.writeStoreList((List<StoreDefinition>) value.getValue());
                innerStore.put(key, new Versioned<String>(storesString, value.getVersion()));
            } else if(SERVER_STATE_KEY.equals(key) || REBALANCING_PROXY_DEST.equals(key)) {
                innerStore.put(key, new Versioned<String>(value.getValue().toString(),
                                                          value.getVersion()));
            }

            throw new VoldemortException("Unsupported key in writeBack:" + key);
        }

        public Versioned<String> getInnerValue(String key, boolean required)
                throws VoldemortException {
            List<Versioned<String>> values = innerStore.get(key);

            if(values.size() > 1)
                throw new VoldemortException("Inconsistent metadata found: expected 1 version but found "
                                             + values.size() + " for key:" + key);
            if(values.size() > 0) {
                return values.get(0);
            } else {
                if(required)
                    throw new VoldemortException("No metadata found for required key:" + key);
            }

            return null;
        }
    }

    public Cluster getRollBackCluster() {
        // TODO remove this method entirely
        return null;
    }
}
