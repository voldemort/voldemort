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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableSet;

public class MetadataStore implements StorageEngine<ByteArray, byte[]> {

    public static final String METADATA_STORE_NAME = "metadata";
    public static final String CLUSTER_KEY = "cluster.xml";
    public static final String STORES_KEY = "stores.xml";
    public static final Set<String> KNOWN_KEYS = ImmutableSet.of("cluster.xml", "stores.xml");
    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    private final Cluster cluster;
    private final List<StoreDefinition> storeDefs;

    public MetadataStore(Cluster cluster, List<StoreDefinition> defs) {
        this.cluster = cluster;
        this.storeDefs = defs;
    }

    public static MetadataStore readFromDirectory(File dir) {
        if(!Utils.isReadableDir(dir))
            throw new IllegalArgumentException("Metadata directory " + dir.getAbsolutePath()
                                               + " does not exist or can not be read.");
        if(dir.listFiles() == null)
            throw new IllegalArgumentException("No configuration found in " + dir.getAbsolutePath()
                                               + ".");

        try {
            Cluster cluster = clusterMapper.readCluster(new File(dir, CLUSTER_KEY));
            List<StoreDefinition> defs = storeMapper.readStoreList(new File(dir, STORES_KEY));
            return new MetadataStore(cluster, defs);
        } catch(IOException e) {
            throw new VoldemortException("Error reading configuration.", e);
        }
    }

    public String getName() {
        return METADATA_STORE_NAME;
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        throw new VoldemortException("You can't delete your metadata, fool!");
    }

    /**
     * Store the new metadata, and apply any changes made by adding or deleting
     * stores
     */
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        throw new VoldemortException("No metadata modifications allowed (yet).");
    }

    public void close() throws VoldemortException {

    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        String keyStr = ByteUtils.getString(key.get(), "UTF-8");
        if(CLUSTER_KEY.equals(keyStr))
            return Collections.singletonList(new Versioned<byte[]>(ByteUtils.getBytes(clusterMapper.writeCluster(cluster),
                                                                                      "UTF-8")));
        else if(STORES_KEY.equals(keyStr))
            return Collections.singletonList(new Versioned<byte[]>(ByteUtils.getBytes(storeMapper.writeStoreList(storeDefs),
                                                                                      "UTF-8")));
        else
            throw new VoldemortException("Unknown metadata key " + keyStr);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public List<StoreDefinition> getStores() {
        return storeDefs;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new UnsupportedOperationException("Not implemented.");
    }

}
