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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.Entry;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableSet;

public class MetadataStore implements StorageEngine<byte[], byte[]> {

    public static final String METADATA_STORE_NAME = "metadata";
    public static final String CLUSTER_KEY = "cluster.xml";
    public static final String STORES_KEY = "stores.xml";
    public static final Set<String> KNOWN_KEYS = ImmutableSet.of("cluster.xml", "stores.xml");

    private final File directory;
    private final ClusterMapper clusterMapper;
    private final StoreDefinitionsMapper storeMapper;
    private final Map<String, ? extends Store<byte[], byte[]>> stores;

    public MetadataStore(File directory, Map<String, ? extends Store<byte[], byte[]>> stores) {
        this.directory = directory;
        this.storeMapper = new StoreDefinitionsMapper();
        this.clusterMapper = new ClusterMapper();
        this.stores = stores;
        if(this.directory.listFiles() == null)
            throw new IllegalArgumentException("No configuration found in "
                                               + this.directory.getAbsolutePath() + ".");
        if(!this.directory.exists() && this.directory.canRead())
            throw new IllegalArgumentException("Metadata directory "
                                               + this.directory.getAbsolutePath()
                                               + " does not exist or can not be read.");
    }

    public String getName() {
        return METADATA_STORE_NAME;
    }

    public boolean delete(byte[] key, Version version) throws VoldemortException {
        throw new VoldemortException("You can't delete your metadata, fool!");
    }

    /**
     * Store the new metadata, and apply any changes made by adding or deleting
     * stores
     */
    public void put(byte[] key, Versioned<byte[]> value) throws VoldemortException {
        throw new VoldemortException("No metadata modifications allowed (yet).");
    }

    public void close() throws VoldemortException {

    }

    public List<Versioned<byte[]>> get(byte[] key) throws VoldemortException {
        String keyStr = new String(key);
        if(!KNOWN_KEYS.contains(keyStr))
            throw new IllegalArgumentException("Unknown metadata key: " + keyStr);
        File file = new File(this.directory, keyStr);
        if(!Utils.isReadableFile(file.getAbsolutePath()))
            throw new VoldemortException("Attempt to read metadata failed: "
                                         + file.getAbsolutePath() + " is not a readable file!");
        try {
            return Collections.singletonList(new Versioned<byte[]>(FileUtils.readFileToByteArray(file)));
        } catch(IOException e) {
            throw new VoldemortException("Error reading metadata value '" + keyStr + "': ", e);
        }
    }

    public Cluster getCluster() {
        return clusterMapper.readCluster(new StringReader(getSingleValue(get(ByteUtils.getBytes(CLUSTER_KEY,
                                                                                                "UTF-8")))));
    }

    public List<StoreDefinition> getStores() {
        return storeMapper.readStoreList(new StringReader(getSingleValue(get(ByteUtils.getBytes(STORES_KEY,
                                                                                                "UTF-8")))));
    }

    private String getSingleValue(List<Versioned<byte[]>> found) {
        if(found.size() != 1)
            throw new VoldemortException("Inconsistent metadata found: expected 1 version but found "
                                         + found.size());
        return ByteUtils.getString(found.get(0).getValue(), "UTF-8");
    }

    public ClosableIterator<Entry<byte[], Versioned<byte[]>>> entries() {
        throw new IllegalStateException("Not implemented.");
    }

}
