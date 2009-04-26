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

    private final File directory;
    private final ClusterMapper clusterMapper;
    private final StoreDefinitionsMapper storeMapper;

    public MetadataStore(File directory) {
        this.directory = directory;
        this.storeMapper = new StoreDefinitionsMapper();
        this.clusterMapper = new ClusterMapper();
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
        String keyStr = new String(key.get());
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

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public Cluster getCluster() {
        return clusterMapper.readCluster(createStringReader(CLUSTER_KEY));
    }

    private StringReader createStringReader(String keyName) {
        return new StringReader(getSingleValue(get(new ByteArray(ByteUtils.getBytes(keyName,
                                                                                    "UTF-8")))));
    }

    public List<StoreDefinition> getStores() {
        return storeMapper.readStoreList(createStringReader(STORES_KEY));
    }

    private String getSingleValue(List<Versioned<byte[]>> found) {
        if(found.size() != 1)
            throw new VoldemortException("Inconsistent metadata found: expected 1 version but found "
                                         + found.size());
        return ByteUtils.getString(found.get(0).getValue(), "UTF-8");
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new UnsupportedOperationException("Not implemented.");
    }

}
