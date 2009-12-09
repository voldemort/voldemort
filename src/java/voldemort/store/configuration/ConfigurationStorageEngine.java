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

package voldemort.store.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A FileSystem based Storage Engine to persist configuration metadata.<br>
 * <imp>Used only by {@link MetadataStore}</imp><br>
 * 
 * @author bbansal
 * 
 */
public class ConfigurationStorageEngine implements StorageEngine<String, String> {

    private final String name;
    private final File directory;

    private static final Logger logger = Logger.getLogger(ConfigurationStorageEngine.class);

    public ConfigurationStorageEngine(String name, String directory) {
        this.name = name;
        this.directory = new File(directory);
        if(!this.directory.exists() && this.directory.canRead())
            throw new IllegalArgumentException("Directory " + this.directory.getAbsolutePath()
                                               + " does not exist or can not be read.");
    }

    public ClosableIterator<Pair<String, Versioned<String>>> entries() {
        throw new VoldemortException("Iteration  not supported in ConfigurationStorageEngine");
    }

    public void close() throws VoldemortException {

    }

    public synchronized boolean delete(String key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        for(File file: getDirectory(key).listFiles()) {
            if(file.getName().equals(key)) {
                try {
                    // delete the file and the version file
                    return file.delete()
                           && new File(getVersionDirectory(), file.getName()).delete();
                } catch(Exception e) {
                    logger.error("Error while attempt to delete key:" + key, e);
                }
            }
        }

        return false;
    }

    public synchronized List<Versioned<String>> get(String key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return get(key, getDirectory(key).listFiles());
    }

    public List<Version> getVersions(String key) {
        List<Versioned<String>> values = get(key);
        List<Version> versions = new ArrayList<Version>(values.size());
        for(Versioned<?> value: values) {
            versions.add(value.getVersion());
        }
        return versions;
    }

    public synchronized Map<String, List<Versioned<String>>> getAll(Iterable<String> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<String, List<Versioned<String>>> result = StoreUtils.newEmptyHashMap(keys);
        for(String key: keys) {
            List<Versioned<String>> values = get(key, getDirectory(key).listFiles());
            if(!values.isEmpty())
                result.put(key, values);
        }
        return result;
    }

    public String getName() {
        return name;
    }

    public synchronized void put(String key, Versioned<String> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        if(null == value.getValue()) {
            throw new VoldemortException("metadata cannot be null !!");
        }
        // Check for obsolete version
        File[] files = getDirectory(key).listFiles();
        for(File file: files) {
            if(file.getName().equals(key)) {
                VectorClock clock = readVersion(key);
                if(value.getVersion().compare(clock) == Occured.AFTER) {
                    // continue
                } else if(value.getVersion().compare(clock) == Occured.BEFORE) {
                    throw new ObsoleteVersionException("A successor version " + clock
                                                       + "  to this " + value.getVersion()
                                                       + "exists for key " + key);
                } else if(value.getVersion().compare(clock) == Occured.CONCURRENTLY) {
                    throw new ObsoleteVersionException("Concurrent Operation not allowed on Metadata.");
                }
            }
        }

        File keyFile = new File(getDirectory(key), key);
        VectorClock newClock = (VectorClock) value.getVersion();
        if(!keyFile.exists() || keyFile.delete()) {
            try {
                FileUtils.writeStringToFile(keyFile, value.getValue(), "UTF-8");
                writeVersion(key, newClock);
            } catch(IOException e) {
                throw new VoldemortException(e);
            }
        }
    }

    private File getDirectory(String key) {
        if(MetadataStore.OPTIONAL_KEYS.contains(key))
            return getTempDirectory();
        else
            return this.directory;
    }

    private List<Versioned<String>> get(String key, File[] files) {
        try {
            List<Versioned<String>> found = new ArrayList<Versioned<String>>();
            for(File file: files) {
                if(file.getName().equals(key)) {
                    VectorClock clock = readVersion(key);
                    if(null != clock) {
                        found.add(new Versioned<String>(FileUtils.readFileToString(file, "UTF-8"),
                                                        clock));
                    }
                }
            }
            return found;
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    private VectorClock readVersion(String key) {
        try {
            File versionFile = new File(getVersionDirectory(), key);
            if(!versionFile.exists()) {
                // bootstrap file save default clock as version.
                VectorClock clock = new VectorClock();
                writeVersion(key, clock);
                return clock;
            } else {
                // read the version file and return version.
                String hexCode = FileUtils.readFileToString(versionFile, "UTF-8");
                return new VectorClock(Hex.decodeHex(hexCode.toCharArray()));
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to read Version for Key:" + key, e);
        }
    }

    private void writeVersion(String key, VectorClock version) {
        try {
            File versionFile = new File(getVersionDirectory(), key);
            if(!versionFile.exists() || versionFile.delete()) {
                // write the version file.
                String hexCode = new String(Hex.encodeHex(version.toBytes()));
                FileUtils.writeStringToFile(versionFile, hexCode, "UTF-8");
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to write Version for Key:" + key, e);
        }
    }

    private File getVersionDirectory() {
        File versionDir = new File(this.directory, ".version");
        if(!versionDir.exists() || !versionDir.isDirectory()) {
            versionDir.delete();
            versionDir.mkdirs();
        }

        return versionDir;
    }

    private File getTempDirectory() {
        File tempDir = new File(this.directory, ".temp");
        if(!tempDir.exists() || !tempDir.isDirectory()) {
            tempDir.delete();
            tempDir.mkdirs();
        }

        return tempDir;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new VoldemortException("No extra capability.");
    }

    public ClosableIterator<String> keys() {
        throw new VoldemortException("keys iteration not supported.");
    }
}
