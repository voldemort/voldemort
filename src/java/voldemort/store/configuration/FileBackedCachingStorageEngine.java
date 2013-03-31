/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Storage Engine used to persist the keys and values in a human readable
 * format on disk. The data is primarily served off of the cache. After each
 * put, the entire cache state is flushed to the backing file. The data is UTF-8
 * serialized when writing to the file in order to make it human readable.
 * 
 * The primary purpose of this storage engine is for maintaining the cluster
 * metadata which is characterized by low QPS and not latency sensitive.
 * 
 * @author csoman
 * 
 */
public class FileBackedCachingStorageEngine extends
        AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private final static Logger logger = Logger.getLogger(FileBackedCachingStorageEngine.class);
    private static final CharSequence NEW_PROPERTY_SEPARATOR = "[name=";
    private static final String NEW_LINE = System.getProperty("line.separator");

    private final String inputPath;
    private final String inputDirectory;
    private Map<String, String> metadataMap;
    private VectorClock cachedVersion = null;

    public FileBackedCachingStorageEngine(String name, String inputDirectory) {
        super(name);
        this.inputDirectory = inputDirectory;
        File directory = new File(this.inputDirectory);
        if(!directory.exists() && directory.canRead()) {
            throw new IllegalArgumentException("Directory " + directory.getAbsolutePath()
                                               + " does not exist or can not be read.");
        }

        this.inputPath = inputDirectory + System.getProperty("file.separator") + name;
        this.metadataMap = new HashMap<String, String>();
        this.loadData();
        if(logger.isDebugEnabled()) {
            logger.debug("Created a new File backed caching engine. File location = " + inputPath);
        }
    }

    private File getVersionFile() {
        return new File(this.inputDirectory, getName() + ".version");
    }

    // Read the Vector clock stored in '${name}.version' file
    private VectorClock readVersion() {
        try {

            if(this.cachedVersion == null) {
                File versionFile = getVersionFile();
                if(versionFile.exists()) {
                    // read the version file and return version.
                    String hexCode = FileUtils.readFileToString(versionFile, "UTF-8");
                    this.cachedVersion = new VectorClock(Hex.decodeHex(hexCode.toCharArray()));
                }
            }
            return this.cachedVersion;
        } catch(Exception e) {
            throw new VoldemortException("Failed to read Version for file :" + getName(), e);
        }
    }

    // Write a new Vector clock stored in '${name}.version' file
    private void writeVersion(VectorClock newClock) {
        File versionFile = getVersionFile();
        try {
            if(!versionFile.exists() || versionFile.delete()) {
                String hexCode = new String(Hex.encodeHex(newClock.toBytes()));
                FileUtils.writeStringToFile(versionFile, hexCode, "UTF-8");
                this.cachedVersion = newClock;
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to write Version for the current file :"
                                         + getName(), e);
        }
    }

    private void loadData() {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(this.inputPath)));
            String line = reader.readLine();

            while(line != null) {
                if(line.startsWith(NEW_PROPERTY_SEPARATOR.toString())) {
                    String key = null;
                    StringBuilder value = new StringBuilder();
                    String parts[] = line.split("=");

                    // Found a new property block.
                    // First read the key
                    if(parts.length == 2) {
                        key = parts[1].substring(0, parts[1].length() - 1);

                        // Now read the value block !
                        while((line = reader.readLine()) != null && line.length() != 0
                              && !line.startsWith(NEW_PROPERTY_SEPARATOR.toString())) {
                            if(value.length() == 0) {
                                value.append(line);
                            } else {
                                value.append(NEW_LINE + line);
                            }
                        }

                        // Now add the key and value to the hashmap
                        this.metadataMap.put(key, value.toString());
                    }
                } else {
                    line = reader.readLine();
                }
            }
        } catch(FileNotFoundException e) {
            logger.debug("File used for persistence does not exist !!");
        } catch(IOException e) {
            logger.debug("Error in flushing data to file : " + e);
        }
    }

    // Flush the in-memory data to the file
    private synchronized void flushData() {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File(this.inputPath)));
            for(String key: this.metadataMap.keySet()) {
                writer.write(NEW_PROPERTY_SEPARATOR + key.toString() + "]" + NEW_LINE);
                writer.write(this.metadataMap.get(key).toString());
                writer.write("" + NEW_LINE + "" + NEW_LINE);
            }
            writer.flush();
        } catch(IOException e) {
            logger.error("IO exception while flushing data to file backed storage: "
                         + e.getMessage());
        }

        try {
            if(writer != null)
                writer.close();
        } catch(Exception e) {
            logger.error("Error while flushing data to file backed storage: " + e.getMessage());
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        throw new VoldemortException("No extra capability.");
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new FileBackedStorageIterator(this.metadataMap, this);
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    @Override
    public void truncate() {
        throw new VoldemortException("Truncate not supported in FileBackedCachingStorageEngine");
    }

    // Assigning new Vector clock here: TODO: Decide what vector clock to use ?
    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String keyString = new String(key.get());
        List<Versioned<byte[]>> found = new ArrayList<Versioned<byte[]>>();
        byte[] resultBytes = null;
        String value = this.metadataMap.get(keyString);
        if(value != null) {
            resultBytes = value.getBytes();
            found.add(new Versioned<byte[]>(resultBytes, readVersion()));
        }
        return found;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        for(ByteArray key: keys) {
            List<Versioned<byte[]>> values = get(key, null);
            if(!values.isEmpty())
                result.put(key, values);
        }
        return result;
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        List<Versioned<byte[]>> values = get(key, null);
        List<Version> versions = new ArrayList<Version>(values.size());
        for(Versioned<?> value: values) {
            versions.add(value.getVersion());
        }
        return versions;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        // Validate the Vector clock
        VectorClock clock = readVersion();
        if(clock != null) {
            if(value.getVersion().compare(clock) == Occurred.BEFORE) {
                throw new ObsoleteVersionException("A successor version " + clock + "  to this "
                                                   + value.getVersion()
                                                   + " exists for the current file : " + getName());
            } else if(value.getVersion().compare(clock) == Occurred.CONCURRENTLY) {
                throw new ObsoleteVersionException("Concurrent Operation not allowed on Metadata.");
            }
        }

        // Update the cache copy
        this.metadataMap.put(new String(key.get()), new String(value.getValue()));

        // Flush the data to the file
        this.flushData();

        // Persist the new Vector clock
        writeVersion((VectorClock) value.getVersion());
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        boolean deleteSuccessful = false;
        StoreUtils.assertValidKey(key);
        String keyString = new String(key.get());
        String initialValue = this.metadataMap.get(keyString);
        if(initialValue != null) {
            String removedValue = this.metadataMap.remove(keyString);
            if(removedValue != null) {
                deleteSuccessful = (initialValue.equals(removedValue));
            }
        }
        if(deleteSuccessful) {
            this.flushData();
        }
        return deleteSuccessful;
    }

    private static class FileBackedStorageIterator implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private final Iterator<Entry<String, String>> iterator;
        private final FileBackedCachingStorageEngine storageEngineRef;

        public FileBackedStorageIterator(Map<String, String> metadataMap,
                                         FileBackedCachingStorageEngine storageEngine) {
            iterator = metadataMap.entrySet().iterator();
            storageEngineRef = storageEngine;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            Entry<String, String> entry = iterator.next();
            Pair<ByteArray, Versioned<byte[]>> nextValue = null;
            if(entry != null && entry.getKey() != null && entry.getValue() != null) {
                ByteArray key = new ByteArray(entry.getKey().getBytes());
                byte[] resultBytes = entry.getValue().getBytes();
                Versioned<byte[]> versionedValue = new Versioned<byte[]>(resultBytes,
                                                                         storageEngineRef.readVersion());
                nextValue = Pair.create(key, versionedValue);
            }

            return nextValue;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        @Override
        public void close() {}

    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based keys scan not supported for this storage type");
    }
}
