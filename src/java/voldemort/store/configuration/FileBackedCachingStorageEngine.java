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
import voldemort.store.StorageEngine;
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

public class FileBackedCachingStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private final static Logger logger = Logger.getLogger(FileBackedCachingStorageEngine.class);
    private static final CharSequence NEW_PROPERTY_SEPARATOR = "[name=";

    private final String inputPath;
    private final String inputDirectory;
    private final String name;
    private Map<String, String> metadataMap;
    private VectorClock cachedVersion = null;

    public FileBackedCachingStorageEngine(String name, String inputDirectory) {
        this.name = name;
        this.inputDirectory = inputDirectory;
        File directory = new File(this.inputDirectory);
        if(!directory.exists() && directory.canRead()) {
            throw new IllegalArgumentException("Directory " + directory.getAbsolutePath()
                                               + " does not exist or can not be read.");
        }

        this.inputPath = inputDirectory + "/" + name;
        this.metadataMap = new HashMap<String, String>();
        this.loadData();
        logger.debug("Created a new File backed caching engine. File location = " + inputPath);
    }

    private File getVersionFile() {
        return new File(this.inputDirectory, this.name + ".version");
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
            throw new VoldemortException("Failed to read Version for file :" + this.name, e);
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
                                         + this.name, e);
        }
    }

    private void loadData() {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(this.inputPath)));
            String line = reader.readLine();

            while(line != null) {
                if(line.contains(NEW_PROPERTY_SEPARATOR)) {
                    String key = null;
                    StringBuilder value = new StringBuilder();
                    String parts[] = line.split("=");

                    // Found a new property block.
                    // First read the key
                    if(parts.length == 2) {
                        key = parts[1].substring(0, parts[1].length() - 1);

                        // Now read the value block !
                        while((line = reader.readLine()) != null && line.length() != 0
                              && !line.contains(NEW_PROPERTY_SEPARATOR)) {
                            if(value.length() == 0) {
                                value.append(line);
                            } else {
                                value.append("\n" + line);
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
                writer.write(NEW_PROPERTY_SEPARATOR + key.toString() + "]\n");
                writer.write(this.metadataMap.get(key).toString());
                writer.write("\n\n");
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

    public String getName() {
        return this.name;
    }

    public void close() throws VoldemortException {}

    public Object getCapability(StoreCapabilityType capability) {
        throw new VoldemortException("No extra capability.");
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new FileBackedStorageIterator(this.metadataMap, this);
    }

    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    public void truncate() {
        throw new VoldemortException("Truncate not supported in FileBackedCachingStorageEngine");
    }

    public boolean isPartitionAware() {
        return false;
    }

    // Assigning new Vector clock here: TODO: Decide what vector clock to use ?
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

    public List<Version> getVersions(ByteArray key) {
        List<Versioned<byte[]>> values = get(key, null);
        List<Version> versions = new ArrayList<Version>(values.size());
        for(Versioned<?> value: values) {
            versions.add(value.getVersion());
        }
        return versions;
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        // Validate the Vector clock
        VectorClock clock = readVersion();
        if(clock != null) {
            if(value.getVersion().compare(clock) == Occurred.BEFORE) {
                throw new ObsoleteVersionException("A successor version " + clock + "  to this "
                                                   + value.getVersion()
                                                   + " exists for the current file : " + this.name);
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

        public boolean hasNext() {
            return iterator.hasNext();
        }

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

        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        public void close() {}

    }

}
