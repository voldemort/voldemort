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

package voldemort.store.readonly;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * A read-only store that fronts a big file
 * 
 * 
 */
public class ReadOnlyStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static Logger logger = Logger.getLogger(ReadOnlyStorageEngine.class);

    /*
     * The overhead for each cache element is the key size + 4 byte array length
     * + 12 byte object overhead + 8 bytes for a 64-bit reference to the thing
     */
    public static final int MEMORY_OVERHEAD_PER_KEY = ReadOnlyUtils.KEY_HASH_SIZE + 4 + 12 + 8;

    private final String name;
    private final int numBackups;
    private int maxVersion;
    private final File storeDir;
    private final ReadWriteLock fileModificationLock;
    private final SearchStrategy searchStrategy;
    private volatile ChunkedFileSet fileSet;
    private volatile boolean isOpen;

    /**
     * Create an instance of the store
     * 
     * @param name The name of the store
     * @param searchStrategy The algorithm to use for searching for keys
     * @param storeDir The directory in which the .data and .index files reside
     * @param numBackups The number of backups of these files to retain
     */
    public ReadOnlyStorageEngine(String name,
                                 SearchStrategy searchStrategy,
                                 File storeDir,
                                 int numBackups) {
        this.storeDir = storeDir;
        this.numBackups = numBackups;
        this.name = Utils.notNull(name);
        this.searchStrategy = searchStrategy;
        this.fileSet = null;
        this.maxVersion = 0;
        /*
         * A lock that blocks reads during swap(), open(), and close()
         * operations
         */
        this.fileModificationLock = new ReentrantReadWriteLock();
        this.isOpen = false;
        open(null);
    }

    /**
     * Open the store with the version directory specified
     * 
     * @param versionDir Version Directory to use
     */
    public void open(File versionDir) {
        /* acquire modification lock */
        fileModificationLock.writeLock().lock();
        try {
            /* check that the store is currently closed */
            if(isOpen)
                throw new IllegalStateException("Attempt to open already open store.");

            // Find latest directory
            if(versionDir == null) {
                versionDir = findLatestVersion();
            } else {
                setMaxVersion(versionDir);
            }
            versionDir.mkdirs();

            // Create symbolic link
            logger.info("Creating symbolic link for '" + getName() + "' using directory "
                        + versionDir.getAbsolutePath());
            Utils.symlink(versionDir.getAbsolutePath(), storeDir.getAbsolutePath() + File.separator
                                                        + "latest");
            this.fileSet = new ChunkedFileSet(versionDir);
            isOpen = true;
        } finally {
            fileModificationLock.writeLock().unlock();
        }
    }

    /**
     * Find the latest version directory. First checks for 'latest' symbolic
     * link, if it does not exist falls back to max version number
     * 
     * @param storeDir
     * @return the directory with the latest version
     */
    public File findLatestVersion() {
        // Return latest symbolic link if it exists
        File latestVersion = new File(storeDir, "latest");
        if(latestVersion.exists() && Utils.isSymLink(latestVersion)) {
            File canonicalLatestDir = null;
            try {
                canonicalLatestDir = latestVersion.getCanonicalFile();
            } catch(IOException e) {}

            // Check if canonical directory exists, if not fall back to manual
            // search
            if(canonicalLatestDir != null) {
                return setMaxVersion(canonicalLatestDir);
            }
        }

        File[] versionDirs = storeDir.listFiles();

        // No version directories exist, create new empty folder
        if(versionDirs == null || versionDirs.length == 0) {
            File version0 = new File(storeDir, "version-0");
            return setMaxVersion(version0);
        }

        return setMaxVersion(versionDirs);
    }

    public int getMaxVersion() {
        return maxVersion;
    }

    public String getStoreDirPath() {
        return storeDir.getAbsolutePath();
    }

    private File setMaxVersion(File[] versionDirs) {
        int max = 0;
        for(File versionDir: versionDirs) {
            if(versionDir.isDirectory() && versionDir.getName().contains("version-")) {
                int version = Integer.parseInt(versionDir.getName().replace("version-", ""));
                if(version > max) {
                    max = version;
                }
            }
        }
        maxVersion = max;
        return new File(storeDir, "version-" + maxVersion);
    }

    private File setMaxVersion(File versionDir) {
        if(versionDir.isDirectory() && versionDir.getName().contains("version-")) {
            maxVersion = Integer.parseInt(versionDir.getName().replace("version-", ""));
        }
        return new File(storeDir, "version-" + maxVersion);
    }

    /**
     * Close the store.
     */
    public void close() throws VoldemortException {
        logger.debug("Close called for read-only store.");
        this.fileModificationLock.writeLock().lock();

        try {
            if(isOpen) {
                this.isOpen = false;
                fileSet.close();
            } else {
                logger.debug("Attempt to close already closed store " + getName());
            }
        } finally {
            this.fileModificationLock.writeLock().unlock();
        }
    }

    /**
     * Swap the current index and data files for a new pair
     * 
     * @param newIndexFile The path to the new index file
     * @param newDataFile The path to the new data file
     */
    @JmxOperation(description = "swapFiles(newStoreDirectory) changes this store "
                                + " to use the new data directory.")
    public void swapFiles(String newStoreDirectory) {
        logger.info("Swapping files for store '" + getName() + "' to " + newStoreDirectory);
        File newDataDir = new File(newStoreDirectory);
        if(!newDataDir.exists())
            throw new VoldemortException("File " + newDataDir.getAbsolutePath()
                                         + " does not exist.");

        if(!newDataDir.getName().startsWith("version-"))
            throw new VoldemortException("Invalid version folder name '" + newDataDir.getName()
                                         + "'. Should be of the format 'version-n'");

        logger.info("Acquiring write lock on '" + getName() + "':");
        fileModificationLock.writeLock().lock();
        boolean success = false;
        try {
            close();
            logger.info("Opening primary files for store '" + getName() + "' at "
                        + newStoreDirectory);

            // open the new store
            open(newDataDir);
            success = true;
        } finally {
            try {
                // we failed to do the swap, attempt a rollback
                if(!success)
                    rollback();
            } finally {
                fileModificationLock.writeLock().unlock();
                if(success)
                    logger.info("Swap operation completed successfully on store " + getName()
                                + ", releasing lock.");
                else
                    logger.error("Swap operation failed.");
            }
        }
        // okay we have released the lock and the store is now open again, it is
        // safe to do a potentially slow delete if we have one too many backups
        File extraBackup = new File(storeDir, "version-" + (maxVersion - numBackups - 1));
        if(extraBackup.exists())
            deleteAsync(extraBackup);
    }

    /**
     * Delete the given file in a separate thread
     * 
     * @param file The file to delete
     */
    public void deleteAsync(final File file) {
        new Thread(new Runnable() {

            public void run() {
                try {
                    logger.info("Deleting file " + file);
                    Utils.rm(file);
                    logger.info("Delete completed successfully.");
                } catch(Exception e) {
                    logger.error(e);
                }
            }
        }, "background-file-delete").start();
    }

    @JmxOperation(description = "Rollback to the most recent backup of the current store.")
    public void rollback() {
        logger.info("Rolling back store '" + getName() + "'");
        fileModificationLock.writeLock().lock();
        try {
            if(isOpen)
                close();
            File backup = new File(storeDir, "version-" + (maxVersion - 1));
            if(!backup.exists())
                throw new VoldemortException("Version " + (maxVersion - 1)
                                             + " does not exists, nothing to roll back to.");

            File primary = new File(storeDir, "version-" + maxVersion);
            DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
            if(primary.exists())
                Utils.move(primary, new File(storeDir, "version-" + maxVersion + "."
                                                       + df.format(new Date()) + ".bak"));
            open(backup);
        } finally {
            fileModificationLock.writeLock().unlock();
            logger.info("Rollback operation completed on '" + getName() + "', releasing lock.");
        }
    }

    public ClosableIterator<ByteArray> keys() {
        throw new UnsupportedOperationException("Iteration is not supported for "
                                                + getClass().getName());
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new UnsupportedOperationException("Iteration is not supported for "
                                                + getClass().getName());
    }

    public void truncate() {
        if(isOpen)
            close();
        Utils.rm(storeDir);
        logger.debug("Truncate successful for read-only store ");
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        byte[] keyMd5 = ByteUtils.md5(key.get());
        int chunk = fileSet.getChunkForKey(keyMd5);
        int location = searchStrategy.indexOf(fileSet.indexFileFor(chunk),
                                              keyMd5,
                                              fileSet.getIndexFileSize(chunk));
        if(location >= 0) {
            byte[] value = readValue(chunk, location);
            return Collections.singletonList(Versioned.value(value));
        } else {
            return Collections.emptyList();
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> results = StoreUtils.newEmptyHashMap(keys);
        try {
            fileModificationLock.readLock().lock();
            List<KeyValueLocation> keysAndValueLocations = Lists.newArrayList();
            for(ByteArray key: keys) {
                byte[] keyMd5 = ByteUtils.md5(key.get());
                int chunk = fileSet.getChunkForKey(keyMd5);
                int valueLocation = searchStrategy.indexOf(fileSet.indexFileFor(chunk),
                                                           keyMd5,
                                                           fileSet.getIndexFileSize(chunk));
                if(valueLocation >= 0)
                    keysAndValueLocations.add(new KeyValueLocation(chunk, key, valueLocation));
            }
            Collections.sort(keysAndValueLocations);

            for(KeyValueLocation keyVal: keysAndValueLocations) {
                byte[] value = readValue(keyVal.getChunk(), keyVal.getValueLocation());
                results.put(keyVal.getKey(), Collections.singletonList(Versioned.value(value)));
            }
            return results;
        } finally {
            fileModificationLock.readLock().unlock();
        }
    }

    private byte[] readValue(int chunk, int valueLocation) {
        FileChannel dataFile = fileSet.dataFileFor(chunk);
        try {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            dataFile.read(sizeBuffer, valueLocation);
            int size = sizeBuffer.getInt(0);
            ByteBuffer valueBuffer = ByteBuffer.allocate(size);
            dataFile.read(valueBuffer, valueLocation + 4);
            return valueBuffer.array();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    /**
     * Not supported, throws UnsupportedOperationException if called
     */
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        throw new UnsupportedOperationException("Delete is not supported on this store, it is read-only.");
    }

    /**
     * Not supported, throws UnsupportedOperationException if called
     */
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        throw new UnsupportedOperationException("Put is not supported on this store, it is read-only.");
    }

    @JmxGetter(name = "name", description = "The name of the store.")
    public String getName() {
        return name;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    private final static class KeyValueLocation implements Comparable<KeyValueLocation> {

        private final int chunk;
        private final ByteArray key;
        private final int valueLocation;

        private KeyValueLocation(int chunk, ByteArray key, int valueLocation) {
            super();
            this.chunk = chunk;
            this.key = key;
            this.valueLocation = valueLocation;
        }

        public int getChunk() {
            return chunk;
        }

        public ByteArray getKey() {
            return key;
        }

        public int getValueLocation() {
            return valueLocation;
        }

        public int compareTo(KeyValueLocation kvl) {
            if(chunk == kvl.getChunk()) {
                if(valueLocation == kvl.getValueLocation())
                    return ByteUtils.compare(getKey().get(), kvl.getKey().get());
                else
                    return Integer.signum(valueLocation - kvl.getValueLocation());
            } else {
                return getChunk() - kvl.getChunk();
            }
        }
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key));
    }
}
