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
import java.nio.MappedByteBuffer;
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
 * @author jay
 * 
 */
public class ReadOnlyStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static Logger logger = Logger.getLogger(ReadOnlyStorageEngine.class);

    public static final int KEY_HASH_SIZE = 16;
    public static final int POSITION_SIZE = 4;
    public static final int INDEX_ENTRY_SIZE = KEY_HASH_SIZE + POSITION_SIZE;

    /*
     * The overhead for each cache element is the key size + 4 byte array length
     * + 12 byte object overhead + 8 bytes for a 64-bit reference to the thing
     */
    public static final int MEMORY_OVERHEAD_PER_KEY = KEY_HASH_SIZE + 4 + 12 + 8;

    private final String name;
    private final int numBackups;
    private final int numFileHandles;
    private final long bufferWaitTimeoutMs;
    private final File storeDir;
    private final ReadWriteLock fileModificationLock;
    private volatile ChunkedFileSet fileSet;
    private volatile boolean isOpen;

    /**
     * Create an instance of the store
     * 
     * @param name The name of the store
     * @param storageDir The directory in which the .data and .index files
     *        reside
     * @param numBackups The number of backups of these files to retain
     * @param numFileHandles The number of file descriptors to keep pooled for
     *        each file
     * @param bufferWaitTimeoutMs The maximum time to wait to acquire a file
     *        handle
     * @param maxCacheSizeBytes The maximum size of the cache, in bytes. The
     *        actual size of the cache will be the largest power of two lower
     *        than this number
     */
    public ReadOnlyStorageEngine(String name,
                                 File storeDir,
                                 int numBackups,
                                 int numFileHandles,
                                 long bufferWaitTimeoutMs) {
        this.bufferWaitTimeoutMs = bufferWaitTimeoutMs;
        this.numFileHandles = numFileHandles;
        this.storeDir = storeDir;
        this.numBackups = numBackups;
        this.name = Utils.notNull(name);
        this.fileSet = null;
        /*
         * A lock that blocks reads during swap(), open(), and close()
         * operations
         */
        this.fileModificationLock = new ReentrantReadWriteLock();
        this.isOpen = false;
        open();
    }

    /**
     * Open the store
     */
    public void open() {
        /* acquire modification lock */
        fileModificationLock.writeLock().lock();
        try {
            /* check that the store is currently closed */
            if(isOpen)
                throw new IllegalStateException("Attempt to open already open store.");

            File version0 = new File(storeDir, "version-0");
            version0.mkdirs();
            this.fileSet = new ChunkedFileSet(version0,
                                              this.numFileHandles,
                                              this.bufferWaitTimeoutMs);
            isOpen = true;
        } finally {
            fileModificationLock.writeLock().unlock();
        }
    }

    /**
     * Close the store.
     */
    public void close() throws VoldemortException {
        logger.debug("Close called for read-only store.");
        this.fileModificationLock.writeLock().lock();
        if(!isOpen) {
            fileModificationLock.writeLock().unlock();
            throw new IllegalStateException("Attempt to close non-open store.");
        }
        try {
            this.isOpen = false;
            fileSet.close();
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
    @JmxOperation(description = "swapFiles(newIndexFile, newDataFile) changes this store "
                                + " to use the given index and data file.")
    public void swapFiles(String newStoreDirectory) {
        logger.info("Swapping files for store '" + getName() + "' from " + newStoreDirectory);
        File newDataDir = new File(newStoreDirectory);
        if(!newDataDir.exists())
            throw new VoldemortException("File " + newDataDir.getAbsolutePath()
                                         + " does not exist.");

        logger.info("Acquiring write lock on '" + getName() + "':");
        fileModificationLock.writeLock().lock();
        boolean success = false;
        try {
            close();
            logger.info("Renaming data and index files for '" + getName() + "':");
            shiftBackupsRight();
            // copy in new files
            logger.info("Setting primary files for store '" + getName() + "' to "
                        + newStoreDirectory);
            File destDir = new File(storeDir, "version-0");
            success = newDataDir.renameTo(destDir);

            // open the new store
            if(success) {
                try {
                    open();
                } catch(Exception e) {
                    logger.error(e);
                    success = false;
                }
            } else {
                logger.error("Renaming " + newDataDir.getAbsolutePath() + " to "
                             + destDir.getAbsolutePath() + " failed!");
            }
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
        File extraBackup = new File(storeDir, "version-" + (numBackups + 1));
        if(extraBackup.exists()) {
            logger.info("Deleting oldest backup file " + extraBackup);
            Utils.rm(extraBackup);
            logger.info("Delete completed successfully.");
        }
    }

    @JmxOperation(description = "Rollback to the most recent backup of the current store.")
    public void rollback() {
        logger.info("Rolling back store '" + getName() + "' to version 1.");
        fileModificationLock.writeLock().lock();
        try {
            if(isOpen)
                close();
            File backup = new File(storeDir, "version-1");
            if(!backup.exists())
                throw new VoldemortException("Version 1 does not exists, nothing to roll back to.");
            shiftBackupsLeft();
            open();
        } finally {
            fileModificationLock.writeLock().unlock();
            logger.info("Rollback operation completed on '" + getName() + "', releasing lock.");
        }
    }

    /**
     * Shift all store versions so that 1 becomes 0, 2 becomes 1, etc.
     */
    private void shiftBackupsLeft() {
        if(isOpen)
            throw new VoldemortException("Can't move backup files while store is open.");

        // Turn the current data into a .bak so we can take a look at it
        // manually if we want
        File primary = new File(storeDir, "version-0");
        DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        if(primary.exists())
            Utils.move(primary, new File(storeDir, "version-0." + df.format(new Date()) + ".bak"));

        shiftBackupsLeft(0);
    }

    private void shiftBackupsLeft(int beginShift) {
        File source = new File(storeDir, "version-" + Integer.toString(beginShift + 1));
        File dest = new File(storeDir, "version-" + Integer.toString(beginShift));

        // if the source file doesn't exist there is nothing to shift
        if(!source.exists())
            return;

        // rename the file
        source.renameTo(dest);

        // now rename any remaining files
        shiftBackupsLeft(beginShift + 1);
    }

    /**
     * Shift all store versions so that 0 becomes 1, 1 becomes 2, etc.
     */
    private void shiftBackupsRight() {
        if(isOpen)
            throw new VoldemortException("Can't move backup files while store is open.");
        shiftBackupsRight(0);
    }

    private void shiftBackupsRight(int beginShift) {
        if(isOpen)
            throw new VoldemortException("Can't move backup files while store is open.");

        File source = new File(storeDir, "version-" + Integer.toString(beginShift));

        // if the source file doesn't exist there is nothing to shift
        if(!source.exists())
            return;

        // if the dest file exists, it will need to be shifted too
        File dest = new File(storeDir, "version-" + Integer.toString(beginShift + 1));
        if(dest.exists())
            shiftBackupsRight(beginShift + 1);

        // okay finally do the rename
        source.renameTo(dest);
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        throw new UnsupportedOperationException("Iteration is not supported for "
                                                + getClass().getName());
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        byte[] keyMd5 = ByteUtils.md5(key.get());
        int chunk = fileSet.getChunkForKey(keyMd5);
        int location = getValueLocation(chunk, keyMd5);
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
                int valueLocation = getValueLocation(chunk, keyMd5);
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
        FileChannel dataFile = fileSet.getDataFile(chunk);
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
     * Get the byte offset in the data file at which the given key is stored
     * 
     * @param index The index file
     * @param key The key to lookup
     * @return The offset into the file.
     * @throws IOException
     * @throws InterruptedException
     */
    private int getValueLocation(int chunk, byte[] keyMd5) {
        MappedByteBuffer index = fileSet.checkoutIndexFile(chunk);
        int indexFileSize = fileSet.getIndexFileSize(chunk);
        try {
            byte[] keyBuffer = new byte[KEY_HASH_SIZE];
            int low = 0;
            int high = indexFileSize / INDEX_ENTRY_SIZE - 1;
            while(low <= high) {
                int mid = (low + high) / 2;
                byte[] foundKey = readKey(index, mid * INDEX_ENTRY_SIZE, keyBuffer);
                int cmp = ByteUtils.compare(foundKey, keyMd5);
                if(cmp == 0) {
                    // they are equal, return the location stored here
                    index.position(mid * INDEX_ENTRY_SIZE + KEY_HASH_SIZE);
                    return index.getInt();
                } else if(cmp > 0) {
                    // midVal is bigger
                    high = mid - 1;
                } else if(cmp < 0) {
                    // the keyMd5 is bigger
                    low = mid + 1;
                }
            }
            return -1;
        } finally {
            fileSet.checkinIndexFile(index, chunk);
        }
    }

    /*
     * Read the key, potentially from the cache
     */
    private byte[] readKey(MappedByteBuffer index, int indexByteOffset, byte[] foundKey) {
        index.position(indexByteOffset);
        index.get(foundKey);
        return foundKey;
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
