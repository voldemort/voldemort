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

import static java.lang.Math.floor;
import static java.lang.Math.log;
import static java.lang.Math.pow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.Entry;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

/**
 * A read-only store that fronts a big file
 * 
 * @author jay
 * 
 */
public class RandomAccessFileStore implements StorageEngine<byte[], byte[]> {

    private static Logger logger = Logger.getLogger(RandomAccessFileStore.class);

    public static int KEY_HASH_SIZE = 16;
    public static int POSITION_SIZE = 8;
    public static int INDEX_ENTRY_SIZE = KEY_HASH_SIZE + POSITION_SIZE;

    private final String name;
    private final long fdWaitTimeoutMs;
    private long indexFileSize;
    private final int numBackups;
    private final int numFileHandles;
    private final File storageDir;
    private final File dataFile;
    private final File indexFile;
    private final ReadWriteLock fileModificationLock;
    private final byte[][] keyCache;
    private final int maxCacheDepth;
    private final AtomicBoolean isOpen;

    private BlockingQueue<RandomAccessFile> indexFiles;
    private BlockingQueue<RandomAccessFile> dataFiles;

    /**
     * Create an instance of the store
     * 
     * @param name The name of the store
     * @param storageDir The directory in which the .data and .index files
     *        reside
     * @param numBackups The number of backups of these files to retain
     * @param numFileHandles The number of file descriptors to keep pooled for
     *        each file
     * @param fdWaitTimeoutMs The maximum time to wait to acquire a file handle
     * @param maxCacheSize The maximum size of the cache, in bytes. The actual
     *        size of the cache will be the largest power of two lower than this
     *        number
     */
    public RandomAccessFileStore(String name,
                                 File storageDir,
                                 int numBackups,
                                 int numFileHandles,
                                 long fdWaitTimeoutMs,
                                 long maxCacheSizeBytes) {
        this.storageDir = storageDir;
        this.numBackups = numBackups;
        this.indexFile = new File(storageDir, name + ".index");
        this.dataFile = new File(storageDir, name + ".data");
        this.name = Objects.nonNull(name);
        this.fdWaitTimeoutMs = fdWaitTimeoutMs;
        this.dataFiles = new ArrayBlockingQueue<RandomAccessFile>(numFileHandles);
        this.indexFiles = new ArrayBlockingQueue<RandomAccessFile>(numFileHandles);
        this.numFileHandles = numFileHandles;
        /*
         * A lock that blocks reads during swap(), open(), and close()
         * operations
         */
        this.fileModificationLock = new ReentrantReadWriteLock();
        /*
         * The overhead for each cache element is the key size + 4 byte array
         * length + 12 byte object overhead + 8 bytes for a 64-bit reference to
         * the thing
         */
        int cacheElements = (int) floor(maxCacheSizeBytes / (KEY_HASH_SIZE + 24));
        this.maxCacheDepth = (int) floor(log(cacheElements) / log(2));
        this.keyCache = new byte[(int) pow(2, maxCacheDepth)][];
        this.isOpen = new AtomicBoolean(false);
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
            boolean isClosed = isOpen.compareAndSet(false, true);
            if(!isClosed)
                throw new IllegalStateException("Attempt to open already open store.");

            /* initialize the pool of file descriptors */
            this.indexFiles = new ArrayBlockingQueue<RandomAccessFile>(numFileHandles);
            this.dataFiles = new ArrayBlockingQueue<RandomAccessFile>(numFileHandles);
            for(int i = 0; i < numFileHandles; i++) {
                indexFiles.add(new RandomAccessFile(indexFile, "r"));
                dataFiles.add(new RandomAccessFile(dataFile, "r"));
            }

            this.indexFileSize = getFileSize(indexFiles);
            long dataFileSize = getFileSize(dataFiles);

            /* sanity check file sizes */
            if(indexFileSize % INDEX_ENTRY_SIZE != 0L)
                throw new VoldemortException("Invalid index file, file length must be a multiple of "
                                             + (KEY_HASH_SIZE + POSITION_SIZE)
                                             + " but is only "
                                             + indexFileSize + " bytes.");

            if(dataFileSize < 4 * indexFileSize / INDEX_ENTRY_SIZE)
                throw new VoldemortException("Invalid data file, file length must not be less than num_index_entries * 4 bytes, but data file is only "
                                             + dataFileSize + " bytes.");

            // clear Cache now
            clearCache();
        } catch(FileNotFoundException e) {
            throw new VoldemortException("Could not open store.", e);
        } finally {
            fileModificationLock.writeLock().unlock();
        }
    }

    /**
     * Close the store.
     */
    public void close() throws VoldemortException {
        logger.debug("Close called for read-only store.");
        this.isOpen.compareAndSet(true, false);
        this.fileModificationLock.writeLock().lock();
        try {
            while(this.indexFiles.size() > 0) {
                RandomAccessFile f = this.indexFiles.take();
                f.close();
            }

            while(this.dataFiles.size() > 0) {
                RandomAccessFile f = this.dataFiles.poll();
                f.close();
            }
        } catch(IOException e) {
            throw new VoldemortException("Error while closing store.", e);
        } catch(InterruptedException e) {
            throw new VoldemortException("Interrupted while waiting for file descriptor.", e);
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
    public void swapFiles(String newIndexFile, String newDataFile) {
        logger.info("Swapping index and data files for store '" + getName() + "':");
        logger.info("Locking all reads on '" + getName() + "':");
        fileModificationLock.writeLock().lock();
        try {
            close();

            logger.info("Renaming data and index files for '" + getName() + "':");
            shiftBackups(".index");
            shiftBackups(".data");
            File firstIndexBackup = new File(storageDir, name + ".index.1");
            File firstDataBackup = new File(storageDir, name + ".data.1");
            boolean success = indexFile.getAbsoluteFile().renameTo(firstIndexBackup)
                              && dataFile.getAbsoluteFile().renameTo(firstDataBackup);
            if(!success)
                throw new VoldemortException("Error while renaming backups.");

            // copy in new files
            logger.info("Setting primary data and index files for store '" + getName() + "'to "
                        + newDataFile + " and " + newIndexFile + " respectively.");
            success = new File(newIndexFile).renameTo(indexFile)
                      && new File(newDataFile).renameTo(dataFile);
            if(!success) {
                logger.error("Failure while copying in new data files, restoring from backup and aborting.");
                success = firstIndexBackup.renameTo(indexFile)
                          && firstDataBackup.renameTo(dataFile);
                if(success) {
                    logger.error("Restored from backup.");
                    throw new VoldemortException("Failure while copying in new data files, but managed to restore from backup.");
                } else {
                    logger.error("Rollback failed too.");
                    throw new VoldemortException("Failure while copying in new data files, and restoration failed, everything is FUBAR.");
                }
            }

            open();
        } finally {
            logger.info("Swap operation completed on '" + getName() + "', releasing lock.");
            fileModificationLock.writeLock().unlock();
        }
    }

    /**
     * Shift all store backups so that the .data file becomes .data.1, .data.1
     * becomes .data.2, etc.
     * 
     * @param suffix Either .data or .index depending on which you want to shift
     */
    private void shiftBackups(String suffix) {
        // do the hokey pokey and turn the files around
        for(int i = numBackups - 1; i > 0; i--) {
            File theFile = new File(storageDir, name + suffix + "." + i);
            if(theFile.exists()) {
                File theDest = new File(storageDir, name + suffix + "." + i + 1);
                boolean succeeded = theFile.renameTo(theDest);
                if(!succeeded)
                    throw new VoldemortException("Rename of " + theFile + " to " + theDest
                                                 + " failed.");
            }
        }
    }

    /**
     * Get the size of the given file
     * 
     * @param files The pool of file handles for the file
     * @return The size of the file
     */
    private long getFileSize(BlockingQueue<RandomAccessFile> files) {
        RandomAccessFile f = null;
        try {
            f = getFile(files);
            return f.length();
        } catch(IOException e) {
            throw new VoldemortException(e);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        } finally {
            if(f != null)
                files.add(f);
        }
    }

    public ClosableIterator<Entry<byte[], Versioned<byte[]>>> entries() {
        throw new UnsupportedOperationException("Iteration is not supported for "
                                                + getClass().getName());
    }

    /**
     * The get method provided by this store
     */
    public List<Versioned<byte[]>> get(byte[] key) throws VoldemortException {
        RandomAccessFile index = null;
        RandomAccessFile data = null;
        try {
            fileModificationLock.readLock().lock();
            index = getFile(indexFiles);
            long valueLocation = getValueLocation(index, key);
            if(valueLocation < 0) {
                return Collections.emptyList();
            } else {
                data = getFile(dataFiles);
                data.seek(valueLocation);
                int size = data.readInt();
                byte[] value = new byte[size];
                data.readFully(value);
                return Collections.singletonList(new Versioned<byte[]>(value, new VectorClock()));
            }
        } catch(InterruptedException e) {
            throw new VoldemortException("Thread was interrupted.", e);
        } catch(IOException e) {
            throw new PersistenceFailureException(e);
        } finally {
            fileModificationLock.readLock().unlock();
            if(index != null)
                indexFiles.add(index);
            if(data != null)
                dataFiles.add(data);
        }
    }

    public void clearCache() {
        try {
            logger.info("Clearing cache.");
            this.fileModificationLock.readLock().lock();
            for(int i = 0; i < keyCache.length; i++)
                this.keyCache[i] = null;
        } finally {
            this.fileModificationLock.readLock().unlock();
        }
    }

    public void preloadCache() {
        logger.info("Starting cache preloading...");
        try {
            this.fileModificationLock.readLock().lock();
            RandomAccessFile index = this.getFile(this.indexFiles);
            preloadCache(index,
                         0,
                         0L,
                         this.indexFileSize / INDEX_ENTRY_SIZE - 1,
                         1,
                         this.maxCacheDepth);
            logger.info("Cache loading complete.");
        } catch(IOException e) {
            throw new PersistenceFailureException(e);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        } finally {
            this.fileModificationLock.readLock().unlock();
        }
    }

    public void preloadCache(RandomAccessFile index,
                             int cacheIndex,
                             long low,
                             long high,
                             int currentDepth,
                             int maxDepth) throws IOException {
        long mid = (low + high) / 2;

        // go left if we aren't too deep and we aren't at a leaf
        if(currentDepth < maxDepth && low <= mid - 1)
            preloadCache(index, 2 * cacheIndex + 1, low, mid - 1, currentDepth + 1, maxDepth);

        // cache the current item
        byte[] key = new byte[KEY_HASH_SIZE];
        readFrom(index, mid * INDEX_ENTRY_SIZE, key);
        this.keyCache[cacheIndex] = key;

        // go right if we aren't too deep and we aren't at a leaf
        if(currentDepth < maxDepth && mid + 1 <= high)
            preloadCache(index, 2 * cacheIndex + 2, mid + 1, high, currentDepth + 1, maxDepth);
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
    private long getValueLocation(RandomAccessFile index, byte[] key) throws IOException,
            InterruptedException {
        byte[] keyMd5 = ByteUtils.md5(key);
        byte[] keyBuffer = new byte[KEY_HASH_SIZE];
        long low = 0;
        long high = indexFileSize / INDEX_ENTRY_SIZE - 1;
        int cacheIndex = 0;
        int depth = 0;
        while(low <= high) {
            depth++;
            long mid = (low + high) / 2;

            byte[] foundKey = readKey(index, mid * INDEX_ENTRY_SIZE, keyBuffer, depth, cacheIndex);

            int cmp = ByteUtils.compare(foundKey, keyMd5);
            if(cmp == 0) {
                // they are equal, return the location stored here
                index.seek(mid * INDEX_ENTRY_SIZE + KEY_HASH_SIZE);
                return index.readLong();
            } else if(cmp > 0) {
                // midVal is bigger
                high = mid - 1;
                cacheIndex = 2 * cacheIndex + 1;
            } else if(cmp < 0) {
                // the keyMd5 is bigger
                low = mid + 1;
                cacheIndex = 2 * cacheIndex + 2;
            }
        }

        return -1;
    }

    /*
     * Read the key, potentially from the cache
     */
    private byte[] readKey(RandomAccessFile index,
                           long indexByteOffset,
                           byte[] foundKey,
                           int depth,
                           int cacheIndex) throws IOException {
        if(depth < maxCacheDepth) {
            // do cached lookup
            if(keyCache[cacheIndex] == null) {
                readFrom(index, indexByteOffset, foundKey);
                // this is a race condition, but not a harmful one:
                keyCache[cacheIndex] = ByteUtils.copy(foundKey, 0, foundKey.length);
            }
            return keyCache[cacheIndex];
        } else {
            // do direct lookup
            readFrom(index, indexByteOffset, foundKey);
            return foundKey;
        }
    }

    /*
     * Seek to the given object and read into the buffer exactly buffer.length
     * bytes
     */
    private static void readFrom(RandomAccessFile file, long indexByteOffset, byte[] buffer)
            throws IOException {
        file.seek(indexByteOffset);
        file.readFully(buffer);
    }

    /**
     * Not supported, throws UnsupportedOperationException if called
     */
    public boolean delete(byte[] key, Version version) throws VoldemortException {
        throw new UnsupportedOperationException("Delete is not supported on this store, it is read-only.");
    }

    /**
     * Not supported, throws UnsupportedOperationException if called
     */
    public void put(byte[] key, Versioned<byte[]> value) throws VoldemortException {
        throw new UnsupportedOperationException("Put is not supported on this store, it is read-only.");
    }

    @JmxGetter(name = "name", description = "The name of the store.")
    public String getName() {
        return name;
    }

    private RandomAccessFile getFile(BlockingQueue<RandomAccessFile> files)
            throws InterruptedException {
        RandomAccessFile file = files.poll(fdWaitTimeoutMs, TimeUnit.MILLISECONDS);
        if(file == null)
            throw new VoldemortException("Timeout after waiting for " + fdWaitTimeoutMs
                                         + " ms to acquire file descriptor");
        else
            return file;
    }

    @JmxGetter(name = "dataFile", description = "The name of the file currently storing data for this store.")
    public String getDataFileName() {
        return this.dataFile.getAbsolutePath();
    }

    @JmxGetter(name = "indexFile", description = "The name of the file currently storing the index for this store.")
    public String getIndexFileName() {
        return this.indexFile.getAbsolutePath();
    }

}
