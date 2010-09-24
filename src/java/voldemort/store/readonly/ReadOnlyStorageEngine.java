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
import voldemort.routing.RoutingStrategy;
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

    private final String name;
    private final int numBackups, nodeId;
    private long currentVersionId;
    private final File storeDir;
    private final ReadWriteLock fileModificationLock;
    private final SearchStrategy searchStrategy;
    private RoutingStrategy routingStrategy;
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
                                 RoutingStrategy routingStrategy,
                                 int nodeId,
                                 File storeDir,
                                 int numBackups) {
        this.storeDir = storeDir;
        this.numBackups = numBackups;
        this.name = Utils.notNull(name);
        this.searchStrategy = searchStrategy;
        this.routingStrategy = Utils.notNull(routingStrategy);
        this.nodeId = nodeId;
        this.fileSet = null;
        this.currentVersionId = 0L;
        /*
         * A lock that blocks reads during swap(), open(), and close()
         * operations
         */
        this.fileModificationLock = new ReentrantReadWriteLock();
        this.isOpen = false;
        open(null);
    }

    /**
     * Open the store with the version directory specified. If null is specified
     * we open the directory with the maximum version
     * 
     * @param versionDir Version Directory to open. If null, we open the max
     *        versioned / latest directory
     */
    public void open(File versionDir) {
        /* acquire modification lock */
        fileModificationLock.writeLock().lock();
        try {
            /* check that the store is currently closed */
            if(isOpen)
                throw new IllegalStateException("Attempt to open already open store.");

            // Find version directory from symbolic link or max version id
            if(versionDir == null) {
                versionDir = getCurrentVersion();

                if(versionDir == null)
                    versionDir = new File(storeDir, "version-0");
            }

            // Set the max version id
            long versionId = ReadOnlyUtils.getVersionId(versionDir);
            if(versionId == -1) {
                throw new VoldemortException("Unable to parse id from version directory "
                                             + versionDir.getAbsolutePath());
            }
            currentVersionId = versionId;
            Utils.mkdirs(versionDir);

            // Create symbolic link
            logger.info("Creating symbolic link for '" + getName() + "' using directory "
                        + versionDir.getAbsolutePath());
            Utils.symlink(versionDir.getAbsolutePath(), storeDir.getAbsolutePath() + File.separator
                                                        + "latest");
            this.fileSet = new ChunkedFileSet(versionDir, routingStrategy, nodeId);
            isOpen = true;
        } finally {
            fileModificationLock.writeLock().unlock();
        }
    }

    /**
     * Set the routing strategy required to find which partition the key belongs
     * to
     */
    public void setRoutingStrategy(RoutingStrategy routingStrategy) {
        if(this.fileSet == null)
            throw new VoldemortException("File set should not be null");

        this.routingStrategy = routingStrategy;
    }

    /**
     * Retrieve the dir pointed to by 'latest' symbolic-link or the current
     * version dir
     * 
     * @return Current version directory, else null
     */
    private File getCurrentVersion() {
        File latestDir = ReadOnlyUtils.getLatestDir(storeDir);
        if(latestDir != null)
            return latestDir;

        File[] versionDirs = ReadOnlyUtils.getVersionDirs(storeDir);
        if(versionDirs == null || versionDirs.length == 0) {
            return null;
        } else {
            return ReadOnlyUtils.findKthVersionedDir(versionDirs,
                                                     versionDirs.length - 1,
                                                     versionDirs.length - 1)[0];
        }
    }

    public String getCurrentDirPath() {
        return storeDir.getAbsolutePath() + File.separator + "version-"
               + Long.toString(currentVersionId);
    }

    public long getCurrentVersionId() {
        return currentVersionId;
    }

    public String getStoreDirPath() {
        return storeDir.getAbsolutePath();
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
     * Swap the current version folder for a new one
     * 
     * @param newStoreDirectory The path to the new version directory
     */
    @JmxOperation(description = "swapFiles(newStoreDirectory) changes this store "
                                + " to use the new data directory.")
    public void swapFiles(String newStoreDirectory) {
        logger.info("Swapping files for store '" + getName() + "' to " + newStoreDirectory);
        File newVersionDir = new File(newStoreDirectory);

        if(!newVersionDir.exists())
            throw new VoldemortException("File " + newVersionDir.getAbsolutePath()
                                         + " does not exist.");

        if(!(newVersionDir.getParentFile().compareTo(storeDir.getAbsoluteFile()) == 0 && ReadOnlyUtils.checkVersionDirName(newVersionDir)))
            throw new VoldemortException("Invalid version folder name '"
                                         + newVersionDir
                                         + "'. Either parent directory is incorrect or format(version-n) is incorrect");

        // retrieve previous version for (a) check if last write is winning
        // (b) if failure, rollback use
        File previousVersionDir = getCurrentVersion();
        if(previousVersionDir == null)
            throw new VoldemortException("Could not find any latest directory to swap with in store '"
                                         + getName() + "'");

        long newVersionId = ReadOnlyUtils.getVersionId(newVersionDir);
        long previousVersionId = ReadOnlyUtils.getVersionId(previousVersionDir);
        if(newVersionId == -1 || previousVersionId == -1)
            throw new VoldemortException("Unable to parse folder names (" + newVersionDir.getName()
                                         + "," + previousVersionDir.getName()
                                         + ") since format(version-n) is incorrect");

        // check if we're greater than latest since we want last write to win
        if(previousVersionId > newVersionId) {
            logger.info("No swap required since current latest version " + previousVersionId
                        + " is greater than swap version " + newVersionId);
            deleteBackups();
            return;
        }

        logger.info("Acquiring write lock on '" + getName() + "':");
        fileModificationLock.writeLock().lock();
        boolean success = false;
        try {
            close();
            logger.info("Opening primary files for store '" + getName() + "' at "
                        + newStoreDirectory);

            // open the latest store
            open(newVersionDir);
            success = true;
        } finally {
            try {
                // we failed to do the swap, attempt a rollback to last version
                if(!success)
                    rollback(previousVersionDir);

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
        deleteBackups();
    }

    /**
     * Delete all backups asynchronously
     */
    private void deleteBackups() {
        File[] storeDirList = ReadOnlyUtils.getVersionDirs(storeDir, 0L, currentVersionId);
        if(storeDirList.length > (numBackups + 1)) {
            // delete ALL old directories asynchronously
            File[] extraBackups = ReadOnlyUtils.findKthVersionedDir(storeDirList,
                                                                    0,
                                                                    storeDirList.length
                                                                            - (numBackups + 1) - 1);
            if(extraBackups != null) {
                for(File backUpFile: extraBackups) {
                    deleteAsync(backUpFile);
                }
            }
        }
    }

    /**
     * Delete the given file in a separate thread
     * 
     * @param file The file to delete
     */
    private void deleteAsync(final File file) {
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

    /**
     * Rollback to the specified push version
     * 
     * @param rollbackToDir The version directory to rollback to
     */
    @JmxOperation(description = "Rollback to a previous version")
    public void rollback(File rollbackToDir) {
        logger.info("Rolling back store '" + getName() + "'");
        fileModificationLock.writeLock().lock();
        try {
            if(rollbackToDir == null || !rollbackToDir.exists())
                throw new VoldemortException("Version directory specified to rollback to does not exist or is null");

            long versionId = ReadOnlyUtils.getVersionId(rollbackToDir);
            if(versionId == -1)
                throw new VoldemortException("Cannot parse version id");

            File[] backUpDirs = ReadOnlyUtils.getVersionDirs(storeDir, versionId, Long.MAX_VALUE);
            if(backUpDirs.length <= 1) {
                logger.warn("No rollback performed since there are no back-up directories");
                return;
            }
            backUpDirs = ReadOnlyUtils.findKthVersionedDir(backUpDirs, 0, backUpDirs.length - 1);

            if(isOpen)
                close();

            // open the rollback directory
            open(rollbackToDir);

            // back-up all other directories
            DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
            for(int index = 1; index < backUpDirs.length; index++) {
                Utils.move(backUpDirs[index], new File(storeDir, backUpDirs[index].getName() + "."
                                                                 + df.format(new Date()) + ".bak"));
            }

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
        try {
            fileModificationLock.readLock().lock();
            int chunk = fileSet.getChunkForKey(key.get());
            int location = searchStrategy.indexOf(fileSet.indexFileFor(chunk),
                                                  ByteUtils.md5(key.get()),
                                                  fileSet.getIndexFileSize(chunk));
            if(location >= 0) {
                byte[] value = readValue(chunk, location);
                return Collections.singletonList(Versioned.value(value));
            } else {
                return Collections.emptyList();
            }
        } finally {
            fileModificationLock.readLock().unlock();
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
                int chunk = fileSet.getChunkForKey(key.get());
                int valueLocation = searchStrategy.indexOf(fileSet.indexFileFor(chunk),
                                                           ByteUtils.md5(key.get()),
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
