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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.VoldemortUnsupportedOperationalException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.routing.RoutingStrategy;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StoreUtils;
import voldemort.store.readonly.chunk.ChunkedFileSet;
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
public class ReadOnlyStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private static Logger logger = Logger.getLogger(ReadOnlyStorageEngine.class);

    private final int numBackups, nodeId;
    private long currentVersionId;
    private final File storeDir;
    private final ReadWriteLock fileModificationLock;
    private final SearchStrategy searchStrategy;
    private RoutingStrategy routingStrategy;
    private volatile ChunkedFileSet fileSet;
    private volatile boolean isOpen;
    private int deleteBackupMs = 0;
    private long lastSwapped;
    private boolean enforceMlock = false;

    /**
     * Create an instance of the store
     * 
     * @param name The name of the store
     * @param searchStrategy The algorithm to use for searching for keys
     * @param routingStrategy The routing strategy used to route keys
     * @param nodeId Node id
     * @param storeDir The directory in which the .data and .index files reside
     * @param numBackups The number of backups of these files to retain
     * @param deleteBackupMs The time in ms for which we'll wait before we
     *        delete a backup
     */
    public ReadOnlyStorageEngine(String name,
                                 SearchStrategy searchStrategy,
                                 RoutingStrategy routingStrategy,
                                 int nodeId,
                                 File storeDir,
                                 int numBackups,
                                 int deleteBackupMs) {
        this(name, searchStrategy, routingStrategy, nodeId, storeDir, numBackups);
        this.deleteBackupMs = deleteBackupMs;
    }

    /**
     * Create an instance of the store
     * 
     * @param name The name of the store
     * @param searchStrategy The algorithm to use for searching for keys
     * @param routingStrategy The routing strategy used to route keys
     * @param nodeId Node id
     * @param storeDir The directory in which the .data and .index files reside
     * @param numBackups The number of backups of these files to retain
     */
    public ReadOnlyStorageEngine(String name,
                                 SearchStrategy searchStrategy,
                                 RoutingStrategy routingStrategy,
                                 int nodeId,
                                 File storeDir,
                                 int numBackups) {
        this(name, searchStrategy, routingStrategy, nodeId, storeDir, numBackups, 0, false);
    }

    /*
     * Overload constructor to accept the mlock config
     */
    public ReadOnlyStorageEngine(String name,
                                 SearchStrategy searchStrategy,
                                 RoutingStrategy routingStrategy,
                                 int nodeId,
                                 File storeDir,
                                 int numBackups,
                                 int deleteBackupMs,
                                 boolean enforceMlock) {

        super(name);
        this.enforceMlock = enforceMlock;
        this.storeDir = storeDir;
        this.numBackups = numBackups;
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
     * Returns the internal chunked file set
     * 
     * @return The chunked file set
     */
    public ChunkedFileSet getChunkedFileSet() {
        return this.fileSet;
    }

    /**
     * Returns a string representation of map of chunk id to number of chunks
     * 
     * @return String of map of chunk id to number of chunks
     */
    @JmxGetter(name = "getChunkIdToNumChunks", description = "Returns a string representation of the map of chunk id to number of chunks")
    public String getChunkIdToNumChunks() {
        StringBuilder builder = new StringBuilder();
        for(Entry<Object, Integer> entry: fileSet.getChunkIdToNumChunks().entrySet()) {
            builder.append(entry.getKey().toString() + " - " + entry.getValue().toString() + ", ");
        }
        return builder.toString();
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
                versionDir = ReadOnlyUtils.getCurrentVersion(storeDir);

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
            this.fileSet = new ChunkedFileSet(versionDir, routingStrategy, nodeId, enforceMlock);
            this.lastSwapped = System.currentTimeMillis();
            this.isOpen = true;
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
     * Retrieve the absolute path of the current version
     * 
     * @return Returns the absolute path of the current dir
     */
    public String getCurrentDirPath() {
        return storeDir.getAbsolutePath() + File.separator + "version-"
               + Long.toString(currentVersionId);
    }

    /**
     * Retrieve the version id of the current directory
     * 
     * @return Returns a long indicating the version number
     */
    public long getCurrentVersionId() {
        return currentVersionId;
    }

    /**
     * Retrieves the path of the store
     * 
     * @return The absolute path of the store
     */
    public String getStoreDirPath() {
        return storeDir.getAbsolutePath();
    }

    /**
     * Retrieve the storage format of RO
     * 
     * @return The type of the storage format
     */
    public ReadOnlyStorageFormat getReadOnlyStorageFormat() {
        return fileSet.getReadOnlyStorageFormat();
    }

    /**
     * Time since last time the store was swapped
     * 
     * @return Time in milliseconds since the store was swapped
     */
    @JmxGetter(name = "lastSwapped", description = "Time in milliseconds since the store was swapped")
    public long getLastSwapped() {
        long timeSinceLastSwap = System.currentTimeMillis() - lastSwapped;
        return timeSinceLastSwap > 0 ? timeSinceLastSwap : 0;
    }

    /**
     * Close the store.
     */
    @Override
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
    @JmxOperation(description = "swapFiles changes this store to use the new data directory")
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
        File previousVersionDir = ReadOnlyUtils.getCurrentVersion(storeDir);
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
        if(storeDirList != null && storeDirList.length > (numBackups + 1)) {
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

            @Override
            public void run() {
                try {
                    try {
                        logger.info("Waiting for " + deleteBackupMs
                                    + " milliseconds before deleting " + file.getAbsolutePath());
                        Thread.sleep(deleteBackupMs);
                    } catch(InterruptedException e) {
                        logger.warn("Did not sleep enough before deleting backups");
                    }
                    logger.info("Deleting file " + file.getAbsolutePath());
                    Utils.rm(file);
                    logger.info("Deleting of " + file.getAbsolutePath()
                                + " completed successfully.");
                } catch(Exception e) {
                    logger.error(e);
                }
            }
        }, "background-file-delete").start();
    }

    /**
     * Rollback to the specified push version
     * 
     * @param rollbackToDir The full path of the version directory to rollback
     *        to
     */
    @JmxOperation(description = "Rollback to a previous version directory ( full path ) ")
    public void rollback(String rollbackToDir) {
        rollback(new File(rollbackToDir));
    }

    /**
     * Rollback to the specified push version
     * 
     * @param rollbackToDir The version directory to rollback to
     */
    public void rollback(File rollbackToDir) {
        logger.info("Rolling back store '" + getName() + "'");
        fileModificationLock.writeLock().lock();
        try {
            if(rollbackToDir == null)
                throw new VoldemortException("Version directory specified to rollback is null");

            if(!rollbackToDir.exists())
                throw new VoldemortException("Version directory " + rollbackToDir.getAbsolutePath()
                                             + " specified to rollback does not exist");

            long versionId = ReadOnlyUtils.getVersionId(rollbackToDir);
            if(versionId == -1)
                throw new VoldemortException("Cannot parse version id");

            File[] backUpDirs = ReadOnlyUtils.getVersionDirs(storeDir, versionId, Long.MAX_VALUE);
            if(backUpDirs == null || backUpDirs.length <= 1) {
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

    @Override
    public ClosableIterator<ByteArray> keys() {
        if(!(fileSet.getReadOnlyStorageFormat().compareTo(ReadOnlyStorageFormat.READONLY_V2) == 0))
            throw new UnsupportedOperationException("Iteration is not supported for "
                                                    + getClass().getName()
                                                    + " with storage format "
                                                    + fileSet.getReadOnlyStorageFormat());
        return new ChunkedFileSet.ROKeyIterator(fileSet, fileModificationLock);
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        if(!(fileSet.getReadOnlyStorageFormat().compareTo(ReadOnlyStorageFormat.READONLY_V2) == 0))
            throw new UnsupportedOperationException("Iteration is not supported for "
                                                    + getClass().getName()
                                                    + " with storage format "
                                                    + fileSet.getReadOnlyStorageFormat());
        return new ChunkedFileSet.ROEntriesIterator(fileSet, fileModificationLock);
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public void truncate() {
        if(isOpen)
            close();
        Utils.rm(storeDir);
        logger.debug("Truncate successful for read-only store ");
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            fileModificationLock.readLock().lock();
            int chunk = fileSet.getChunkForKey(key.get());
            if(chunk < 0) {
                logger.warn("Invalid chunk id returned. Either routing strategy is inconsistent or storage format not understood");
                return Collections.emptyList();
            }
            int location = searchStrategy.indexOf(fileSet.indexFileFor(chunk),
                                                  fileSet.keyToStorageFormat(key.get()),
                                                  fileSet.getIndexFileSize(chunk));
            if(location >= 0) {
                byte[] value = fileSet.readValue(key.get(), chunk, location);
                if(value.length == 0) {
                    return Collections.emptyList();
                } else {
                    return Collections.singletonList(Versioned.value(value));
                }
            } else {
                return Collections.emptyList();
            }
        } finally {
            fileModificationLock.readLock().unlock();
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> results = StoreUtils.newEmptyHashMap(keys);
        try {
            fileModificationLock.readLock().lock();
            List<KeyValueLocation> keysAndValueLocations = Lists.newArrayList();
            for(ByteArray key: keys) {
                int chunk = fileSet.getChunkForKey(key.get());
                int valueLocation = searchStrategy.indexOf(fileSet.indexFileFor(chunk),
                                                           fileSet.keyToStorageFormat(key.get()),
                                                           fileSet.getIndexFileSize(chunk));
                if(valueLocation >= 0)
                    keysAndValueLocations.add(new KeyValueLocation(chunk, key, valueLocation));
            }
            Collections.sort(keysAndValueLocations);

            for(KeyValueLocation keyVal: keysAndValueLocations) {
                byte[] value = fileSet.readValue(keyVal.getKey().get(),
                                                 keyVal.getChunk(),
                                                 keyVal.getValueLocation());
                if(value.length > 0)
                    results.put(keyVal.getKey(), Collections.singletonList(Versioned.value(value)));
            }
            return results;
        } finally {
            fileModificationLock.readLock().unlock();
        }
    }

    /**
     * Not supported, throws UnsupportedOperationException if called
     */
    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        throw new UnsupportedOperationException("Delete is not supported on this store, it is read-only.");
    }

    /**
     * Not supported, throws UnsupportedOperationException if called
     */
    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        throw new VoldemortUnsupportedOperationalException("Put is not supported on this store, it is read-only.");
    }

    @JmxGetter(name = "name", description = "The name of the store.")
    @Override
    public String getName() {
        return super.getName();
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

        @Override
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

    @Override
    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    @Override
    public boolean isPartitionAware() {
        return true;
    }
}
