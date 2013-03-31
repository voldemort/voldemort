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

package voldemort.store.bdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageInitializationException;
import voldemort.store.Store;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreUtils;
import voldemort.store.backup.NativeBackupable;
import voldemort.store.bdb.stats.BdbEnvironmentStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

/**
 * A store that uses BDB for persistence
 * 
 */
public class BdbStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> implements
        NativeBackupable {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);
    private static final Hex hexCodec = new Hex();

    private Database bdbDatabase;
    private final Environment environment;
    private final AtomicBoolean isOpen;
    private final LockMode readLockMode;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    protected final BdbEnvironmentStats bdbEnvironmentStats;
    protected final boolean minimizeScanImpact;
    protected final boolean checkpointerOffForBatchWrites;
    private volatile int numOutstandingBatchWriteJobs = 0;

    public BdbStorageEngine(String name,
                            Environment environment,
                            Database database,
                            BdbRuntimeConfig config) {
        super(name);
        this.bdbDatabase = Utils.notNull(database);
        this.environment = Utils.notNull(environment);
        this.isOpen = new AtomicBoolean(true);
        this.readLockMode = config.getLockMode();
        this.bdbEnvironmentStats = new BdbEnvironmentStats(environment,
                                                           database,
                                                           config.getStatsCacheTtlMs(),
                                                           config.getExposeSpaceUtil());
        this.minimizeScanImpact = config.getMinimizeScanImpact();
        this.checkpointerOffForBatchWrites = config.isCheckpointerOffForBatchWrites();
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            // evict data brought in by the cursor walk right away
            if(this.minimizeScanImpact)
                cursor.setCacheMode(CacheMode.EVICT_BIN);
            return new BdbEntriesIterator(cursor, this);
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            // evict data brought in by the cursor walk right away
            if(this.minimizeScanImpact)
                cursor.setCacheMode(CacheMode.EVICT_BIN);
            return new BdbKeysIterator(cursor, this);
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
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

        if(isTruncating.compareAndSet(false, true)) {
            Transaction transaction = null;
            boolean succeeded = false;

            try {
                transaction = this.environment.beginTransaction(null, null);

                // close current bdbDatabase first
                bdbDatabase.close();

                // truncate the database
                environment.truncateDatabase(transaction, this.getName(), false);
                succeeded = true;
            } catch(DatabaseException e) {
                this.bdbEnvironmentStats.reportException(e);
                logger.error(e);
                throw new VoldemortException("Failed to truncate Bdb store " + getName(), e);

            } finally {
                commitOrAbort(succeeded, transaction);
                // reopen the bdb database for future queries.
                if(reopenBdbDatabase()) {
                    isTruncating.compareAndSet(true, false);
                } else {
                    throw new VoldemortException("Failed to reopen Bdb Database after truncation, All request will fail on store "
                                                 + getName());
                }
            }
        } else {
            throw new VoldemortException("Store " + getName()
                                         + " is already truncating, cannot start another one.");
        }
    }

    private void commitOrAbort(boolean succeeded, Transaction transaction) {
        try {
            if(succeeded) {
                attemptCommit(transaction);
            } else {
                attemptAbort(transaction);
            }
        } catch(Exception e) {
            logger.error(e);
        }
    }

    /**
     * Reopens the bdb Database after a successful truncate operation.
     */
    private boolean reopenBdbDatabase() {
        try {
            bdbDatabase = environment.openDatabase(null,
                                                   this.getName(),
                                                   this.bdbDatabase.getConfig());
            return true;
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            throw new StorageInitializationException("Failed to reinitialize BdbStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    /**
     * truncate() operation mandates that all opened Database be closed before
     * attempting truncation.
     * <p>
     * This method throws an exception while truncation is happening to any
     * request attempting in parallel with store truncation.
     * 
     * @return
     */
    protected Database getBdbDatabase() {
        if(isTruncating.get()) {
            throw new VoldemortException("Bdb Store " + getName()
                                         + " is currently truncating cannot serve any request.");
        }
        return bdbDatabase;
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        DatabaseEntry valueEntry = new DatabaseEntry();

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        try {
            // uncommitted reads are perfectly fine now, since we have no
            // je-delete() in put()
            OperationStatus status = getBdbDatabase().get(null, keyEntry, valueEntry, readLockMode);
            if(OperationStatus.SUCCESS == status) {
                return StoreBinaryFormat.fromByteArray(valueEntry.getData());
            } else {
                return Collections.emptyList();
            }
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            if(logger.isTraceEnabled()) {
                logger.trace("Completed GET (" + getName() + ") from key " + key + " (keyRef: "
                             + System.identityHashCode(key) + ") in "
                             + (System.nanoTime() - startTimeNs) + " ns at "
                             + System.currentTimeMillis());
            }
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> results = null;
        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();
        try {
            results = StoreUtils.getAll(this, keys, transforms);
        } catch(PersistenceFailureException pfe) {
            throw pfe;
        } finally {
            if(logger.isTraceEnabled()) {
                String keyStr = "";
                for(ByteArray key: keys)
                    keyStr += key + " ";
                logger.trace("Completed GETALL (" + getName() + ") from keys " + keyStr + " in "
                             + (System.nanoTime() - startTimeNs) + " ns at "
                             + System.currentTimeMillis());
            }
        }

        return results;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        StoreUtils.assertValidKey(key);
        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        DatabaseEntry valueEntry = new DatabaseEntry();

        boolean succeeded = false;
        Transaction transaction = null;
        List<Versioned<byte[]>> vals = null;

        try {
            transaction = environment.beginTransaction(null, null);

            // do a get for the existing values
            OperationStatus status = getBdbDatabase().get(transaction,
                                                          keyEntry,
                                                          valueEntry,
                                                          LockMode.RMW);
            if(OperationStatus.SUCCESS == status) {
                // update
                vals = StoreBinaryFormat.fromByteArray(valueEntry.getData());
                // compare vector clocks and throw out old ones, for updates

                Iterator<Versioned<byte[]>> iter = vals.iterator();
                while(iter.hasNext()) {
                    Versioned<byte[]> curr = iter.next();
                    Occurred occurred = value.getVersion().compare(curr.getVersion());
                    if(occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Key "
                                                           + new String(hexCodec.encode(key.get()))
                                                           + " "
                                                           + value.getVersion().toString()
                                                           + " is obsolete, it is no greater than the current version of "
                                                           + curr.getVersion().toString() + ".");
                    else if(occurred == Occurred.AFTER)
                        iter.remove();
                }
            } else {
                // insert
                vals = new ArrayList<Versioned<byte[]>>();
            }

            // update the new value
            vals.add(value);

            valueEntry.setData(StoreBinaryFormat.toByteArray(vals));
            status = getBdbDatabase().put(transaction, keyEntry, valueEntry);

            if(status != OperationStatus.SUCCESS)
                throw new PersistenceFailureException("Put operation failed with status: " + status);
            succeeded = true;

        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            if(succeeded)
                attemptCommit(transaction);
            else
                attemptAbort(transaction);
            if(logger.isTraceEnabled()) {
                logger.trace("Completed PUT (" + getName() + ") to key " + key + " (keyRef: "
                             + System.identityHashCode(key) + " value " + value + " in "
                             + (System.nanoTime() - startTimeNs) + " ns at "
                             + System.currentTimeMillis());
            }
        }
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {

        StoreUtils.assertValidKey(key);

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        Transaction transaction = null;
        try {
            transaction = this.environment.beginTransaction(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(key.get());

            if(version == null) {
                // unversioned delete. Just blow away the whole thing
                OperationStatus status = getBdbDatabase().delete(transaction, keyEntry);
                if(OperationStatus.SUCCESS == status)
                    return true;
                else
                    return false;
            } else {
                // versioned deletes; need to determine what to delete
                DatabaseEntry valueEntry = new DatabaseEntry();

                // do a get for the existing values
                OperationStatus status = getBdbDatabase().get(transaction,
                                                              keyEntry,
                                                              valueEntry,
                                                              LockMode.RMW);
                // key does not exist to begin with.
                if(OperationStatus.NOTFOUND == status)
                    return false;

                List<Versioned<byte[]>> vals = StoreBinaryFormat.fromByteArray(valueEntry.getData());
                Iterator<Versioned<byte[]>> iter = vals.iterator();
                int numVersions = vals.size();
                int numDeletedVersions = 0;

                // go over the versions and remove everything before the
                // supplied version
                while(iter.hasNext()) {
                    Versioned<byte[]> curr = iter.next();
                    Version currentVersion = curr.getVersion();
                    if(currentVersion.compare(version) == Occurred.BEFORE) {
                        iter.remove();
                        numDeletedVersions++;
                    }
                }

                if(numDeletedVersions < numVersions) {
                    // we still have some valid versions
                    valueEntry.setData(StoreBinaryFormat.toByteArray(vals));
                    getBdbDatabase().put(transaction, keyEntry, valueEntry);
                } else {
                    // we have deleted all the versions; so get rid of the entry
                    // in the database
                    getBdbDatabase().delete(transaction, keyEntry);
                }
                return numDeletedVersions > 0;
            }
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptCommit(transaction);
            if(logger.isTraceEnabled()) {
                logger.trace("Completed DELETE (" + getName() + ") of key "
                             + ByteUtils.toHexString(key.get()) + " (keyRef: "
                             + System.identityHashCode(key) + ") in "
                             + (System.nanoTime() - startTimeNs) + " ns at "
                             + System.currentTimeMillis());
            }
        }
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !Store.class.isAssignableFrom(o.getClass()))
            return false;
        Store<?, ?, ?> s = (Store<?, ?, ?>) o;
        return s.getName().equals(s.getName());
    }

    @Override
    public void close() throws PersistenceFailureException {
        try {
            if(this.isOpen.compareAndSet(true, false))
                this.getBdbDatabase().close();
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    private void attemptAbort(Transaction transaction) {
        try {
            if(transaction != null)
                transaction.abort();
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error("Abort failed!", e);
        }
    }

    private void attemptCommit(Transaction transaction) {
        try {
            if(transaction != null)
                transaction.commit();
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error("Transaction commit failed!", e);
            attemptAbort(transaction);
            throw new PersistenceFailureException(e);
        }
    }

    public DatabaseStats getStats(boolean setFast) {
        try {
            StatsConfig config = new StatsConfig();
            config.setFast(setFast);
            return this.getBdbDatabase().getStats(config);
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new VoldemortException(e);
        }
    }

    @JmxOperation(description = "A variety of quickly computable stats about the BDB for this store.")
    public String getBdbStats() {
        return getBdbStats(true);
    }

    @JmxOperation(description = "A variety of stats about the BDB for this store.")
    public String getBdbStats(boolean fast) {
        String dbStats = getStats(fast).toString();
        logger.debug(dbStats);
        return dbStats;
    }

    public BdbEnvironmentStats getBdbEnvironmentStats() {
        return bdbEnvironmentStats;
    }

    protected Logger getLogger() {
        return logger;
    }

    private static class BdbEntriesIterator extends BdbIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private List<Pair<ByteArray, Versioned<byte[]>>> cache;

        public BdbEntriesIterator(Cursor cursor, BdbStorageEngine bdbEngine) {
            super(cursor, bdbEngine);
            this.cache = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        }

        @Override
        public boolean hasNext() {
            // we have a next element if there is at least one cached
            // element or we can make more
            return cache.size() > 0 || makeMore();
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(cache.size() == 0) {
                if(!makeMore())
                    throw new NoSuchElementException("Iterated to end.");
            }
            // must now have at least one thing in the cache
            return cache.remove(cache.size() - 1);
        }

        protected boolean makeMore() {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            try {
                OperationStatus status = cursor.getNext(keyEntry,
                                                        valueEntry,
                                                        LockMode.READ_UNCOMMITTED);

                if(OperationStatus.NOTFOUND == status) {
                    // we have reached the end of the cursor
                    return false;
                }
                ByteArray key = null;
                if(bdbEngine.isPartitionScanSupported())
                    key = new ByteArray(StoreBinaryFormat.extractKey(keyEntry.getData()));
                else
                    key = new ByteArray(keyEntry.getData());

                for(Versioned<byte[]> val: StoreBinaryFormat.fromByteArray(valueEntry.getData()))
                    this.cache.add(Pair.create(key, val));
                return true;
            } catch(DatabaseException e) {
                bdbEngine.bdbEnvironmentStats.reportException(e);
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
        }
    }

    private static class BdbKeysIterator extends BdbIterator<ByteArray> {

        ByteArray current = null;

        public BdbKeysIterator(Cursor cursor, BdbStorageEngine bdbEngine) {
            super(cursor, bdbEngine);
        }

        @Override
        public boolean hasNext() {
            return current != null || fetchNextKey();
        }

        @Override
        public ByteArray next() {
            ByteArray result = null;
            if(current == null) {
                if(!fetchNextKey())
                    throw new NoSuchElementException("Iterated to end.");
            }
            result = current;
            current = null;
            return result;
        }

        private boolean fetchNextKey() {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            valueEntry.setPartial(true);
            try {
                OperationStatus status = cursor.getNext(keyEntry,
                                                        valueEntry,
                                                        LockMode.READ_UNCOMMITTED);
                if(OperationStatus.NOTFOUND == status) {
                    // we have reached the end of the cursor
                    return false;
                }

                if(bdbEngine.isPartitionScanSupported())
                    current = new ByteArray(StoreBinaryFormat.extractKey(keyEntry.getData()));
                else
                    current = new ByteArray(keyEntry.getData());
                return true;
            } catch(DatabaseException e) {
                bdbEngine.bdbEnvironmentStats.reportException(e);
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
        }
    }

    @Override
    public void nativeBackup(File toDir,
                             boolean verifyFiles,
                             boolean isIncremental,
                             AsyncOperationStatus status) {
        new BdbNativeBackup(environment, verifyFiles, isIncremental).performBackup(toDir, status);
    }

    @Override
    public boolean beginBatchModifications() {
        if(checkpointerOffForBatchWrites) {
            synchronized(this) {
                numOutstandingBatchWriteJobs++;
                // turn the checkpointer off for the first job
                if(numOutstandingBatchWriteJobs == 1) {
                    logger.info("Turning checkpointer off for batch writes");
                    EnvironmentMutableConfig mConfig = environment.getMutableConfig();
                    mConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                           Boolean.toString(false));
                    environment.setMutableConfig(mConfig);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean endBatchModifications() {
        if(checkpointerOffForBatchWrites) {
            synchronized(this) {
                numOutstandingBatchWriteJobs--;
                // turn the checkpointer back on if the last job finishes
                if(numOutstandingBatchWriteJobs == 0) {
                    logger.info("Turning checkpointer on");
                    EnvironmentMutableConfig mConfig = environment.getMutableConfig();
                    mConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                           Boolean.toString(true));
                    environment.setMutableConfig(mConfig);
                    return true;
                }
            }
        }
        return false;
    }
}
