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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.VersionedSerializer;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
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
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;

/**
 * A store that uses BDB for persistence
 * 
 * 
 */
public class BdbStorageEngine implements StorageEngine<ByteArray, byte[], byte[]>, NativeBackupable {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);
    private static final Hex hexCodec = new Hex();

    private final String name;
    private Database bdbDatabase;
    private final Environment environment;
    private final VersionedSerializer<byte[]> versionedSerializer;
    private final AtomicBoolean isOpen;
    private final LockMode readLockMode;
    private final Serializer<Version> versionSerializer;
    private final BdbEnvironmentStats bdbEnvironmentStats;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    public BdbStorageEngine(String name,
                            Environment environment,
                            Database database,
                            BdbRuntimeConfig config) {
        this.name = Utils.notNull(name);
        this.bdbDatabase = Utils.notNull(database);
        this.environment = Utils.notNull(environment);
        this.versionedSerializer = new VersionedSerializer<byte[]>(new IdentitySerializer());
        this.versionSerializer = new Serializer<Version>() {

            public byte[] toBytes(Version object) {
                return ((VectorClock) object).toBytes();
            }

            public Version toObject(byte[] bytes) {
                return versionedSerializer.getVersion(bytes);
            }
        };
        this.isOpen = new AtomicBoolean(true);
        this.readLockMode = config.getLockMode();
        this.bdbEnvironmentStats = new BdbEnvironmentStats(environment, config.getStatsCacheTtlMs());
    }

    public String getName() {
        return name;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            return new BdbEntriesIterator(cursor);
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    public ClosableIterator<ByteArray> keys() {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            return new BdbKeysIterator(cursor);
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

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
            throw new StorageInitializationException("Failed to reinitialize BdbStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    public List<Version> getVersions(ByteArray key) {
        return get(key, null, readLockMode, versionSerializer);
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        return get(key, transforms, readLockMode, versionedSerializer);
    }

    private <T> List<T> get(ByteArray key,
                            @SuppressWarnings("unused") byte[] transforms,
                            LockMode lockMode,
                            Serializer<T> serializer) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        Cursor cursor = null;
        try {
            cursor = getBdbDatabase().openCursor(null, null);
            List<T> result = get(cursor, key, lockMode, serializer);

            // If null, try again in different locking mode to
            // avoid null result due to gap between delete and new write
            if(result.size() == 0 && lockMode != LockMode.DEFAULT) {
                return get(cursor, key, LockMode.DEFAULT, serializer);
            } else {
                return result;
            }
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            if(logger.isTraceEnabled()) {
                logger.trace("Completed GET from key " + key + " (keyRef: "
                             + System.identityHashCode(key) + ") in "
                             + (System.nanoTime() - startTimeNs) + " ns at "
                             + System.currentTimeMillis());
            }

            attemptClose(cursor);
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
    private Database getBdbDatabase() {
        if(isTruncating.get()) {
            throw new VoldemortException("Bdb Store " + getName()
                                         + " is currently truncating cannot serve any request.");
        }

        return bdbDatabase;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        Cursor cursor = null;

        String keyStr = "";

        try {
            cursor = getBdbDatabase().openCursor(null, null);
            for(ByteArray key: keys) {

                if(logger.isTraceEnabled())
                    keyStr += key + " ";

                List<Versioned<byte[]>> values = get(cursor, key, readLockMode, versionedSerializer);
                if(!values.isEmpty())
                    result.put(key, values);
            }
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
        }

        if(logger.isTraceEnabled())
            logger.trace("Completed GETALL from keys " + keyStr + " in "
                         + (System.nanoTime() - startTimeNs) + " ns at "
                         + System.currentTimeMillis());

        return result;
    }

    private static <T> List<T> get(Cursor cursor,
                                   ByteArray key,
                                   LockMode lockMode,
                                   Serializer<T> serializer) throws DatabaseException {
        StoreUtils.assertValidKey(key);

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        DatabaseEntry valueEntry = new DatabaseEntry();
        List<T> results = Lists.newArrayList();

        for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, lockMode); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                        valueEntry,
                                                                                                                                                        lockMode)) {
            results.add(serializer.toObject(valueEntry.getData()));
        }

        if(logger.isTraceEnabled()) {
            logger.trace("Completed GET from key " + key + " in "
                         + (System.nanoTime() - startTimeNs) + " ns at "
                         + System.currentTimeMillis());
        }

        return results;
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        boolean succeeded = false;
        Transaction transaction = null;
        Cursor cursor = null;
        try {
            transaction = this.environment.beginTransaction(null, null);

            // Check existing values
            // if there is a version obsoleted by this value delete it
            // if there is a version later than this one, throw an exception
            DatabaseEntry valueEntry = new DatabaseEntry();
            cursor = getBdbDatabase().openCursor(transaction, null);
            for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, LockMode.RMW); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                                valueEntry,
                                                                                                                                                                LockMode.RMW)) {
                VectorClock clock = new VectorClock(valueEntry.getData());
                Occurred occurred = value.getVersion().compare(clock);
                if(occurred == Occurred.BEFORE)
                    throw new ObsoleteVersionException("Key "
                                                       + new String(hexCodec.encode(key.get()))
                                                       + " "
                                                       + value.getVersion().toString()
                                                       + " is obsolete, it is no greater than the current version of "
                                                       + clock + ".");
                else if(occurred == Occurred.AFTER)
                    // best effort delete of obsolete previous value!
                    cursor.delete();
            }

            // Okay so we cleaned up all the prior stuff, so now we are good to
            // insert the new thing
            valueEntry = new DatabaseEntry(versionedSerializer.toBytes(value));
            OperationStatus status = cursor.put(keyEntry, valueEntry);
            if(status != OperationStatus.SUCCESS)
                throw new PersistenceFailureException("Put operation failed with status: " + status);
            succeeded = true;

        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
            if(succeeded)
                attemptCommit(transaction);
            else
                attemptAbort(transaction);
        }

        if(logger.isTraceEnabled()) {
            logger.trace("Completed PUT to key " + key + " (keyRef: "
                         + System.identityHashCode(key) + " value " + value + " in "
                         + (System.nanoTime() - startTimeNs) + " ns at "
                         + System.currentTimeMillis());
        }
    }

    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        boolean deletedSomething = false;
        Cursor cursor = null;
        Transaction transaction = null;
        try {
            transaction = this.environment.beginTransaction(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(key.get());
            DatabaseEntry valueEntry = new DatabaseEntry();
            cursor = getBdbDatabase().openCursor(transaction, null);
            OperationStatus status = cursor.getSearchKey(keyEntry,
                                                         valueEntry,
                                                         LockMode.READ_UNCOMMITTED);
            while(status == OperationStatus.SUCCESS) {
                // if version is null no comparison is necessary
                if(new VectorClock(valueEntry.getData()).compare(version) == Occurred.BEFORE) {
                    cursor.delete();
                    deletedSomething = true;
                }
                status = cursor.getNextDup(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
            }
            return deletedSomething;
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {

            if(logger.isTraceEnabled()) {
                logger.trace("Completed DELETE of key " + key + " (keyRef: "
                             + System.identityHashCode(key) + ") in "
                             + (System.nanoTime() - startTimeNs) + " ns at "
                             + System.currentTimeMillis());
            }

            try {
                attemptClose(cursor);
            } finally {
                attemptCommit(transaction);
            }
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !Store.class.isAssignableFrom(o.getClass()))
            return false;
        Store<?, ?, ?> s = (Store<?, ?, ?>) o;
        return s.getName().equals(s.getName());
    }

    public void close() throws PersistenceFailureException {
        try {
            if(this.isOpen.compareAndSet(true, false))
                this.getBdbDatabase().close();
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    private void attemptAbort(Transaction transaction) {
        try {
            if(transaction != null)
                transaction.abort();
        } catch(Exception e) {
            logger.error("Abort failed!", e);
        }
    }

    private void attemptCommit(Transaction transaction) {
        try {
            transaction.commit();
        } catch(DatabaseException e) {
            logger.error("Transaction commit failed!", e);
            attemptAbort(transaction);
            throw new PersistenceFailureException(e);
        }
    }

    private static void attemptClose(Cursor cursor) {
        try {
            if(cursor != null)
                cursor.close();
        } catch(DatabaseException e) {
            logger.error("Error closing cursor.", e);
            throw new PersistenceFailureException(e.getMessage(), e);
        }
    }

    public DatabaseStats getStats(boolean setFast) {
        try {
            StatsConfig config = new StatsConfig();
            config.setFast(setFast);
            return this.getBdbDatabase().getStats(config);
        } catch(DatabaseException e) {
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

    private static abstract class BdbIterator<T> implements ClosableIterator<T> {

        private final boolean noValues;
        final Cursor cursor;

        private T current;
        private volatile boolean isOpen;

        public BdbIterator(Cursor cursor, boolean noValues) {
            this.cursor = cursor;
            isOpen = true;
            this.noValues = noValues;
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            if(noValues)
                valueEntry.setPartial(true);
            try {
                cursor.getFirst(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
            } catch(DatabaseException e) {
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
            if(keyEntry.getData() != null)
                current = get(keyEntry, valueEntry);
        }

        protected abstract T get(DatabaseEntry key, DatabaseEntry value);

        protected abstract void moveCursor(DatabaseEntry key, DatabaseEntry value)
                throws DatabaseException;

        public final boolean hasNext() {
            return current != null;
        }

        public final T next() {
            if(!isOpen)
                throw new PersistenceFailureException("Call to next() on a closed iterator.");

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            if(noValues)
                valueEntry.setPartial(true);
            try {
                moveCursor(keyEntry, valueEntry);
            } catch(DatabaseException e) {
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
            T previous = current;
            if(keyEntry.getData() == null)
                current = null;
            else
                current = get(keyEntry, valueEntry);

            return previous;
        }

        public final void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        public final void close() {
            try {
                cursor.close();
                isOpen = false;
            } catch(DatabaseException e) {
                logger.error(e);
            }
        }

        @Override
        protected final void finalize() {
            if(isOpen) {
                logger.error("Failure to close cursor, will be forcably closed.");
                close();
            }

        }
    }

    private static class BdbKeysIterator extends BdbIterator<ByteArray> {

        public BdbKeysIterator(Cursor cursor) {
            super(cursor, true);
        }

        @Override
        protected ByteArray get(DatabaseEntry key, DatabaseEntry value) {
            return new ByteArray(key.getData());
        }

        @Override
        protected void moveCursor(DatabaseEntry key, DatabaseEntry value) throws DatabaseException {
            cursor.getNextNoDup(key, value, LockMode.READ_UNCOMMITTED);
        }

    }

    private static class BdbEntriesIterator extends BdbIterator<Pair<ByteArray, Versioned<byte[]>>> {

        public BdbEntriesIterator(Cursor cursor) {
            super(cursor, false);
        }

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> get(DatabaseEntry key, DatabaseEntry value) {
            VectorClock clock = new VectorClock(value.getData());
            byte[] bytes = ByteUtils.copy(value.getData(),
                                          clock.sizeInBytes(),
                                          value.getData().length);
            return Pair.create(new ByteArray(key.getData()), new Versioned<byte[]>(bytes, clock));
        }

        @Override
        protected void moveCursor(DatabaseEntry key, DatabaseEntry value) throws DatabaseException {
            cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
        }
    }

    public boolean isPartitionAware() {
        return false;
    }

    public void nativeBackup(File toDir,
                             boolean verifyFiles,
                             boolean isIncremental,
                             AsyncOperationStatus status) {
        new BdbNativeBackup(environment, verifyFiles, isIncremental).performBackup(toDir, status);
    }
}
