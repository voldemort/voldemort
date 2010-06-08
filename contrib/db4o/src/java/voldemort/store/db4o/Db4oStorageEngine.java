/*
 * Copyright 2010 Versant Corporation
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

package voldemort.store.db4o;

import java.util.ArrayList;
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
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.db4o.Db4oEmbedded;
import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.config.QueryEvaluationMode;
import com.db4o.ext.Db4oException;
import com.google.common.collect.Lists;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;

/**
 * A store that uses db4o for persistence
 * 
 * 
 */
public class Db4oStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(Db4oStorageEngine.class);
    private static final Hex hexCodec = new Hex();

    private final String path;
    private final EmbeddedConfiguration databaseConfig;
    private final VersionedSerializer<byte[]> versionedSerializer;
    private final AtomicBoolean isOpen;
    private final boolean preload;
    private final Serializer<Version> versionSerializer;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    private ObjectContainer objectContainer;

    public Db4oStorageEngine(String path, EmbeddedConfiguration databaseConfig) {
        this(path, databaseConfig, false);
    }

    public Db4oStorageEngine(String path, EmbeddedConfiguration databaseConfig, boolean preload) {
        this.path = Utils.notNull(path);
        this.databaseConfig = Utils.notNull(databaseConfig);
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
        this.preload = preload;
        // use read uncommitted isolation
        databaseConfig.common().queries().evaluationMode(QueryEvaluationMode.LAZY);
        openDb4oDatabase();
    }

    public String getName() {
        return path;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            /*
             * if(cursorPreload) { PreloadConfig preloadConfig = new
             * PreloadConfig(); preloadConfig.setLoadLNs(true); // preload leaf
             * nodes in the // cache getBdbDatabase().preload(preloadConfig); }
             */
            // Cursor cursor = getBdbDatabase().openCursor(null, null);
            return new Db4oEntriesIterator(objectContainer);
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    public ClosableIterator<ByteArray> keys() {
        try {
            // Cursor cursor = getBdbDatabase().openCursor(null, null);
            return new Db4oKeysIterator(objectContainer);
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    public void truncate() {

        if(isTruncating.compareAndSet(false, true)) {
            boolean succeeded = false;

            try {
                ObjectSet result = objectContainer.queryByExample(null);
                ArrayList<Long> ids = new ArrayList<Long>();
                for(Object obj: result) {
                    ids.add(objectContainer.ext().getID(obj));
                }
                for(long id: ids) {
                    objectContainer.delete(objectContainer.ext().getByID(id));
                }
                objectContainer.commit();
                objectContainer.ext().purge();

                succeeded = true;
            } catch(DatabaseException e) {
                logger.error(e);
                throw new VoldemortException("Failed to truncate Bdb store " + getName(), e);

            } finally {

                commitOrAbort(succeeded, objectContainer);

                // reopen the db4o database for future queries.
                if(reopenDb4oDatabase()) {
                    isTruncating.compareAndSet(true, false);
                } else {
                    throw new VoldemortException("Failed to reopen db4o Database after truncation, All request will fail on store "
                                                 + getName());
                }
            }
        } else {
            throw new VoldemortException("Store " + getName()
                                         + " is already truncating, cannot start another one.");
        }
    }

    private void commitOrAbort(boolean succeeded, ObjectContainer container) {
        try {
            if(succeeded) {
                attemptCommit(container);
            } else {
                attemptAbort(container);
            }
        } catch(Exception e) {
            logger.error(e);
        }
    }

    /**
     * Reopens the db4o Database after a successful truncate operation.
     */
    private boolean reopenDb4oDatabase() {
        try {
            if(objectContainer.ext().isClosed())
                this.openDb4oDatabase();
            return true;
        } catch(Db4oException e) {
            throw new StorageInitializationException("Failed to reinitialize Db4oStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    public List<Version> getVersions(ByteArray key) {
        return get(key, LockMode.READ_UNCOMMITTED, versionSerializer);
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws PersistenceFailureException {
        return get(key, LockMode.READ_UNCOMMITTED, versionedSerializer);
    }

    private <T> List<T> get(ByteArray key, LockMode lockMode, Serializer<T> serializer)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        Cursor cursor = null;
        try {
            cursor = getBdbDatabase().openCursor(null, null);
            return get(cursor, key, lockMode, serializer);
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
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
    private ObjectContainer openDb4oDatabase() {
        if(isTruncating.get()) {
            throw new VoldemortException("Db4o Store " + getName()
                                         + " is currently truncating cannot serve any request.");
        }
        if(objectContainer == null) {
            objectContainer = Db4oEmbedded.openFile(databaseConfig, path);
        }
        return objectContainer;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        Cursor cursor = null;
        try {
            cursor = getBdbDatabase().openCursor(null, null);
            for(ByteArray key: keys) {
                List<Versioned<byte[]>> values = get(cursor,
                                                     key,
                                                     LockMode.READ_UNCOMMITTED,
                                                     versionedSerializer);
                if(!values.isEmpty())
                    result.put(key, values);
            }
        } catch(DatabaseException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
        }
        return result;
    }

    private static <T> List<T> get(ObjectContainer container,
                                   ByteArray key,
                                   LockMode lockMode,
                                   Serializer<T> serializer) throws DatabaseException {
        StoreUtils.assertValidKey(key);
        // TODO Use lock mode somehow?
        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        DatabaseEntry valueEntry = new DatabaseEntry();
        List<T> results = Lists.newArrayList();

        for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, lockMode); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                        valueEntry,
                                                                                                                                                        lockMode)) {
            results.add(serializer.toObject(valueEntry.getData()));
        }
        return results;
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

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
                Occured occured = value.getVersion().compare(clock);
                if(occured == Occured.BEFORE)
                    throw new ObsoleteVersionException("Key "
                                                       + new String(hexCodec.encode(key.get()))
                                                       + " "
                                                       + value.getVersion().toString()
                                                       + " is obsolete, it is no greater than the current version of "
                                                       + clock + ".");
                else if(occured == Occured.AFTER)
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
    }

    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
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
                if(new VectorClock(valueEntry.getData()).compare(version) == Occured.BEFORE) {
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
        return path.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !Store.class.isAssignableFrom(o.getClass()))
            return false;
        Store<?, ?> s = (Store<?, ?>) o;
        return s.getName().equals(s.getName());
    }

    public void close() throws PersistenceFailureException {
        try {
            if(this.isOpen.compareAndSet(true, false))
                objectContainer.close();
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    private void attemptAbort(ObjectContainer container) {
        try {
            if(container != null)
                container.rollback(); // abort transaction
        } catch(Db4oException e) {
            logger.error("Abort failed!", e);
        }
    }

    private void attemptCommit(ObjectContainer container) {
        try {
            container.commit();
        } catch(Db4oException e) {
            logger.error("Transaction commit failed!", e);
            attemptAbort(container);
            throw new PersistenceFailureException(e);
        }
    }

    private static void attemptClose(ObjectContainer container) {
        try {
            if(container != null)
                container.close();
        } catch(Db4oException e) {
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

    @JmxOperation(description = "A variety of stats about the db4o for this store.")
    public String getBdbStats() {
        String stats = getStats(false).toString();
        return stats;
    }

    private static abstract class Db4oIterator<T> implements ClosableIterator<T> {

        private final boolean noValues;
        final Cursor cursor;

        private T current;
        private volatile boolean isOpen;

        public Db4oIterator(Cursor cursor, boolean noValues) {
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

    private static class Db4oKeysIterator extends BdbIterator<ByteArray> {

        public Db4oKeysIterator(Cursor cursor) {
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

    private static class Db4oEntriesIterator extends
            BdbIterator<Pair<ByteArray, Versioned<byte[]>>> {

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
}
