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

import static voldemort.utils.Utils.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.VersionedSerializer;
import voldemort.store.Entry;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * A store that uses BDB for persistence
 * 
 * @author jay
 * 
 */
public class BdbStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);
    private static final Hex hexCodec = new Hex();

    private final String name;
    private final Database bdbDatabase;
    private final Environment environment;
    private final VersionedSerializer<byte[]> serializer;

    public BdbStorageEngine(String name, Environment environment, Database database) {
        assertNotNull("The store name cannot be null.", name);
        assertNotNull("The database cannot be null.", database);
        assertNotNull("The environment cannot be null.", environment);
        this.name = name;
        this.bdbDatabase = database;
        this.environment = environment;
        this.serializer = new VersionedSerializer<byte[]>(new IdentitySerializer());
    }

    public String getName() {
        return name;
    }

    public ClosableIterator<Entry<ByteArray, Versioned<byte[]>>> entries() {
        try {
            Cursor cursor = bdbDatabase.openCursor(null, null);
            return new BdbStoreIterator(cursor);
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        }
    }

    static byte[] makeKey(byte[] key, Version version) {
        VectorClock clock = (VectorClock) version;
        int clockSize = clock == null ? 0 : clock.sizeInBytes();
        byte[] keyBytes = new byte[2 + key.length + clockSize];
        ByteUtils.writeShort(keyBytes, (short) key.length, 0);
        System.arraycopy(key, 0, keyBytes, 2, key.length);
        if(clock != null)
            System.arraycopy(clock.toBytes(), 0, keyBytes, key.length + 2, clockSize);
        return keyBytes;
    }

    static byte[] getObjKey(byte[] bytes) {
        short size = ByteUtils.readShort(bytes, 0);
        byte[] dest = new byte[size];
        if(size > 0)
            System.arraycopy(bytes, 2, dest, 0, size);
        return dest;
    }

    static VectorClock getVersion(byte[] bytes) {
        short size = ByteUtils.readShort(bytes, 0);
        if(size >= bytes.length - 2)
            return null;
        else
            return new VectorClock(ByteUtils.copy(bytes, size + 2, bytes.length));
    }

    static boolean isPrefix(byte[] prefix, byte[] complete) {
        if(prefix.length > complete.length)
            return false;

        for(int i = 0; i < prefix.length; i++)
            if(complete[i] != prefix[i])
                return false;

        return true;
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

    public List<Versioned<byte[]>> get(ByteArray key) throws PersistenceFailureException {
        return get(key, LockMode.READ_UNCOMMITTED);
    }

    private List<Versioned<byte[]>> get(ByteArray key, LockMode lockMode)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        Cursor cursor = null;
        try {
            cursor = bdbDatabase.openCursor(null, null);
            return get(cursor, key, lockMode);
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        Cursor cursor = null;
        try {
            cursor = bdbDatabase.openCursor(null, null);
            for(ByteArray key: keys) {
                List<Versioned<byte[]>> values = get(cursor, key, LockMode.READ_UNCOMMITTED);
                if(!values.isEmpty())
                    result.put(key, values);
            }
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(cursor);
        }
        return result;
    }

    private List<Versioned<byte[]>> get(Cursor cursor, ByteArray key, LockMode lockMode)
            throws DatabaseException {
        StoreUtils.assertValidKey(key);

        DatabaseEntry keyEntry = new DatabaseEntry(key.get());
        DatabaseEntry valueEntry = new DatabaseEntry();
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();

        for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, lockMode); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                        valueEntry,
                                                                                                                                                        LockMode.RMW)) {
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
            cursor = bdbDatabase.openCursor(transaction, null);
            for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, LockMode.RMW); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                                valueEntry,
                                                                                                                                                                LockMode.RMW)) {
                VectorClock clock = new VectorClock(valueEntry.getData());
                Occured occured = value.getVersion().compare(clock);
                if(occured == Occured.BEFORE)
                    throw new ObsoleteVersionException("Key '"
                                                       + new String(hexCodec.encode(key.get()))
                                                       + "' " + value.getVersion().toString()
                                                       + " is obsolete," + " current version is "
                                                       + clock + ".");
                else if(occured == Occured.AFTER)
                    // best effort delete of obsolete previous value!
                    cursor.delete();
            }

            // Okay so we cleaned up all the prior stuff, so now we are good to
            // in
            valueEntry = new DatabaseEntry(serializer.toBytes(value));
            OperationStatus status = cursor.put(keyEntry, valueEntry);
            if(status != OperationStatus.SUCCESS)
                throw new PersistenceFailureException("Put operation failed with status: " + status);
            succeeded = true;

        } catch(DatabaseException e) {
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
            cursor = bdbDatabase.openCursor(transaction, null);
            for(OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, null); status == OperationStatus.SUCCESS; status = cursor.getNextDup(keyEntry,
                                                                                                                                                        valueEntry,
                                                                                                                                                        null)) {
                // if version is null no comparison is necessary
                if(new VectorClock(valueEntry.getData()).compare(version) == Occured.BEFORE) {
                    cursor.delete();
                    deletedSomething = true;
                }
            }
            return deletedSomething;
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        } finally {
            try {
                attemptClose(cursor);
            } finally {
                attemptCommit(transaction);
            }
        }
    }

    @Override
    public int hashCode() {
        return name.hashCode();
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
            this.bdbDatabase.close();
        } catch(DatabaseException e) {
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    private void attemptAbort(Transaction transaction) {
        try {
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

    private static class BdbStoreIterator implements
            ClosableIterator<Entry<ByteArray, Versioned<byte[]>>> {

        private volatile boolean isOpen;
        private final Cursor cursor;
        private Entry<ByteArray, Versioned<byte[]>> current;

        public BdbStoreIterator(Cursor cursor) {
            this.cursor = cursor;
            isOpen = true;
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            try {
                cursor.getFirst(keyEntry, valueEntry, null);
            } catch(DatabaseException e) {
                throw new PersistenceFailureException(e);
            }
            current = getEntry(keyEntry, valueEntry);
        }

        private Entry<ByteArray, Versioned<byte[]>> getEntry(DatabaseEntry key, DatabaseEntry value) {
            if(key == null || key.getData() == null) {
                return null;
            } else {
                VectorClock clock = new VectorClock(value.getData());
                byte[] bytes = ByteUtils.copy(value.getData(),
                                              clock.sizeInBytes(),
                                              value.getData().length);
                return new Entry<ByteArray, Versioned<byte[]>>(new ByteArray(key.getData()),
                                                               new Versioned<byte[]>(bytes, clock));
            }
        }

        public boolean hasNext() {
            return current != null;
        }

        public Entry<ByteArray, Versioned<byte[]>> next() {
            if(!isOpen)
                throw new PersistenceFailureException("Call to next() on a closed iterator.");

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            try {
                cursor.getNext(keyEntry, valueEntry, null);
            } catch(DatabaseException e) {
                throw new PersistenceFailureException(e);
            }
            Entry<ByteArray, Versioned<byte[]>> previous = current;
            if(keyEntry.getData() == null)
                current = null;
            else
                current = getEntry(keyEntry, valueEntry);

            return previous;
        }

        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        public void close() {
            try {
                cursor.close();
                isOpen = false;
            } catch(DatabaseException e) {
                throw new PersistenceFailureException(e);
            }
        }

        @Override
        protected void finalize() {
            if(isOpen) {
                logger.error("Failure to close cursor, will be forcably closed.");
                close();
            }

        }

    }
}
