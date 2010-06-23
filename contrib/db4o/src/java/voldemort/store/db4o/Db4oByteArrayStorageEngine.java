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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.db4o.Db4oEmbedded;
import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.ext.Db4oException;
import com.google.common.collect.Lists;

/**
 * A store that uses db4o for persistence
 * 
 * 
 */
public class Db4oByteArrayStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(Db4oByteArrayStorageEngine.class);

    private final String path;
    private final AtomicBoolean isOpen;
    private final AtomicBoolean isTruncating = new AtomicBoolean(false);

    private EmbeddedConfiguration databaseConfig;
    private ObjectContainer objectContainer;
    private Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> keyValueProvider;

    public Db4oByteArrayStorageEngine(String path, EmbeddedConfiguration databaseConfig) {
        this.path = Utils.notNull(path);
        this.databaseConfig = Utils.notNull(databaseConfig);
        this.isOpen = new AtomicBoolean(true);
        getKeyValueProvider();
    }

    private Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> getKeyValueProvider() {
        if(keyValueProvider == null || keyValueProvider.isClosed())
            keyValueProvider = new Db4oKeyValueProvider<ByteArray, Versioned<byte[]>>(openDb4oDatabase());
        return keyValueProvider;
    }

    public String getName() {
        return path;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        try {
            return new Db4oEntriesIterator<ByteArray, Versioned<byte[]>>(getKeyValueProvider());
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    public ClosableIterator<ByteArray> keys() {
        try {
            return new Db4oKeysIterator<ByteArray, Versioned<byte[]>>(getKeyValueProvider());
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    public void truncate() {
        if(isTruncating.compareAndSet(false, true)) {
            boolean succeeded = false;
            try {
                getKeyValueProvider().truncate();
                getKeyValueProvider().commit();
                succeeded = true;
            } catch(Db4oException e) {
                logger.error(e);
                throw new VoldemortException("Failed to truncate db4o store " + getName(), e);

            } finally {
                commitOrAbort(succeeded, getKeyValueProvider());
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

    private void commitOrAbort(boolean succeeded,
                               Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> provider) {
        try {
            if(succeeded) {
                attemptCommit(provider);
            } else {
                attemptAbort(provider);
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
            getKeyValueProvider();
            return true;
        } catch(Db4oException e) {
            throw new StorageInitializationException("Failed to reinitialize Db4oByteArrayStorageEngine for store:"
                                                             + getName() + " after truncation.",
                                                     e);
        }
    }

    public List<Version> getVersions(ByteArray key) {
        List<Versioned<byte[]>> versioned = get(key);
        List<Version> versions = Lists.newArrayList();
        for(Versioned<byte[]> v: versioned) {
            versions.add(v.getVersion());
        }
        return versions;
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws PersistenceFailureException {
        return getKeyValueProvider().getValues(key);
    }

    /**
     * truncate() operation mandates that all opened Databases be closed before
     * attempting truncation.
     * <p>
     * This method throws an exception while truncation is happening to any
     * request attempted in parallel with store truncation.
     * 
     * @return
     */
    private ObjectContainer openDb4oDatabase() {
        if(isTruncating.get()) {
            throw new VoldemortException("Db4o Store " + getName()
                                         + " is currently truncating cannot serve any request.");
        }
        // try {
        objectContainer = Db4oEmbedded.openFile(databaseConfig, path);
        return objectContainer;
        // } catch(DatabaseFileLockedException dfle) {
        // return objectContainer;
        // }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        try {
            for(ByteArray key: keys) {
                List<Versioned<byte[]>> values = getKeyValueProvider().getValues(key);
                if(!values.isEmpty())
                    result.put(key, values);
            }
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(getKeyValueProvider());
        }
        return result;
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> keyValueProvider = getKeyValueProvider();
        boolean succeeded = false;
        try {
            ObjectSet<Db4oKeyValuePair<ByteArray, Versioned<byte[]>>> candidates = keyValueProvider.get(key);
            for(Db4oKeyValuePair<ByteArray, Versioned<byte[]>> pair: candidates) {
                Occured occured = value.getVersion().compare(pair.getValue().getVersion());
                if(occured == Occured.BEFORE)
                    throw new ObsoleteVersionException("Key "
                                                       + key.toString()
                                                       + " "
                                                       + value.getVersion().toString()
                                                       + " is obsolete, it is no greater than the current version of "
                                                       + pair.getValue().getVersion() + ".");
                else if(occured == Occured.AFTER)
                    // best effort delete of obsolete previous value!
                    keyValueProvider.delete(pair);
            }
            // Okay so we cleaned up all the prior stuff, so we can now insert
            try {
                keyValueProvider.set(key, value);
            } catch(Db4oException de) {
                throw new PersistenceFailureException("Put operation failed with status: "
                                                      + de.getMessage());
            }
            succeeded = true;
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(keyValueProvider);
            if(succeeded)
                attemptCommit(keyValueProvider);
            else
                attemptAbort(keyValueProvider);
        }
    }

    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        boolean deletedSomething = false;
        Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> keyValueProvider = getKeyValueProvider();
        try {
            ObjectSet<Db4oKeyValuePair<ByteArray, Versioned<byte[]>>> candidates = keyValueProvider.get(key);
            for(Db4oKeyValuePair<ByteArray, Versioned<byte[]>> pair: candidates) {
                // if version is null no comparison is necessary
                if(pair.getValue().getVersion().compare(version) == Occured.BEFORE) {
                    keyValueProvider.delete(pair);
                    deletedSomething = true;
                }
            }
            return deletedSomething;
        } catch(Db4oException de) {
            logger.error(de);
            throw new PersistenceFailureException(de);
        } finally {
            try {
                attemptClose(keyValueProvider);
            } finally {
                attemptCommit(keyValueProvider);
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
                getKeyValueProvider().close();
        } catch(Db4oException e) {
            logger.error(e);
            throw new PersistenceFailureException("Shutdown failed.", e);
        }
    }

    private void attemptAbort(Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> provider) {
        try {
            if(provider != null)
                provider.rollback(); // abort transaction
        } catch(Db4oException e) {
            logger.error("Abort failed!", e);
        }
    }

    private void attemptCommit(Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> provider) {
        try {
            provider.commit();
        } catch(Db4oException e) {
            logger.error("Transaction commit failed!", e);
            attemptAbort(provider);
            throw new PersistenceFailureException(e);
        }
    }

    private static void attemptClose(Db4oKeyValueProvider<ByteArray, Versioned<byte[]>> provider) {
        try {
            if(provider != null)
                provider.close();
        } catch(Db4oException e) {
            logger.error("Error closing cursor.", e);
            throw new PersistenceFailureException(e.getMessage(), e);
        }
    }

    @JmxOperation(description = "A variety of stats about the db4o for this store.")
    public String getDb4oStats() {
        throw new VoldemortException("Db4o stats not implemented yet");
    }

}
