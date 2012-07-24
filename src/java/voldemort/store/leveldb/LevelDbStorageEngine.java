/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.leveldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.StripedLock;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A LevelDB-based storage engine.
 * 
 * Values are packed together in a format given by LevelDbBinaryFormat.java.
 * 
 * A striped lock is used for the read, modify, update loop in put/delete
 */
public class LevelDbStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private final String name;
    private final File dataDir;
    private volatile DB db;
    private final StripedLock locks;
    private final Options options;

    public LevelDbStorageEngine(String name, DB db, Options options, File dataDir, int numLocks) {
        if(numLocks <= 0)
            throw new IllegalArgumentException("The number of locks must be positive.");
        this.name = Utils.notNull(name);
        this.db = Utils.notNull(db);
        this.dataDir = dataDir;
        this.locks = new StripedLock(numLocks);
        this.options = options;
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        byte[] found = db.get(key.get());
        if(found == null)
            return Collections.emptyList();
        else
            return LevelDbBinaryFormat.fromByteArray(found);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, null);
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Lock lock = this.locks.lockFor(key.get());
        try {
            lock.lock();
            byte[] bytes = db.get(key.get());

            // deserialize existing values
            List<Versioned<byte[]>> vals = null;
            if(bytes == null)
                vals = new ArrayList<Versioned<byte[]>>(3);
            else
                vals = LevelDbBinaryFormat.fromByteArray(bytes);

            // purge obsolete values, if any
            if(bytes != null) {
                Iterator<Versioned<byte[]>> iter = vals.iterator();
                while(iter.hasNext()) {
                    Versioned<byte[]> curr = iter.next();
                    Occurred occurred = value.getVersion().compare(curr.getVersion());
                    if(occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                           + "': " + value.getVersion());
                    else if(occurred == Occurred.AFTER)
                        iter.remove();
                }
            }

            // finally add the value, and write it back
            vals.add(value);
            db.put(key.get(), LevelDbBinaryFormat.toByteArray(vals));
        } finally {
            lock.unlock();
        }
    }

    public boolean delete(ByteArray key, Version maxVersion) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Lock lock = this.locks.lockFor(key.get());
        try {
            lock.lock();
            byte[] bytes = db.get(key.get());

            // maybe there is nothing to delete?
            if(bytes == null)
                return false;

            // if they don't care about what version, just blow everything away
            if(maxVersion == null) {
                db.delete(key.get());
                return true;
            }

            // do a full delete
            boolean deleted = false;
            List<Versioned<byte[]>> vals = LevelDbBinaryFormat.fromByteArray(bytes);
            Iterator<Versioned<byte[]>> iter = vals.iterator();
            while(iter.hasNext()) {
                Versioned<byte[]> curr = iter.next();
                Version currentVersion = curr.getVersion();
                if(currentVersion.compare(maxVersion) == Occurred.BEFORE) {
                    iter.remove();
                    deleted = true;
                }
            }
            if(deleted)
                db.put(key.get(), LevelDbBinaryFormat.toByteArray(vals));

            return deleted;
        } finally {
            lock.unlock();
        }
    }

    public String getName() {
        return name;
    }

    public void close() throws VoldemortException {
        try {
            this.db.close();
        } catch(Exception e) {
            throw new VoldemortException("Error closing store.", e);
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new LevelDbIterator();
    }

    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    public void truncate() {
        // this is unsafe--callers may get errors while the truncate is
        // happening
        try {
            this.db.close();
            factory.destroy(dataDir, new Options());
            this.db = factory.open(dataDir, options);
        } catch(IOException e) {
            throw new VoldemortException("Error while truncating store.", e);
        }
    }

    public boolean isPartitionAware() {
        return false;
    }

    private class LevelDbIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private final DBIterator iter;
        private final List<Pair<ByteArray, Versioned<byte[]>>> cache;
        private Pair<ByteArray, Versioned<byte[]>> current;

        public LevelDbIterator() {
            this.cache = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
            this.iter = db.iterator();
            iter.seekToFirst();
        }

        public void close() {
            iter.close();
        }

        public boolean hasNext() {
            // we have a next element if there is at least one cached
            // element or we can make more
            return cache.size() > 0 || makeMore();
        }

        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(cache.size() == 0) {
                boolean madeSomeMore = makeMore();
                if(!madeSomeMore)
                    throw new NoSuchElementException("Iterated to end.");
            }
            // must now have at least one thing in the cache
            current = cache.remove(cache.size() - 1);
            return current;
        }

        public void remove() {
            if(current == null)
                throw new IllegalStateException("No current element to remove");
            delete(current.getFirst(), null);
        }

        protected boolean makeMore() {
            try {
                Map.Entry<byte[], byte[]> entry = iter.next();
                ByteArray key = new ByteArray(entry.getKey());
                for(Versioned<byte[]> val: LevelDbBinaryFormat.fromByteArray(entry.getValue()))
                    this.cache.add(Pair.create(key, val));
                return true;
            } catch(NoSuchElementException e) {
                return false;
            }
        }

    }

}
