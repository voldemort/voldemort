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

package voldemort.store.memory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * 
 */
public class InMemoryStorageEngine<K, V, T> extends AbstractStorageEngine<K, V, T> {

    private final ConcurrentMap<K, List<Versioned<V>>> map;

    public InMemoryStorageEngine(String name) {
        super(name);
        this.map = new ConcurrentHashMap<K, List<Versioned<V>>>();
    }

    public InMemoryStorageEngine(String name, ConcurrentMap<K, List<Versioned<V>>> map) {
        super(name);
        this.map = Utils.notNull(map);
    }

    public void deleteAll() {
        this.map.clear();
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

    @Override
    public boolean delete(K key, Version version) {
        StoreUtils.assertValidKey(key);

        if(version == null)
            return map.remove(key) != null;

        List<Versioned<V>> values = map.get(key);
        if(values == null) {
            return false;
        }
        synchronized(values) {
            boolean deletedSomething = false;
            Iterator<Versioned<V>> iterator = values.iterator();
            while(iterator.hasNext()) {
                Versioned<V> item = iterator.next();
                if(item.getVersion().compare(version) == Occurred.BEFORE) {
                    iterator.remove();
                    deletedSomething = true;
                }
            }
            if(values.size() == 0) {
                // If this remove fails, then another delete operation got
                // there before this one
                if(!map.remove(key, values))
                    return false;
            }

            return deletedSomething;
        }
    }

    @Override
    public List<Version> getVersions(K key) {
        return StoreUtils.getVersions(get(key, null));
    }

    @Override
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> results = map.get(key);
        if(results == null) {
            return new ArrayList<Versioned<V>>(0);
        }
        synchronized(results) {
            return new ArrayList<Versioned<V>>(results);
        }
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, transforms);
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        Version version = value.getVersion();
        boolean success = false;
        while(!success) {
            List<Versioned<V>> items = map.get(key);
            // If we have no value, optimistically try to add one
            if(items == null) {
                items = new ArrayList<Versioned<V>>();
                items.add(new Versioned<V>(value.getValue(), version));
                success = map.putIfAbsent(key, items) == null;
            } else {
                synchronized(items) {
                    // if this check fails, items has been removed from the map
                    // by delete, so we try again.
                    if(map.get(key) != items)
                        continue;

                    // Check for existing versions - remember which items to
                    // remove in case of success
                    List<Versioned<V>> itemsToRemove = new ArrayList<Versioned<V>>(items.size());
                    for(Versioned<V> versioned: items) {
                        Occurred occurred = value.getVersion().compare(versioned.getVersion());
                        if(occurred == Occurred.BEFORE) {
                            throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                               + "': " + value.getVersion());
                        } else if(occurred == Occurred.AFTER) {
                            itemsToRemove.add(versioned);
                        }
                    }
                    items.removeAll(itemsToRemove);
                    items.add(value);
                }
                success = true;
            }
        }
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new InMemoryIterator<K, V>(map);
    }

    @Override
    public ClosableIterator<K> keys() {
        // TODO Implement more efficient version.
        return StoreUtils.keys(entries());
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<K> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public void truncate() {
        map.clear();
    }

    @Override
    public String toString() {
        return toString(15);
    }

    public String toString(int size) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int count = 0;
        for(Entry<K, List<Versioned<V>>> entry: map.entrySet()) {
            if(count > size) {
                builder.append("...");
                break;
            }
            builder.append(entry.getKey());
            builder.append(':');
            builder.append(entry.getValue());
            builder.append(',');
        }
        builder.append('}');
        return builder.toString();
    }

    @NotThreadsafe
    private static class InMemoryIterator<K, V> implements ClosableIterator<Pair<K, Versioned<V>>> {

        private final Iterator<Entry<K, List<Versioned<V>>>> iterator;
        private K currentKey;
        private Iterator<Versioned<V>> currentValues;

        public InMemoryIterator(ConcurrentMap<K, List<Versioned<V>>> map) {
            this.iterator = map.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return hasNextInCurrentValues() || iterator.hasNext();
        }

        private boolean hasNextInCurrentValues() {
            return currentValues != null && currentValues.hasNext();
        }

        private Pair<K, Versioned<V>> nextInCurrentValues() {
            Versioned<V> item = currentValues.next();
            return Pair.create(currentKey, item);
        }

        @Override
        public Pair<K, Versioned<V>> next() {
            if(hasNextInCurrentValues()) {
                return nextInCurrentValues();
            } else {
                // keep trying to get a next, until we find one (they could get
                // removed)
                while(true) {
                    Entry<K, List<Versioned<V>>> entry = iterator.next();

                    List<Versioned<V>> list = entry.getValue();
                    synchronized(list) {
                        // okay we may have gotten an empty list, if so try
                        // again
                        if(list.size() == 0)
                            continue;

                        // grab a snapshot of the list while we have exclusive
                        // access
                        currentValues = new ArrayList<Versioned<V>>(list).iterator();
                    }
                    currentKey = entry.getKey();
                    return nextInCurrentValues();
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        @Override
        public void close() {
            // nothing to do here
        }

    }
}
