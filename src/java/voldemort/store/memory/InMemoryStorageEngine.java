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
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * @author jay
 * @author dain
 * 
 */
public class InMemoryStorageEngine<K, V> implements StorageEngine<K, V> {

    private final ConcurrentMap<K, List<Versioned<V>>> map;
    private final String name;

    public InMemoryStorageEngine(String name) {
        this.name = Utils.notNull(name);
        this.map = new ConcurrentHashMap<K, List<Versioned<V>>>();
    }

    public InMemoryStorageEngine(String name, ConcurrentMap<K, List<Versioned<V>>> map) {
        this.name = Utils.notNull(name);
        this.map = Utils.notNull(map);
    }

    public void close() {}

    public void deleteAll() {
        this.map.clear();
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

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
                if(item.getVersion().compare(version) == Occured.BEFORE) {
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

    public List<Version> getVersions(K key) {
        return StoreUtils.getVersions(get(key));
    }

    public List<Versioned<V>> get(K key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> results = map.get(key);
        if(results == null) {
            return new ArrayList<Versioned<V>>(0);
        }
        synchronized(results) {
            return new ArrayList<Versioned<V>>(results);
        }
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public void put(K key, Versioned<V> value) throws VoldemortException {
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
                        Occured occured = value.getVersion().compare(versioned.getVersion());
                        if(occured == Occured.BEFORE) {
                            throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                               + "': " + value.getVersion());
                        } else if(occured == Occured.AFTER) {
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

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new InMemoryIterator<K, V>(map);
    }

    public String getName() {
        return name;
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

        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        public void close() {
        // nothing to do here
        }

    }
}
