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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.store.Entry;
import voldemort.store.StorageEngine;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * @author jay
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

    public boolean delete(K key) {
        return map.remove(key) != null;
    }

    public boolean delete(K key, Version version) {
        StoreUtils.assertValidKey(key);

        if(version == null)
            return delete(key);

        List<Versioned<V>> values = map.get(key);
        if(values == null) {
            return false;
        } else {
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
                if(values.size() == 0)
                    map.remove(key);

                return deletedSomething;
            }
        }
    }

    public List<Versioned<V>> get(K key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> results = map.get(key);
        if(results == null) {
            return new ArrayList<Versioned<V>>(0);
        } else {
            synchronized(results) {
                return new ArrayList<Versioned<V>>(results);
            }
        }
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public void put(K key, Versioned<V> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        VectorClock clock = (VectorClock) value.getVersion();
        boolean success = false;
        while(!success) {
            List<Versioned<V>> items = map.get(key);
            // If we have no value, optimistically try to add one
            if(items == null) {
                items = new ArrayList<Versioned<V>>();
                items.add(new Versioned<V>(value.getValue(), clock));
                success = map.putIfAbsent(key, items) == null;
            } else {
                synchronized(items) {
                    // Check for existing versions
                    int index = 0;
                    while(index < items.size()) {
                        Versioned<V> versioned = items.get(index);
                        Occured occured = value.getVersion().compare(versioned.getVersion());
                        if(occured == Occured.BEFORE)
                            throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                               + "': " + value.getVersion());
                        else if(occured == Occured.AFTER)
                            items.remove(index);
                        else
                            index++;
                    }
                    items.add(value);
                }
                success = true;
            }
        }
    }

    public ClosableIterator<Entry<K, Versioned<V>>> entries() {
        return new InMemoryIterator();
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
        for(Map.Entry<K, List<Versioned<V>>> entry: map.entrySet()) {
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
    private class InMemoryIterator implements ClosableIterator<Entry<K, Versioned<V>>> {

        private Iterator<K> iterator;
        private K currentKey;
        private List<Versioned<V>> currentList;
        private int listIndex;

        public InMemoryIterator() {
            this.iterator = map.keySet().iterator();
        }

        public boolean hasNext() {
            return hasNextInCurrentList() || iterator.hasNext();
        }

        private boolean hasNextInCurrentList() {
            return currentList != null && listIndex < currentList.size();
        }

        private Entry<K, Versioned<V>> getFromCurrentList() {
            Versioned<V> item = currentList.get(listIndex);
            listIndex++;
            return new Entry<K, Versioned<V>>(currentKey, item);
        }

        public Entry<K, Versioned<V>> next() {
            if(hasNextInCurrentList()) {
                return getFromCurrentList();
            } else {
                // keep trying to get a next, until we find one (they could get
                // removed)
                while(true) {
                    K key = null;
                    List<Versioned<V>> list = null;
                    do {
                        key = iterator.next();
                        list = map.get(key);
                    } while(list == null);
                    synchronized(list) {
                        // okay we may have gotten an empty list, if so try
                        // again
                        if(list.size() == 0)
                            continue;
                        currentKey = key;
                        currentList = new ArrayList<Versioned<V>>(list);
                        listIndex = 0;
                        return getFromCurrentList();
                    }
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
