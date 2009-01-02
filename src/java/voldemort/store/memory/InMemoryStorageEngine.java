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
import voldemort.store.KeyWrapper;
import voldemort.store.ObsoleteVersionException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * @author jay
 * 
 */
public class InMemoryStorageEngine<K, V> implements StorageEngine<K, V> {

    private final ConcurrentMap<KeyWrapper, List<Versioned<V>>> map;
    private final String name;

    public InMemoryStorageEngine(String name) {
        this.name = Objects.nonNull(name);
        this.map = new ConcurrentHashMap<KeyWrapper, List<Versioned<V>>>();
    }

    public InMemoryStorageEngine(String name, ConcurrentMap<KeyWrapper, List<Versioned<V>>> map) {
        this.name = Objects.nonNull(name);
        this.map = Objects.nonNull(map);
    }

    public void close() {}

    public boolean delete(K key) {
        return delete(new KeyWrapper(key));
    }

    private boolean delete(KeyWrapper key) {
        return map.remove(key) != null;
    }

    public boolean delete(K k, Version version) {
        StoreUtils.assertValidKey(k);

        KeyWrapper key = new KeyWrapper(k);
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

    public List<Versioned<V>> get(K k) throws VoldemortException {
        StoreUtils.assertValidKey(k);
        KeyWrapper key = new KeyWrapper(k);
        List<Versioned<V>> results = map.get(key);
        if(results == null) {
            return new ArrayList<Versioned<V>>(0);
        } else {
            synchronized(results) {
                return new ArrayList<Versioned<V>>(results);
            }
        }
    }

    public void put(K k, Versioned<V> value) throws VoldemortException {
        StoreUtils.assertValidKey(k);

        KeyWrapper key = new KeyWrapper(k);
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
        for(Map.Entry<KeyWrapper, List<Versioned<V>>> entry: map.entrySet()) {
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

        private Iterator<KeyWrapper> iterator;
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

        @SuppressWarnings("unchecked")
        public Entry<K, Versioned<V>> next() {
            if(hasNextInCurrentList()) {
                return getFromCurrentList();
            } else {
                // keep trying to get a next, until we find one (they could get
                // removed)
                while(true) {
                    KeyWrapper key = null;
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
                        currentKey = (K) key.get();
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
