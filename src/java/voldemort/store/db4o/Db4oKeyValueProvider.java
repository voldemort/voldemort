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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.ext.Db4oException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Db4oKeyValueProvider<Key, Value> {

    private ObjectContainer container;

    public static <Key, Value> Db4oKeyValueProvider<Key, Value> createDb4oKeyValueProvider(ObjectContainer container) {
        return new Db4oKeyValueProvider<Key, Value>(container);
    }

    Db4oKeyValueProvider(ObjectContainer container) {
        this.container = container;
    }

    private ObjectContainer getContainer() {
        if(container == null)
            throw new Db4oException("Db4oKeyValueProvider has no associated ObjectContainer");
        return container;
    }

    public Iterator<Key> keyIterator() {
        return getKeys().iterator();
    }

    /*
     * Returns a List with all Keys present in the database (no values)
     */
    public List<Key> getKeys() {
        List<Key> keys = Lists.newArrayList();
        ObjectSet<Db4oKeyValuePair<Key, Value>> result = getAll();
        while(result.hasNext()) {
            Db4oKeyValuePair<Key, Value> pair = result.next();
            keys.add(pair.getKey());
        }
        return keys;
    }

    /*
     * Returns a List with all Values present in the database (no keys)
     */
    public List<Value> getValues(Key key) {
        List<Value> values = Lists.newArrayList();
        ObjectSet<Db4oKeyValuePair<Key, Value>> result = get(key);
        while(result.hasNext()) {
            Db4oKeyValuePair<Key, Value> pair = result.next();
            values.add(pair.getValue());
        }
        return values;
    }

    /*
     * Returns an Iterator over Key/Value pairs
     */
    public Iterator<Db4oKeyValuePair<Key, Value>> pairIterator() {
        return getAll().iterator();
    }

    /*
     * Given a Key returns a Set with Key/Value pairs matching the provided Key
     */
    public ObjectSet<Db4oKeyValuePair<Key, Value>> get(Key key) {
        Db4oKeyValuePair<Key, Value> pair = new Db4oKeyValuePair<Key, Value>(key, null);
        return getContainer().queryByExample(pair);
        // Query query = getContainer().query();
        // query.constrain(Db4oStorageConfiguration.KEY_VALUE_PAIR_CLASS);
        // query.descend(Db4oStorageConfiguration.KEY_FIELD_NAME).constrain(key);
        // return query.execute();
    }

    /*
     * Deletes all Key/Value pairs matching the provided Key in the database
     */
    public int delete(Key key) {
        int deleteCount = 0;
        ObjectSet<Db4oKeyValuePair<Key, Value>> candidates = get(key);
        while(candidates.hasNext()) {
            delete(candidates.next());
            deleteCount++;
        }
        return deleteCount;
    }

    /*
     * Deletes a specific pair. The database might have more than one pair with
     * the same key so this differs from delete by key. Incurs in some overhead
     * because it performs a query to look for the pair.
     */
    public int delete(Key key, Value value) {
        int deleteCount = 0;
        Db4oKeyValuePair<Key, Value> pair = new Db4oKeyValuePair<Key, Value>(key, value);
        ObjectSet<Db4oKeyValuePair<Key, Value>> candidates = getContainer().queryByExample(pair);
        while(candidates.hasNext()) {
            delete(candidates.next());
            deleteCount++;
        }
        return deleteCount;
    }

    /*
     * Assumes pair was fetched from the database and still on db4o's reference
     * system. Results in low overhead because it does not perform a query
     */
    public void delete(Db4oKeyValuePair<Key, Value> pair) {
        getContainer().delete(pair);
    }

    /*
     * Stores a Key/Value pair in the database
     */
    public void put(Key key, Value value) {
        Db4oKeyValuePair<Key, Value> pair = new Db4oKeyValuePair<Key, Value>(key, value);
        put(pair);
    }

    /*
     * Stores a Key/Value pair in the database
     */
    public void put(Db4oKeyValuePair<Key, Value> pair) {
        getContainer().store(pair);
    }

    /*
     * Returns all Key/Value pairs in the database
     */
    public ObjectSet<Db4oKeyValuePair<Key, Value>> getAll() {
        return getContainer().queryByExample(new Db4oKeyValuePair<Key, Value>(null, null));
    }

    /*
     * Returns the number of Key/Value pairs in the database
     */
    public int size() {
        return getAll().size();
    }

    public Map<Key, List<Value>> getAll(Iterable<Key> keys) {
        Map<Key, List<Value>> result = newEmptyHashMap(keys);
        for(Key key: keys) {
            List<Value> values = getValues(key);
            if(!values.isEmpty())
                result.put(key, values);
        }
        return result;
    }

    private <K, V> HashMap<K, V> newEmptyHashMap(Iterable<?> iterable) {
        if(iterable instanceof Collection<?>)
            return Maps.newHashMapWithExpectedSize(((Collection<?>) iterable).size());
        return Maps.newHashMap();
    }

    public int truncate() {
        int deleteCount = 0;
        ObjectSet<Db4oKeyValuePair<Key, Value>> candidates = getAll();
        while(candidates.hasNext()) {
            delete(candidates.next());
            deleteCount++;
        }
        return deleteCount;
    }

    public void commit() {
        getContainer().commit();
    }

    public void rollback() {
        getContainer().rollback();
    }

    public void close() {
        getContainer().close();
    }

    public boolean isClosed() {
        return getContainer().ext().isClosed();
    }

}
