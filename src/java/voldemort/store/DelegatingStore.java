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

package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Store template that delegates all operations to an inner store.
 * 
 * Convenient for decorating a store and overriding only certain methods to add
 * behavior.
 * 
 * @author jay
 * 
 */
public class DelegatingStore<K, V> implements Store<K, V> {

    private final Store<K, V> innerStore;

    public DelegatingStore(Store<K, V> innerStore) {
        this.innerStore = Utils.notNull(innerStore);
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

    public boolean delete(K key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.delete(key, version);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return innerStore.getAll(keys);
    }

    public List<Versioned<V>> get(K key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.get(key);
    }

    public String getName() {
        return innerStore.getName();
    }

    public void put(K key, Versioned<V> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        innerStore.put(key, value);
    }

    public Store<K, V> getInnerStore() {
        return innerStore;
    }

    public Object getCapability(StoreCapabilityType capability) {
        return innerStore.getCapability(capability);
    }

    @Override
    public String toString() {
        return innerStore.toString();
    }

    public List<Version> getVersions(K key) {
        return innerStore.getVersions(key);
    }
}
