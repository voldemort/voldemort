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
 * 
 */
public class DelegatingStore<K, V, T> extends AbstractStore<K, V, T> {

    private final Store<K, V, T> innerStore;

    public DelegatingStore(Store<K, V, T> innerStore) {
        super(innerStore.getName());
        this.innerStore = Utils.notNull(innerStore);
    }

    @Override
    public void close() throws VoldemortException {
        innerStore.close();
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.delete(key, version);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return innerStore.getAll(keys, transforms);
    }

    @Override
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.get(key, transform);
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        innerStore.put(key, value, transform);
    }

    public Store<K, V, T> getInnerStore() {
        return innerStore;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        return innerStore.getCapability(capability);
    }

    @Override
    public String toString() {
        return innerStore.toString();
    }

    @Override
    public List<Version> getVersions(K key) {
        return innerStore.getVersions(key);
    }

    @Override
    public List<Versioned<V>> get(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        StoreUtils.assertValidKey(request.getKey());
        return innerStore.get(request);
    }

    // TODO: Validate all the keys in the request object
    @Override
    public Map<K, List<Versioned<V>>> getAll(CompositeVoldemortRequest<K, V> request)
            throws VoldemortException {
        return innerStore.getAll(request);
    }

    @Override
    public void put(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        innerStore.put(request);
    }

    @Override
    public boolean delete(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        return innerStore.delete(request);
    }
}
