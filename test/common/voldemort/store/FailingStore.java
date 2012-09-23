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
 * A store that always throws an exception for every operation
 * 
 * 
 */
public class FailingStore<K, V, T> implements Store<K, V, T> {

    private final String name;
    private final VoldemortException exception;

    public FailingStore(String name) {
        this(name, new VoldemortException("Operation failed!"));
    }

    public FailingStore(String name, VoldemortException e) {
        this.name = Utils.notNull(name);
        this.exception = e;
    }

    public void close() throws VoldemortException {
        throw exception;
    }

    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        throw exception;
    }

    public String getName() {
        return name;
    }

    public boolean delete(K key, Version value) throws VoldemortException {
        throw exception;
    }

    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        throw exception;
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        throw exception;
    }

    public Map<K, Boolean> hasKeys(Iterable<K> keys, boolean exact) {
        throw exception;
    }

    public java.util.List<Version> getVersions(K key) {
        throw exception;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

}
