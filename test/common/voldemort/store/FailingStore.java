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
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store that always throws an exception for every operation
 * 
 * 
 */
public class FailingStore<K, V, T> extends AbstractStore<K, V, T> {

    private final VoldemortException exception;

    public FailingStore(String name) {
        this(name, new VoldemortException("Operation failed!"));
    }

    public FailingStore(String name, VoldemortException e) {
        super(name);
        this.exception = e;
    }

    @Override
    public void close() throws VoldemortException {
        throw exception;
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        throw exception;
    }

    @Override
    public boolean delete(K key, Version value) throws VoldemortException {
        throw exception;
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        throw exception;
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        throw exception;
    }

    @Override
    public java.util.List<Version> getVersions(K key) {
        throw exception;
    }
}
