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

public class SleepyStore<K, V, T> extends DelegatingStore<K, V, T> {

    private long sleepTimeMs;

    public SleepyStore(long sleepTimeMs, Store<K, V, T> innerStore) {
        super(innerStore);
        this.sleepTimeMs = sleepTimeMs;
    }

    public void setSleepTimeMs(long sleepTimeMs) {
        this.sleepTimeMs = sleepTimeMs;
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return getInnerStore().delete(key, version);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return getInnerStore().get(key, transforms);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return getInnerStore().getAll(keys, transforms);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            getInnerStore().put(key, value, transforms);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

}
