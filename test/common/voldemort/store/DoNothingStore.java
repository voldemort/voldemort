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
 * A store that does no Harm :)
 * 
 * @author bbansal
 * 
 */
public class DoNothingStore<K, V> implements Store<K, V> {

    private final String name;

    public DoNothingStore(String name) {
        this.name = Utils.notNull(name);
    }

    public void close() throws VoldemortException {
    // Do nothing;
    }

    public List<Versioned<V>> get(K key) throws VoldemortException {
        // do nothing
        return null;
    }

    public String getName() {
        return name;
    }

    public boolean delete(K key, Version value) throws VoldemortException {
        // Do nothing
        return true;
    }

    public void put(K key, Versioned<V> value) throws VoldemortException {
    // Do nothing
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        return null;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public List<Version> getVersions(K key) {
        // Do nothing
        return null;
    }

}
