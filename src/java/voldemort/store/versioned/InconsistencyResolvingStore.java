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

package voldemort.store.versioned;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * A Store that uses a InconsistencyResolver to eliminate some duplicates.
 * 
 * 
 */
/*
 * Note that unlike get and getAll, getVersions is not overridden so the
 * versions are not passed through the inconsistency resolver.
 */
public class InconsistencyResolvingStore<K, V, T> extends DelegatingStore<K, V, T> {

    private final InconsistencyResolver<Versioned<V>> resolver;

    public InconsistencyResolvingStore(Store<K, V, T> innerStore,
                                       InconsistencyResolver<Versioned<V>> resolver) {
        super(innerStore);
        this.resolver = resolver;
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        return resolver.resolveConflicts(super.get(key, transforms));
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        Map<K, List<Versioned<V>>> m = super.getAll(keys, transforms);
        for(Map.Entry<K, List<Versioned<V>>> entry: m.entrySet())
            m.put(entry.getKey(), resolver.resolveConflicts(entry.getValue()));
        return m;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.INCONSISTENCY_RESOLVER)
            return this.resolver;
        else
            return super.getCapability(capability);
    }

    @Override
    public List<Versioned<V>> get(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        if(request.resolveConflicts()) {
            return resolver.resolveConflicts(super.get(request));
        }
        return super.get(request);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(CompositeVoldemortRequest<K, V> request)
            throws VoldemortException {
        Map<K, List<Versioned<V>>> m = super.getAll(request);
        if(request.resolveConflicts()) {
            for(Map.Entry<K, List<Versioned<V>>> entry: m.entrySet()) {
                m.put(entry.getKey(), resolver.resolveConflicts(entry.getValue()));
            }
        }

        return m;
    }

}
