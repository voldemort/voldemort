/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.utils.Pair;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A wrapper for a store {@link StoreClientFactory} which caches requests
 * to <code>getStoreClient</code>
 *
 */
public class CachingStoreClientFactory implements StoreClientFactory {

    private final StoreClientFactory inner;
    private final ConcurrentMap<Pair<String, Object>, StoreClient> cache;

    public CachingStoreClientFactory(StoreClientFactory inner) {
        this.inner = inner;
        this.cache = new ConcurrentHashMap<Pair<String, Object>, StoreClient>();
    }


    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        Pair<String, Object> key = Pair.create(storeName, null);
        if(!cache.containsKey(key)) {
            StoreClient<K, V> result = inner.getStoreClient(storeName);
            cache.putIfAbsent(key, result);
        }

        return cache.get(key);
    }

    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {
        Pair<String, Object> key = Pair.create(storeName, (Object) resolver);
        if(!cache.containsKey(key)) {
            StoreClient<K, V> result = inner.getStoreClient(storeName, resolver);
            cache.putIfAbsent(key, result);
        }

        return cache.get(key);
    }

    public <K, V, T> Store<K, V, T> getRawStore(String storeName,
                                                InconsistencyResolver<Versioned<V>> resolver) {
        return inner.getRawStore(storeName, resolver);
    }

    public void close() {
        try {
            cache.clear();
        } finally {
            inner.close();
        }
    }

    public FailureDetector getFailureDetector() {
        return inner.getFailureDetector();
    }
}
