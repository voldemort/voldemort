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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.utils.Pair;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

/**
 * A wrapper for a store {@link StoreClientFactory} which caches requests to
 * <code>getStoreClient</code>
 * 
 */
@JmxManaged(description = "A StoreClientFactory which caches clients")
public class CachingStoreClientFactory implements StoreClientFactory {

    private final static Logger logger = Logger.getLogger(CachingStoreClientFactory.class);

    private final StoreClientFactory inner;
    private final ConcurrentMap<Pair<String, Object>, StoreClient<?, ?>> cache;

    public CachingStoreClientFactory(StoreClientFactory inner) {
        this.inner = inner;
        this.cache = new ConcurrentHashMap<Pair<String, Object>, StoreClient<?, ?>>();
    }

    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        Pair<String, Object> key = Pair.create(storeName, null);
        if(!cache.containsKey(key)) {
            StoreClient<K, V> result = inner.getStoreClient(storeName);
            cache.putIfAbsent(key, result);
        }

        return (StoreClient<K, V>) cache.get(key);
    }

    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {
        Pair<String, Object> key = Pair.create(storeName, (Object) resolver);
        if(!cache.containsKey(key)) {
            StoreClient<K, V> result = inner.getStoreClient(storeName, resolver);
            cache.putIfAbsent(key, result);
        }

        return (StoreClient<K, V>) cache.get(key);
    }

    public <K, V, T> Store<K, V, T> getRawStore(String storeName,
                                                InconsistencyResolver<Versioned<V>> resolver) {
        return getRawStore(storeName, resolver);
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

    @JmxOperation(description = "Clear the cache")
    public synchronized void clear() {
        try {
            cache.clear();
        } catch(Exception e) {
            logger.warn("Exception when clearing the cache", e);
        }
    }

    @JmxOperation(description = "Bootstrap all clients in the cache")
    public void bootstrapAllClients() {
        List<StoreClient<?, ?>> allClients = ImmutableList.copyOf(cache.values());
        try {
            for(StoreClient<?, ?> client: allClients) {
                if(client instanceof DefaultStoreClient<?, ?>)
                    ((DefaultStoreClient<?, ?>) client).bootStrap();
                else if(client instanceof LazyStoreClient<?, ?>) {
                    LazyStoreClient<?, ?> lazyStoreClient = (LazyStoreClient<?, ?>) client;
                    ((DefaultStoreClient<?, ?>) lazyStoreClient.getStoreClient()).bootStrap();
                }

            }
        } catch(Exception e) {
            logger.warn("Exception during bootstrapAllClients", e);
        }
    }
}
