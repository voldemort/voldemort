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

package voldemort.client;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.Utils;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * The default {@link voldemort.client.StoreClient StoreClient} implementation
 * you get back from a {@link voldemort.client.StoreClientFactory
 * StoreClientFactory}
 * 
 * @author jay
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
@Threadsafe
public class DefaultStoreClient<K, V> implements StoreClient<K, V> {

    private final StoreClientFactory storeFactory;

    private final int metadataRefreshAttempts;
    private final String storeName;
    private final InconsistencyResolver<Versioned<V>> resolver;
    private volatile Store<K, V> store;

    public DefaultStoreClient(String storeName,
                              InconsistencyResolver<Versioned<V>> resolver,
                              StoreClientFactory storeFactory,
                              int maxMetadataRefreshAttempts) {
        this.storeName = Utils.notNull(storeName);
        this.resolver = resolver;
        this.storeFactory = Utils.notNull(storeFactory);
        this.metadataRefreshAttempts = maxMetadataRefreshAttempts;
        reinit();
    }

    private void reinit() {
        this.store = storeFactory.getRawStore(storeName, resolver);
    }

    public boolean delete(K key) {
        Versioned<V> versioned = get(key);
        if(versioned == null)
            return false;
        return delete(key, versioned.getVersion());
    }

    public boolean delete(K key, Version version) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return store.delete(key, version);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.metadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    public V getValue(K key, V defaultValue) {
        Versioned<V> versioned = get(key);
        if(versioned == null)
            return null;
        else
            return versioned.getValue();
    }

    public V getValue(K key) {
        Versioned<V> returned = get(key, null);
        if(returned == null)
            return null;
        else
            return returned.getValue();
    }

    public Versioned<V> get(K key, Versioned<V> defaultValue) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                List<Versioned<V>> items = store.get(key);
                return getItemOrThrow(key, defaultValue, items);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.metadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    private Versioned<V> getItemOrThrow(K key, Versioned<V> defaultValue, List<Versioned<V>> items) {
        if(items.size() == 0)
            return defaultValue;
        else if(items.size() == 1)
            return items.get(0);
        else
            throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                + ") = " + items, items);
    }

    public Versioned<V> get(K key) {
        return get(key, null);
    }

    public Map<K, Versioned<V>> getAll(Iterable<K> keys) {
        Map<K, List<Versioned<V>>> items = null;
        for(int attempts = 0;; attempts++) {
            if(attempts >= this.metadataRefreshAttempts)
                throw new InvalidMetadataException(this.metadataRefreshAttempts
                                                   + " metadata refresh attempts failed.");
            try {
                items = store.getAll(keys);
                break;
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        Map<K, Versioned<V>> result = Maps.newHashMapWithExpectedSize(items.size());

        for(Entry<K, List<Versioned<V>>> mapEntry: items.entrySet()) {
            Versioned<V> value = getItemOrThrow(mapEntry.getKey(), null, mapEntry.getValue());
            result.put(mapEntry.getKey(), value);
        }
        return result;
    }

    public void put(K key, V value) {
        List<Version> versions = getVersions(key);
        Versioned<V> versioned;
        if(versions.isEmpty())
            versioned = Versioned.value(value, new VectorClock());
        else if(versions.size() == 1)
            versioned = Versioned.value(value, versions.get(0));
        else {
            versioned = get(key, null);
            if(versioned == null)
                versioned = Versioned.value(value, new VectorClock());
            else
                versioned.setObject(value);
        }
        put(key, versioned);
    }

    public boolean putIfNotObsolete(K key, Versioned<V> versioned) {
        try {
            put(key, versioned);
            return true;
        } catch(ObsoleteVersionException e) {
            return false;
        }
    }

    public void put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                store.put(key, versioned);
                return;
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.metadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }

    public boolean applyUpdate(UpdateAction<K, V> action) {
        return applyUpdate(action, 3);
    }

    public boolean applyUpdate(UpdateAction<K, V> action, int maxTries) {
        boolean success = false;
        try {
            for(int i = 0; i < maxTries; i++) {
                try {
                    action.update(this);
                    success = true;
                    return success;
                } catch(ObsoleteVersionException e) {
                    // ignore for now
                }
            }
        } finally {
            if(!success)
                action.rollback();
        }

        // if we got here we have seen too many ObsoleteVersionExceptions
        // and have rolled back the updates
        return false;
    }

    public List<Node> getResponsibleNodes(K key) {
        RoutingStrategy strategy = (RoutingStrategy) store.getCapability(StoreCapabilityType.ROUTING_STRATEGY);
        @SuppressWarnings("unchecked")
        Serializer<K> keySerializer = (Serializer<K>) store.getCapability(StoreCapabilityType.KEY_SERIALIZER);
        return strategy.routeRequest(keySerializer.toBytes(key));
    }

    private Version getVersion(K key) {
        List<Version> versions = getVersions(key);
        if(versions.size() == 0)
            return null;
        else if(versions.size() == 1)
            return versions.get(0);
        else
            throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                + ") = " + versions, versions);
    }

    private List<Version> getVersions(K key) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return store.getVersions(key);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new InvalidMetadataException(this.metadataRefreshAttempts
                                           + " metadata refresh attempts failed.");
    }
}
