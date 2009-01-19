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

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.Store;
import voldemort.utils.Utils;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

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
public class DefaultStoreClient<K, V> implements StoreClient<K, V> {

    private final Versioned<V> NOT_FOUND = new Versioned<V>(null, null);

    private final Store<K, V> store;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final RoutingStrategy routingStragy;

    public DefaultStoreClient(Store<K, V> store,
                              Serializer<K> keySerializer,
                              Serializer<V> valueSerializer,
                              RoutingStrategy routingStrategy) {
        this.store = Utils.notNull(store);
        this.keySerializer = Utils.notNull(keySerializer);
        this.valueSerializer = Utils.notNull(valueSerializer);
        this.routingStragy = routingStrategy;
    }

    public boolean delete(K key) {
        Versioned<V> versioned = get(key);
        if(versioned == null)
            return false;
        return store.delete(key, versioned.getVersion());
    }

    public boolean delete(K key, Version version) {
        return store.delete(key, version);
    }

    public V getValue(K key, V defaultValue) {
        return get(key).getValue();
    }

    public V getValue(K key) {
        Versioned<V> returned = get(key, null);
        if(returned == null)
            return null;
        else
            return returned.getValue();
    }

    public Versioned<V> get(K key, Versioned<V> defaultValue) {
        List<Versioned<V>> items = store.get(key);
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

    public void put(K key, V value) {
        Versioned<V> versioned = get(key, NOT_FOUND);
        if(versioned == NOT_FOUND)
            versioned = new Versioned<V>(value, new VectorClock());
        versioned.setObject(value);
        store.put(key, versioned);
    }

    public boolean putIfNotObsolete(K key, Versioned<V> versioned) {
        try {
            store.put(key, versioned);
            return true;
        } catch(ObsoleteVersionException e) {
            return false;
        }
    }

    public void put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        store.put(key, versioned);
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

    public Serializer<K> getKeySerializer() {
        return this.keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return this.valueSerializer;
    }

    public List<Node> getResponsibleNodes(K key) {
        if(this.routingStragy == null)
            throw new UnsupportedOperationException("This store client has no routing strategy.");
        return this.routingStragy.routeRequest(keySerializer.toBytes(key));
    }

}
