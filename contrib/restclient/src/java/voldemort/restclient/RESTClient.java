/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.restclient;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * A RESTful equivalent of the DefaultStoreClient. This uses the R2Store to
 * interact with the RESTful Coordinator
 * 
 */
public class RESTClient<K, V> implements StoreClient<K, V> {

    private Store<K, V, Object> clientStore = null;
    private String storeName;

    private static Logger logger = Logger.getLogger(RESTClient.class);

    /**
     * 
     * @param storeName Name of the store to operate against
     * @param store The store used to perform Voldemort operations
     */
    public RESTClient(String storeName, Store<K, V, Object> store) {
        this.clientStore = store;
        this.storeName = storeName;
    }

    @Override
    public V getValue(K key) {
        return getValue(key, null);
    }

    @Override
    public V getValue(K key, V defaultValue) {
        Versioned<V> retVal = get(key);
        if(retVal == null) {
            return defaultValue;
        } else {
            return retVal.getValue();
        }
    }

    @Override
    public Versioned<V> get(K key) {
        return get(key, null);
    }

    @Override
    public Versioned<V> get(K key, Object transforms) {
        List<Versioned<V>> resultList = this.clientStore.get(key, null);
        return getItemOrThrow(key, null, resultList);
    }

    @Override
    public Versioned<V> get(K key, Versioned<V> defaultValue) {
        List<Versioned<V>> resultList = this.clientStore.get(key, null);
        return getItemOrThrow(key, defaultValue, resultList);
    }

    protected Versioned<V> getItemOrThrow(K key, Versioned<V> defaultValue, List<Versioned<V>> items) {
        if(items.size() == 0)
            return defaultValue;
        else if(items.size() == 1)
            return items.get(0);
        else
            throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                + ") = " + items, items);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys) {
        return getAll(keys, null);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, Map<K, Object> transforms) {
        Map<K, List<Versioned<V>>> items = null;
        items = this.clientStore.getAll(keys, null);
        Map<K, Versioned<V>> result = Maps.newHashMapWithExpectedSize(items.size());

        for(Entry<K, List<Versioned<V>>> mapEntry: items.entrySet()) {
            Versioned<V> value = getItemOrThrow(mapEntry.getKey(), null, mapEntry.getValue());
            result.put(mapEntry.getKey(), value);
        }
        return result;
    }

    /**
     * An empty Versioned<V> value object is created and passed on to the actual
     * put operation. It defers the task of obtaining the existing Vector clock
     * to the Receiver of this request.
     */
    @Override
    public Version put(K key, V value) {
        return put(key, new Versioned<V>(value));
    }

    @Override
    public Version put(K key, V value, Object transforms) {
        return put(key, value);
    }

    @Override
    public Version put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        clientStore.put(key, versioned, null);
        return versioned.getVersion();
    }

    @Override
    public boolean putIfNotObsolete(K key, Versioned<V> versioned) {
        try {
            put(key, versioned);
            return true;
        } catch(ObsoleteVersionException e) {
            return false;
        }
    }

    @Override
    public boolean applyUpdate(UpdateAction<K, V> action) {
        return applyUpdate(action, 3);
    }

    @Override
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

    @Override
    public boolean delete(K key) {
        Versioned<V> versioned = get(key);
        if(versioned == null)
            return false;
        return this.clientStore.delete(key, versioned.getVersion());
    }

    @Override
    public boolean delete(K key, Version version) {
        return this.clientStore.delete(key, version);
    }

    @Override
    public List<Node> getResponsibleNodes(K key) {
        return null;
    }

    public void close() {
        this.clientStore.close();
    }

    public String getName() {
        return this.storeName;
    }
}
