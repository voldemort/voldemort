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

import voldemort.client.RoutingTier;
import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.ChainedResolver;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.TimeBasedInconsistencyResolver;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class RESTClient<K, V> implements StoreClient<K, V> {

    private Store<K, V, Object> clientStore = null;
    private SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private StoreDefinition storeDef;

    /**
     * A REST ful equivalent of the DefaultStoreClient. This uses the R2Store to
     * interact with the RESTful Coordinator
     * 
     * @param bootstrapURL The bootstrap URL of the Voldemort cluster
     * @param storeName Name of the store to interact with
     */
    public RESTClient(String bootstrapURL, String storeName) {

        String baseURL = "http://" + bootstrapURL.split(":")[1].substring(2) + ":8080";
        // The lowest layer : Transporting request to coordinator
        Store<ByteArray, byte[], byte[]> store = (Store<ByteArray, byte[], byte[]>) new R2Store(baseURL,
                                                                                                "R2Store");

        // TODO
        // Get the store definition so that we can learn the Serialization
        // and
        // compression properties

        // TODO
        // Add compression layer

        // Add Serialization layer

        // Set the following values although we don't need them
        // TODO: Fix this, so that we only need to set the needed parameters
        storeDef = new StoreDefinitionBuilder().setName(storeName)
                                               .setType("bdb")
                                               .setKeySerializer(new SerializerDefinition("string"))
                                               .setValueSerializer(new SerializerDefinition("string"))
                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                               .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                               .setReplicationFactor(1)
                                               .setPreferredReads(1)
                                               .setRequiredReads(1)
                                               .setPreferredWrites(1)
                                               .setRequiredWrites(1)
                                               .build();
        Serializer<K> keySerializer = (Serializer<K>) serializerFactory.getSerializer(storeDef.getKeySerializer());
        Serializer<V> valueSerializer = (Serializer<V>) serializerFactory.getSerializer(storeDef.getValueSerializer());
        clientStore = SerializingStore.wrap(store, keySerializer, valueSerializer, null);

        // Add inconsistency Resolving layer
        InconsistencyResolver<Versioned<V>> secondaryResolver = new TimeBasedInconsistencyResolver();
        clientStore = new InconsistencyResolvingStore<K, V, Object>(clientStore,
                                                                    new ChainedResolver<Versioned<V>>(new VectorClockInconsistencyResolver(),
                                                                                                      secondaryResolver));
    }

    @Override
    public V getValue(K key) {
        return getValue(key, null);
    }

    @Override
    public V getValue(K key, V defaultValue) {
        Versioned<V> retVal = get(key);
        return retVal.getValue();
    }

    @Override
    public Versioned<V> get(K key) {
        return get(key, null);
    }

    @Override
    public Versioned<V> get(K key, Object transforms) {
        return this.clientStore.get(key, null).get(0);
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys) {
        return null;
    }

    @Override
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, Map<K, Object> transforms) {
        return null;
    }

    @Override
    public Versioned<V> get(K key, Versioned<V> defaultValue) {
        return this.clientStore.get(key, null).get(0);
    }

    @Override
    public Version put(K key, V value) {
        clientStore.put(key, new Versioned<V>(value), null);
        return new VectorClock();
    }

    @Override
    public Version put(K key, V value, Object transforms) {
        return put(key, value);
    }

    @Override
    public Version put(K key, Versioned<V> versioned) throws ObsoleteVersionException {
        clientStore.put(key, versioned, null);
        return new VectorClock();
    }

    @Override
    public boolean putIfNotObsolete(K key, Versioned<V> versioned) {
        return false;
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
        return false;
    }

    @Override
    public boolean delete(K key, Version version) {
        return false;
    }

    @Override
    public List<Node> getResponsibleNodes(K key) {
        return null;
    }

}
