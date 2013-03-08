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

package voldemort.store.serialized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.serialization.Serializer;
import voldemort.store.AbstractStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A store that transforms requests to a Store<ByteArray,byte[], byte[]> to a
 * Store<K,V,T>
 * 
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 * @param <T> The type of transform
 */
public class SerializingStore<K, V, T> extends AbstractStore<K, V, T> {

    private final Store<ByteArray, byte[], byte[]> store;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Serializer<T> transformsSerializer;

    public SerializingStore(Store<ByteArray, byte[], byte[]> store,
                            Serializer<K> keySerializer,
                            Serializer<V> valueSerializer,
                            Serializer<T> transformsSerializer) {
        super(store.getName());
        this.store = Utils.notNull(store);
        this.keySerializer = Utils.notNull(keySerializer);
        this.valueSerializer = Utils.notNull(valueSerializer);
        this.transformsSerializer = transformsSerializer;
    }

    public static <K1, V1, T1> SerializingStore<K1, V1, T1> wrap(Store<ByteArray, byte[], byte[]> s,
                                                                 Serializer<K1> k,
                                                                 Serializer<V1> v,
                                                                 Serializer<T1> t) {
        return new SerializingStore<K1, V1, T1>(s, k, v, t);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        return store.delete(keyToBytes(key), version);
    }

    private ByteArray keyToBytes(K key) {
        return new ByteArray(keySerializer.toBytes(key));
    }

    private Map<ByteArray, K> keysToBytes(Iterable<K> keys) {
        Map<ByteArray, K> result = StoreUtils.newEmptyHashMap(keys);
        for(K key: keys)
            result.put(keyToBytes(key), key);
        return result;
    }

    private byte[] transformToBytes(T transform) {
        if(transform == null)
            return null;
        if(transformsSerializer == null)
            return null;
        return transformsSerializer.toBytes(transform);
    }

    private Map<ByteArray, byte[]> transformsToBytes(Map<K, T> transforms) {
        if(transforms == null)
            return null;
        Map<ByteArray, byte[]> result = Maps.newHashMap();
        for(Map.Entry<K, T> transform: transforms.entrySet()) {
            result.put(keyToBytes(transform.getKey()), transformToBytes(transform.getValue()));
        }
        return result;
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        List<Versioned<byte[]>> found = store.get(keyToBytes(key),
                                                  (transformsSerializer != null && transforms != null) ? transformsSerializer.toBytes(transforms)
                                                                                                      : null);
        List<Versioned<V>> results = new ArrayList<Versioned<V>>(found.size());
        for(Versioned<byte[]> versioned: found)
            results.add(new Versioned<V>(valueSerializer.toObject(versioned.getValue()),
                                         versioned.getVersion()));
        return results;
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, K> byteKeyToKey = keysToBytes(keys);
        Map<ByteArray, List<Versioned<byte[]>>> storeResult = store.getAll(byteKeyToKey.keySet(),
                                                                           transformsToBytes(transforms));
        Map<K, List<Versioned<V>>> result = Maps.newHashMapWithExpectedSize(storeResult.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: storeResult.entrySet()) {
            List<Versioned<V>> values = Lists.newArrayListWithExpectedSize(mapEntry.getValue()
                                                                                   .size());
            for(Versioned<byte[]> versioned: mapEntry.getValue())
                values.add(new Versioned<V>(valueSerializer.toObject(versioned.getValue()),
                                            versioned.getVersion()));

            result.put(byteKeyToKey.get(mapEntry.getKey()), values);
        }
        return result;
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        store.put(keyToBytes(key),
                  new Versioned<byte[]>(valueSerializer.toBytes(value.getValue()),
                                        value.getVersion()),
                  transformToBytes(transforms));
    }

    @Override
    public List<Version> getVersions(K key) {
        return store.getVersions(keyToBytes(key));
    }

    @Override
    public void close() {
        store.close();
    }

    protected Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    protected Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case KEY_SERIALIZER:
                return this.keySerializer;
            case VALUE_SERIALIZER:
                return this.valueSerializer;
            default:
                return store.getCapability(capability);
        }
    }
}
