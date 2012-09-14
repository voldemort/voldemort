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

import voldemort.serialization.Serializer;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * A StorageEngine that handles serialization to bytes, transforming each
 * request to a request to StorageEngine<byte[],byte[], byte[]>
 * 
 * 
 * @param <K> The key type
 * @param <V> The value type
 * @param <T> the transforms type
 */
public class SerializingStorageEngine<K, V, T> extends SerializingStore<K, V, T> implements
        StorageEngine<K, V, T> {

    private final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    public SerializingStorageEngine(StorageEngine<ByteArray, byte[], byte[]> innerStorageEngine,
                                    Serializer<K> keySerializer,
                                    Serializer<V> valueSerializer,
                                    Serializer<T> transformsSerializer) {
        super(innerStorageEngine, keySerializer, valueSerializer, transformsSerializer);
        this.storageEngine = Utils.notNull(innerStorageEngine);
    }

    public static <K1, V1, T1> SerializingStorageEngine<K1, V1, T1> wrap(StorageEngine<ByteArray, byte[], byte[]> s,
                                                                         Serializer<K1> k,
                                                                         Serializer<V1> v,
                                                                         Serializer<T1> t) {
        return new SerializingStorageEngine<K1, V1, T1>(s, k, v, t);
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new EntriesIterator(storageEngine.entries());
    }

    public ClosableIterator<K> keys() {
        return new KeysIterator(storageEngine.keys());
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries(int partition) {
        return new EntriesIterator(storageEngine.entries(partition));
    }

    public ClosableIterator<K> keys(int partition) {
        return new KeysIterator(storageEngine.keys(partition));
    }

    public void truncate() {
        storageEngine.truncate();
    }

    private class KeysIterator implements ClosableIterator<K> {

        private final ClosableIterator<ByteArray> iterator;

        public KeysIterator(ClosableIterator<ByteArray> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public K next() {
            ByteArray key = iterator.next();
            if(key == null)
                return null;
            return getKeySerializer().toObject(key.get());
        }

        public void remove() {
            iterator.remove();
        }

        public void close() {
            iterator.close();
        }
    }

    private class EntriesIterator implements ClosableIterator<Pair<K, Versioned<V>>> {

        private final ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator;

        public EntriesIterator(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public Pair<K, Versioned<V>> next() {
            Pair<ByteArray, Versioned<byte[]>> keyAndVal = iterator.next();
            if(keyAndVal == null) {
                return null;
            } else {
                Versioned<byte[]> versioned = keyAndVal.getSecond();
                return Pair.create(getKeySerializer().toObject(keyAndVal.getFirst().get()),
                                   new Versioned<V>(getValueSerializer().toObject(versioned.getValue()),
                                                    versioned.getVersion()));
            }

        }

        public void remove() {
            iterator.remove();
        }

        public void close() {
            iterator.close();
        }
    }

    public boolean isPartitionAware() {
        return storageEngine.isPartitionAware();
    }

    public boolean isPartitionScanSupported() {
        return storageEngine.isPartitionScanSupported();
    }
}
