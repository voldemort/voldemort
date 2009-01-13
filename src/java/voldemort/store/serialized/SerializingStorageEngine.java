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
import voldemort.store.Entry;
import voldemort.store.StorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

/**
 * A StorageEngine that handles serialization to bytes, transforming each
 * request to a request to StorageEngine<byte[],byte[]>
 * 
 * @author jay
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
public class SerializingStorageEngine<K, V> extends SerializingStore<K, V> implements
        StorageEngine<K, V> {

    private final StorageEngine<byte[], byte[]> storageEngine;

    public SerializingStorageEngine(StorageEngine<byte[], byte[]> innerStorageEngine,
                                    Serializer<K> keySerializer,
                                    Serializer<V> valueSerializer) {
        super(innerStorageEngine, keySerializer, valueSerializer);
        this.storageEngine = Objects.nonNull(innerStorageEngine);
    }

    public ClosableIterator<Entry<K, Versioned<V>>> entries() {
        return new DelegatingClosableIterator(storageEngine.entries());
    }

    private class DelegatingClosableIterator implements ClosableIterator<Entry<K, Versioned<V>>> {

        private final ClosableIterator<Entry<byte[], Versioned<byte[]>>> iterator;

        public DelegatingClosableIterator(ClosableIterator<Entry<byte[], Versioned<byte[]>>> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public Entry<K, Versioned<V>> next() {
            Entry<byte[], Versioned<byte[]>> next = iterator.next();
            if(next == null) {
                return null;
            } else {
                Versioned<byte[]> versioned = next.getValue();
                return new Entry<K, Versioned<V>>(getKeySerializer().toObject(next.getKey()),
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

}
