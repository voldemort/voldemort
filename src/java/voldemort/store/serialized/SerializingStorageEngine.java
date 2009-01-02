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
