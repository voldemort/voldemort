package voldemort.store;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.server.storage.KeyLockHandle;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class RandomlyFailingDelegatingStore<K, V, T> extends DelegatingStore<K, V, T> implements
        StorageEngine<K, V, T> {

    private static double FAIL_PROBABILITY = 0.60;
    private final StorageEngine<K, V, T> innerStorageEngine;

    public RandomlyFailingDelegatingStore(StorageEngine<K, V, T> innerStorageEngine) {
        super(innerStorageEngine);
        this.innerStorageEngine = innerStorageEngine;
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new ClosableIterator<Pair<K, Versioned<V>>>() {

            ClosableIterator<Pair<K, Versioned<V>>> iterator = innerStorageEngine.entries();

            @Override
            public void close() {
                iterator.close();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Pair<K, Versioned<V>> next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            @Override
            public void remove() {}
        };
    }

    @Override
    public ClosableIterator<K> keys() {
        return new ClosableIterator<K>() {

            ClosableIterator<K> iterator = innerStorageEngine.keys();

            @Override
            public void close() {
                iterator.close();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public K next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            @Override
            public void remove() {}
        };
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries(final int partition) {
        return new ClosableIterator<Pair<K, Versioned<V>>>() {

            ClosableIterator<Pair<K, Versioned<V>>> iterator = innerStorageEngine.entries(partition);

            @Override
            public void close() {
                iterator.close();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Pair<K, Versioned<V>> next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            @Override
            public void remove() {}
        };
    }

    @Override
    public ClosableIterator<K> keys(final int partition) {
        return new ClosableIterator<K>() {

            ClosableIterator<K> iterator = innerStorageEngine.keys(partition);

            @Override
            public void close() {
                iterator.close();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public K next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            @Override
            public void remove() {}
        };
    }

    @Override
    public void truncate() {
        if(Math.random() > FAIL_PROBABILITY) {
            innerStorageEngine.truncate();
        }

        throw new VoldemortException("Failing now !!");
    }

    @Override
    public boolean isPartitionAware() {
        return innerStorageEngine.isPartitionAware();
    }

    @Override
    public boolean isPartitionScanSupported() {
        return innerStorageEngine.isPartitionScanSupported();
    }

    @Override
    public boolean beginBatchModifications() {
        return false;
    }

    @Override
    public boolean endBatchModifications() {
        return false;
    }

    @Override
    public List<Versioned<V>> multiVersionPut(K key, List<Versioned<V>> values) {
        return innerStorageEngine.multiVersionPut(key, values);
    }

    @Override
    public KeyLockHandle<V> getAndLock(K key) {
        return innerStorageEngine.getAndLock(key);
    }

    @Override
    public void putAndUnlock(K key, KeyLockHandle<V> handle) {
        innerStorageEngine.putAndUnlock(key, handle);
    }

    @Override
    public void releaseLock(KeyLockHandle<V> handle) {
        innerStorageEngine.releaseLock(handle);
    }
}