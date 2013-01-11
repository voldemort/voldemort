package voldemort.store;

import voldemort.VoldemortException;
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

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new ClosableIterator<Pair<K, Versioned<V>>>() {

            ClosableIterator<Pair<K, Versioned<V>>> iterator = innerStorageEngine.entries();

            public void close() {
                iterator.close();
            }

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public Pair<K, Versioned<V>> next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            public void remove() {}
        };
    }

    public ClosableIterator<K> keys() {
        return new ClosableIterator<K>() {

            ClosableIterator<K> iterator = innerStorageEngine.keys();

            public void close() {
                iterator.close();
            }

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public K next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            public void remove() {}
        };
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries(final int partition) {
        return new ClosableIterator<Pair<K, Versioned<V>>>() {

            ClosableIterator<Pair<K, Versioned<V>>> iterator = innerStorageEngine.entries(partition);

            public void close() {
                iterator.close();
            }

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public Pair<K, Versioned<V>> next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            public void remove() {}
        };
    }

    public ClosableIterator<K> keys(final int partition) {
        return new ClosableIterator<K>() {

            ClosableIterator<K> iterator = innerStorageEngine.keys(partition);

            public void close() {
                iterator.close();
            }

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public K next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            public void remove() {}
        };
    }

    public void truncate() {
        if(Math.random() > FAIL_PROBABILITY) {
            innerStorageEngine.truncate();
        }

        throw new VoldemortException("Failing now !!");
    }

    public boolean isPartitionAware() {
        return innerStorageEngine.isPartitionAware();
    }

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
}