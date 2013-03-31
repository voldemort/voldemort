package voldemort.store;

import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class AbstractStorageEngine<K, V, T> extends AbstractStore<K, V, T> implements
        StorageEngine<K, V, T> {

    public AbstractStorageEngine(String name) {
        super(name);
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return null;
    }

    @Override
    public ClosableIterator<K> keys() {
        return null;
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries(int partitionId) {
        return null;
    }

    @Override
    public ClosableIterator<K> keys(int partitionId) {
        return null;
    }

    @Override
    public void truncate() {}

    @Override
    public boolean isPartitionAware() {
        return false;
    }

    @Override
    public boolean isPartitionScanSupported() {
        return false;
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
