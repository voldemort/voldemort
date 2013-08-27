package voldemort.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import voldemort.server.storage.KeyLockHandle;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Occurred;
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
    public List<Versioned<V>> multiVersionPut(K key, List<Versioned<V>> values) {
        KeyLockHandle<V> handle = null;
        try {
            handle = getAndLock(key);
            List<Versioned<V>> obsoleteVals = resolveAndConstructVersionsToPersist(handle.getValues(),
                                                                                   values);
            putAndUnlock(key, handle);
            return obsoleteVals;
        } catch(UnsupportedOperationException uoe) {
            throw new UnsupportedOperationException("multiVersionPut is not supported for "
                                                    + this.getClass().getName());
        } catch(PersistenceFailureException pfe) {
            if(handle != null && !handle.isClosed()) {
                releaseLock(handle);
            }
            throw pfe;
        }
    }

    @Override
    public boolean endBatchModifications() {
        return false;
    }

    /**
     * Computes the final list of versions to be stored, on top of what is
     * currently being stored. Final list is valsInStorage modified in place
     * 
     * 
     * @param valuesInStorage list of versions currently in storage
     * @param multiPutValues list of new versions being written to storage
     * @return list of versions from multiPutVals that were rejected as obsolete
     */
    protected List<Versioned<V>> resolveAndConstructVersionsToPersist(List<Versioned<V>> valuesInStorage,
                                                                      List<Versioned<V>> multiPutValues) {
        List<Versioned<V>> obsoleteVals = new ArrayList<Versioned<V>>(multiPutValues.size());
        // Go over all the values and determine whether the version is
        // acceptable
        for(Versioned<V> value: multiPutValues) {
            Iterator<Versioned<V>> iter = valuesInStorage.iterator();
            boolean obsolete = false;
            // Compare the current version with a set of accepted versions
            while(iter.hasNext()) {
                Versioned<V> curr = iter.next();
                Occurred occurred = value.getVersion().compare(curr.getVersion());
                if(occurred == Occurred.BEFORE) {
                    obsolete = true;
                    break;
                } else if(occurred == Occurred.AFTER) {
                    iter.remove();
                }
            }
            if(obsolete) {
                // add to return value if obsolete
                obsoleteVals.add(value);
            } else {
                // else update the set of accepted versions
                valuesInStorage.add(value);
            }
        }

        return obsoleteVals;
    }

    @Override
    public KeyLockHandle<V> getAndLock(K key) {
        throw new UnsupportedOperationException("getAndLock is not supported for "
                                                + this.getClass().getName());
    }

    @Override
    public void putAndUnlock(K key, KeyLockHandle<V> handle) {
        throw new UnsupportedOperationException("putAndUnlock is not supported for "
                                                + this.getClass().getName());
    }

    @Override
    public void releaseLock(KeyLockHandle<V> handle) {
        throw new UnsupportedOperationException("releaseLock is not supported for "
                                                + this.getClass().getName());
    }
}
