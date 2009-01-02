package voldemort.store;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class SleepyStore<K, V> extends DelegatingStore<K, V> {

    private final long sleepTimeMs;

    public SleepyStore(long sleepTimeMs, Store<K, V> innerStore) {
        super(innerStore);
        this.sleepTimeMs = sleepTimeMs;
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return getInnerStore().delete(key, version);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return getInnerStore().get(key);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            getInnerStore().put(key, value);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

}
