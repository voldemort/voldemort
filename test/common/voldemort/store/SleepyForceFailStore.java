package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class SleepyForceFailStore<K, V, T> extends ForceFailStore<K, V, T> {

    private long sleepTimeMs;

    public SleepyForceFailStore(Store<K, V, T> innerStore, VoldemortException e, long sleepTimeInMs) {
        super(innerStore, e);
        this.sleepTimeMs = sleepTimeInMs;
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return super.delete(key, version);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return super.get(key, transforms);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            return super.getAll(keys, transforms);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        try {
            Thread.sleep(sleepTimeMs);
            super.put(key, value, transforms);
        } catch(InterruptedException e) {
            throw new VoldemortException(e);
        }
    }

}
