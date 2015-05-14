package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class FailingReadsStore<K, V, T> extends AbstractStore<K, V, T> {

    private final InMemoryStorageEngine<K, V, T> engine;

    public FailingReadsStore(String name) {
        super(name);
        this.engine = new InMemoryStorageEngine<K, V, T>(name);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        return engine.delete(key, version);
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }

    @Override
    public java.util.List<Version> getVersions(K key) {
        throw new VoldemortException("Operation failed");
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        engine.put(key, value, transforms);
    }
}
