package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ForceFailStore<K, V, T> extends DelegatingStore<K, V, T> {

    private final VoldemortException e;
    private final Object identifier;

    private volatile boolean fail = false;

    public ForceFailStore(Store<K, V, T> innerStore) {
        this(innerStore, new VoldemortException("Operation failed!"));
    }

    public ForceFailStore(Store<K, V, T> innerStore, VoldemortException e) {
        this(innerStore, e, "unknown");
    }

    public ForceFailStore(Store<K, V, T> innerStore, VoldemortException e, Object identifier) {
        super(innerStore);
        this.e = e;
        this.identifier = identifier;
    }

    public void setFail(boolean fail) {
        this.fail = fail;
    }

    public Object getIdentifier() {
        return identifier;
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        if(fail)
            throw e;

        getInnerStore().put(key, value, transform);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        if(fail)
            throw e;

        return getInnerStore().delete(key, version);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        if(fail)
            throw e;

        return getInnerStore().getAll(keys, transforms);
    }

    @Override
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        if(fail)
            throw e;

        return getInnerStore().get(key, transform);
    }
}
