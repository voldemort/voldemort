package voldemort.store;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;

public class ForceFailStore<K, V> extends DelegatingStore<K, V> {

    private final VoldemortException e;
    private final Object identifier;

    private volatile boolean fail = false;

    public ForceFailStore(Store<K, V> innerStore) {
        this(innerStore, new VoldemortException("Operation failed!"));
    }

    public ForceFailStore(Store<K, V> innerStore, VoldemortException e) {
        this(innerStore, e, "unknown");
    }

    public ForceFailStore(Store<K, V> innerStore, VoldemortException e, Object identifier) {
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
    public void put(K key, Versioned<V> value) throws VoldemortException {
        if(fail)
            throw e;

        getInnerStore().put(key, value);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        if(fail)
            throw e;

        return getInnerStore().delete(key, version);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        if(fail)
            throw e;

        return getInnerStore().getAll(keys);    
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        if(fail)
            throw e;

        return getInnerStore().get(key);
    }
}
