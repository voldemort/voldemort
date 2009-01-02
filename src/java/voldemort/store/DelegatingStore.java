package voldemort.store;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

/**
 * A Store template that delegates all operations to an inner store.
 * 
 * Convenient for decorating a store and overriding only certain methods to add
 * behavior.
 * 
 * @author jay
 * 
 */
public class DelegatingStore<K, V> implements Store<K, V> {

    private final Store<K, V> innerStore;

    public DelegatingStore(Store<K, V> innerStore) {
        this.innerStore = Objects.nonNull(innerStore);
    }

    public void close() throws VoldemortException {
        innerStore.close();
    }

    public boolean delete(K key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.delete(key, version);
    }

    public List<Versioned<V>> get(K key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.get(key);
    }

    public String getName() {
        return innerStore.getName();
    }

    public void put(K key, Versioned<V> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        innerStore.put(key, value);
    }

    public Store<K, V> getInnerStore() {
        return innerStore;
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
