package voldemort.store.versioned;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * A Store that uses a InconsistencyResolver to eleminate some duplicates
 * 
 * @author jay
 * 
 */
public class InconsistencyResolvingStore<K, V> extends DelegatingStore<K, V> {

    private final InconsistencyResolver<Versioned<V>> resolver;

    public InconsistencyResolvingStore(Store<K, V> innerStore,
                                       InconsistencyResolver<Versioned<V>> resolver) {
        super(innerStore);
        this.resolver = resolver;
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        return resolver.resolveConflicts(super.get(key));
    }

}
