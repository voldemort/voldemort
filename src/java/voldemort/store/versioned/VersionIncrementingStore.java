package voldemort.store.versioned;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A wrapper that increments the version on the value for puts and delegates all
 * other operations
 * 
 * @author jay
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
public class VersionIncrementingStore<K, V> extends DelegatingStore<K, V> implements Store<K, V> {

    private final short nodeId;
    private final Time time;

    public VersionIncrementingStore(Store<K, V> innerStore, int nodeId, Time time) {
        super(innerStore);
        this.nodeId = (short) nodeId;
        this.time = time;
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        value = value.cloneVersioned();
        VectorClock clock = (VectorClock) value.getVersion();
        clock.incrementVersion(nodeId, time.getMilliseconds());
        super.put(key, value);
    }

}
