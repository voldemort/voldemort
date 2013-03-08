package voldemort.store;

import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class VoldemortRequestWrapper<K, V> {

    private final K key;
    private final V rawValue;
    private final Iterable<K> getAllIterableKeys;
    private final Versioned<V> value;
    private final Version version;
    private final long routingTimeout;
    private final boolean resolveConflicts;

    // To be used in get call
    public VoldemortRequestWrapper(K key, long timeout, boolean resolveConflicts) {
        this(key, null, null, null, null, timeout, resolveConflicts);
    }

    // To be used in getAll call
    public VoldemortRequestWrapper(Iterable<K> keys, long timeout, boolean resolveConflicts) {
        this(null, null, keys, null, null, timeout, resolveConflicts);
    }

    // To be used in put call
    public VoldemortRequestWrapper(K key, V rawValue, long timeout) {
        this(key, rawValue, null, null, null, timeout, true);
    }

    // To be used in versioned put call
    public VoldemortRequestWrapper(K key, Versioned<V> value, long timeout) {
        this(key, null, null, value, null, timeout, true);
    }

    public VoldemortRequestWrapper(K key,
                                   V rawValue,
                                   Iterable<K> keys,
                                   Versioned<V> value,
                                   Version version,
                                   long timeout,
                                   boolean resolveConflicts) {
        this.key = key;
        this.rawValue = rawValue;
        this.getAllIterableKeys = keys;
        this.routingTimeout = timeout;
        this.value = value;
        this.version = version;
        this.resolveConflicts = resolveConflicts;
    }

    public K getKey() {
        return key;
    }

    public Versioned<V> getValue() {
        return value;
    }

    public Version getVersion() {
        return version;
    }

    public long getRoutingTimeout() {
        return routingTimeout;
    }

    public boolean resolveConflicts() {
        return resolveConflicts;
    }

    public Iterable<K> getIterableKeys() {
        return getAllIterableKeys;
    }

    public V getRawValue() {
        return rawValue;
    }

}
