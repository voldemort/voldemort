package voldemort.store;

import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class CompositeVoldemortRequest<K, V> {

    private final K key;
    private final V rawValue;
    private final Iterable<K> getAllIterableKeys;
    private final Versioned<V> value;
    private Version version;
    private long routingTimeout;
    private final boolean resolveConflicts;
    private final byte operationType;

    public CompositeVoldemortRequest(K key,
                                     V rawValue,
                                     Iterable<K> keys,
                                     Versioned<V> value,
                                     Version version,
                                     long timeout,
                                     boolean resolveConflicts,
                                     byte operationType) {
        this.key = key;
        this.rawValue = rawValue;
        this.getAllIterableKeys = keys;
        this.routingTimeout = timeout;
        this.value = value;
        this.version = version;
        this.resolveConflicts = resolveConflicts;
        this.operationType = operationType;
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

    public void setVersion(Version version) {
        this.version = version;
    }

    public long getRoutingTimeoutInMs() {
        return routingTimeout;
    }

    public void setRoutingTimeoutInMs(long timeout) {
        this.routingTimeout = timeout;
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

    public byte getOperationType() {
        return operationType;
    }

}
