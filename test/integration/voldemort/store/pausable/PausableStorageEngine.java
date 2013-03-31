package voldemort.store.pausable;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A storage engine that can be paused to simulate a failure for testing. While
 * paused all operations on the store will block indefinitely. The methods to
 * pause and unpause are also exposed through JMX.
 * 
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 * @param <T> The type of the transforms
 */
public class PausableStorageEngine<K, V, T> extends AbstractStorageEngine<K, V, T> {

    private static final Logger logger = Logger.getLogger(PausableStorageEngine.class);

    private final InMemoryStorageEngine<K, V, T> inner;
    private final Object condition = new Object();
    private volatile boolean paused;

    public PausableStorageEngine(InMemoryStorageEngine<K, V, T> inner) {
        super(inner.getName());
        this.inner = inner;
    }

    @Override
    public void close() throws VoldemortException {
        inner.close();
    }

    @Override
    public boolean delete(K key, Version version) {
        blockIfNecessary();
        return inner.delete(key);
    }

    private void blockIfNecessary() {
        synchronized(condition) {
            while(paused) {
                try {
                    condition.wait();
                } catch(InterruptedException e) {
                    throw new VoldemortException("Pausable store interrupted while paused.");
                }
            }
        }
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) {
        blockIfNecessary();
        return inner.get(key, transforms);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms) {
        blockIfNecessary();
        return inner.getAll(keys, transforms);
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) {
        blockIfNecessary();
        inner.put(key, value, transforms);
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        blockIfNecessary();
        return inner.entries();
    }

    @Override
    public ClosableIterator<K> keys() {
        blockIfNecessary();
        return inner.keys();
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries(int partition) {
        blockIfNecessary();
        return inner.entries(partition);
    }

    @Override
    public ClosableIterator<K> keys(int partition) {
        blockIfNecessary();
        return inner.keys(partition);
    }

    @Override
    public void truncate() {
        blockIfNecessary();
        inner.deleteAll();
    }

    @Override
    public List<Version> getVersions(K key) {
        blockIfNecessary();
        return inner.getVersions(key);
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        return inner.getCapability(capability);
    }

    @JmxOperation(description = "Pause all operations on the storage engine.")
    public void pause() {
        logger.info("Pausing store '" + getName() + "'.");
        paused = true;
    }

    @JmxOperation(description = "Unpause the storage engine.")
    public void unpause() {
        logger.info("Unpausing store '" + getName() + "'.");
        paused = false;
        synchronized(condition) {
            condition.notifyAll();
        }
    }

    @Override
    public boolean isPartitionAware() {
        return inner.isPartitionAware();
    }

    @Override
    public boolean isPartitionScanSupported() {
        return inner.isPartitionScanSupported();
    }
}
