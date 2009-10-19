package voldemort.store.pausable;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.StorageEngine;
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
 * @author jay
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class PausableStorageEngine<K, V> implements StorageEngine<K, V> {

    private static final Logger logger = Logger.getLogger(PausableStorageEngine.class);

    private final InMemoryStorageEngine<K, V> inner;
    private final Object condition = new Object();
    private volatile boolean paused;

    public PausableStorageEngine(InMemoryStorageEngine<K, V> inner) {
        super();
        this.inner = inner;
    }

    public void close() throws VoldemortException {
        inner.close();
    }

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

    public List<Versioned<V>> get(K key) {
        blockIfNecessary();
        return inner.get(key);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) {
        blockIfNecessary();
        return inner.getAll(keys);
    }

    public void put(K key, Versioned<V> value) {
        blockIfNecessary();
        inner.put(key, value);
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        blockIfNecessary();
        return inner.entries();
    }

    public List<Version> getVersions(K key) {
        blockIfNecessary();
        return inner.getVersions(key);
    }

    public Object getCapability(StoreCapabilityType capability) {
        return inner.getCapability(capability);
    }

    public String getName() {
        return inner.getName();
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

}
