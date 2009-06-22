package voldemort.store.pausable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * A storage engine that can be paused via JMX to simulate a failure for testing
 * 
 * @author jay
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class PausableStorageEngine<K, V> implements StorageEngine<K, V> {

    private static final Logger logger = Logger.getLogger(PausableStorageEngine.class);

    private final InMemoryStorageEngine<K, V> inner;
    private final ReadWriteLock lock;

    public PausableStorageEngine(InMemoryStorageEngine<K, V> inner) {
        super();
        this.inner = inner;
        this.lock = new ReentrantReadWriteLock();
    }

    public void close() throws VoldemortException {
        inner.close();
    }

    public boolean delete(K key, Version version) {
        try {
            lock.readLock().lock();
            return inner.delete(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Versioned<V>> get(K key) {
        try {
            lock.readLock().lock();
            return inner.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) {
        try {
            lock.readLock().lock();
            return inner.getAll(keys);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void put(K key, Versioned<V> value) {
        try {
            lock.readLock().lock();
            inner.put(key, value);
        } finally {
            lock.readLock().unlock();
        }
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return inner.entries();
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
        this.lock.writeLock().lock();
    }

    @JmxOperation(description = "Unpause the storage engine.")
    public void unpause() {
        logger.info("Unpausing store '" + getName() + "'.");
        this.lock.writeLock().unlock();
    }

}
