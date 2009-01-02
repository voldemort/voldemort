package voldemort.store;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The basic interface used for storage and storage decorators. Allows the usual
 * crud operations.
 * 
 * @author jay
 * 
 */
public interface Store<K, V> {

    /**
     * Get the value associated with the given key
     * 
     * @param key The key to check for
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    public List<Versioned<V>> get(K key) throws VoldemortException;

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public void put(K key, Versioned<V> value) throws VoldemortException;

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public boolean delete(K key, Version version) throws VoldemortException;

    /**
     * @return The name of the store.
     */
    public String getName();

    /**
     * Close the store.
     * 
     * @throws VoldemortException If closing fails.
     */
    public void close() throws VoldemortException;

}
