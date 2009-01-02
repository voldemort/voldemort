package voldemort.store;

import voldemort.utils.ClosableIterator;
import voldemort.versioning.Versioned;

/**
 * A base storage class which is actually responsible for data persistence.  This interface implies
 * all the usual responsibilities of a Store implementation, and in addition
 * 1. The implementation MUST throw an ObsoleteVersionException if the user attempts
 * to put a version which is strictly before an existing version (concurrent is okay)
 * 2. The implementation MUST increment this version number when the value is stored.
 * 3. The implementation MUST contain an ID identifying it as part of the cluster
 * 
 * A hash value can be produced for known subtrees of a StorageEngine
 * 
 * @author jay
 *
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 * 
 */
public interface StorageEngine<K,V> extends Store<K,V> {
	
    /**
     * @return An iterator over the entries in this StorageEngine.  Note that the iterator MUST be closed after use.
     */
	public ClosableIterator<Entry<K,Versioned<V>>> entries();

}
