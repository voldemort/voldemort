package voldemort.utils.pool;

/**
 * Basic interface for poolable Object Factory
 * 
 * 
 */
public interface ResourceFactory<K, V> {

    /**
     * Create the given resource for the key. This is called once for each
     * resource to create it.
     * 
     * @param key The key
     * @return The created resource
     * @throws Exception
     */
    V create(K key) throws Exception;

    /**
     * Destroy the given resource. This is called only when validate() returns
     * false for the resource or the pool is closed.
     * 
     * @param key The key of the resource
     * @param obj The resource
     */
    void destroy(K key, V obj) throws Exception;

    /**
     * Check that the given resource is valid. This is called once on every
     * checkout, so that the checked out resource is guaranteed to be valid
     * (though it could immediately become invalid).
     * 
     * @param key The key of the resource
     * @param value The resource
     * @return True iff the resource is valid
     */
    boolean validate(K key, V value);

    void close();

}
