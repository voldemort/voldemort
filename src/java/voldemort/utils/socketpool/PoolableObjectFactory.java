package voldemort.utils.socketpool;

/**
 * Basic interface for poolable Object Factory
 * 
 * @author bbansal
 * 
 */
public interface PoolableObjectFactory<K, V> {

    V create(K key) throws Exception;

    void destroy(K key, V obj) throws Exception;

    boolean validate(K key, V value) throws Exception;

    V activate(K key, V value) throws Exception;

    V passivate(K key, V value) throws Exception;
}
