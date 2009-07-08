package voldemort.utils.socketpool;

/**
 * Basic interface for poolable Object Factory
 * 
 * @author bbansal
 * 
 */
public interface PoolableObjectFactory<K, V> {

    V createObject(Object K) throws Exception;

    void destroyObject(K key, V obj) throws Exception;

    boolean validateObject(K key, V value) throws Exception;

    V activateObject(K key, V value) throws Exception;

    V passivateObject(K key, V value) throws Exception;
}
