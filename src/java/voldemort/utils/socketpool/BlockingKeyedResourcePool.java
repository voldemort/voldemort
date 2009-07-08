package voldemort.utils.socketpool;

import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * A very simple naive implementation of a resource pool <br>
 * <ul>
 * <li>blocks if resource is not available.
 * <li>allocates resources in FIFO order
 * <li>Pools are per key and there is no global maximum pool limit.
 * </ul>
 */
public class BlockingKeyedResourcePool<K, V> {

    private static final Logger logger = Logger.getLogger(BlockingKeyedResourcePool.class.getName());

    private final PoolableObjectFactory<K, V> objectFactory;
    private final ResourcePoolConfig config;

    private final ConcurrentMap<K, ArrayBlockingQueue<V>> resourceQueueMap;
    private final ConcurrentMap<K, AtomicInteger> resourceSizeMap;
    private final AtomicBoolean isRunningState = new AtomicBoolean(true);

    public BlockingKeyedResourcePool(PoolableObjectFactory<K, V> objectFactory,
                                     ResourcePoolConfig config) {
        this.objectFactory = objectFactory;
        this.config = config;
        this.resourceQueueMap = new ConcurrentHashMap<K, ArrayBlockingQueue<V>>();
        this.resourceSizeMap = new ConcurrentHashMap<K, AtomicInteger>();
    }

    public V borrowResource(K key) throws Exception {
        if(!isRunningState.get()) {
            throw new RuntimeException("IllegalState: Cannot borrow when resource pool is closing down.");
        }

        long startTime = System.currentTimeMillis();

        // check and create blocking queue if needed
        if(!resourceQueueMap.containsKey(key)) {
            resourceSizeMap.putIfAbsent(key, new AtomicInteger(0));
            resourceQueueMap.putIfAbsent(key,
                                         new ArrayBlockingQueue<V>(config.getDefaultPoolSize(),
                                                                   true));
        }

        // get a valid resource
        V resource = getResource(key, config.getBorrowTimeout(), config.getReturnTimeoutUnit());
        while(!objectFactory.validateObject(key, resource)) {
            // destroy bad resource
            cleanDestroyResource(key, resource);
            // try get new resource with updated timeout.
            resource = getResource(key,
                                   computeNewTimeout(startTime,
                                                     config.getBorrowTimeout(),
                                                     config.getBorrowTimeoutUnit()),
                                   config.getBorrowTimeoutUnit());
        }

        // activate the resource and return.
        return objectFactory.activateObject(key, resource);
    }

    /**
     * Computes leftTimeout = Original Timeout - timeElapsed <br>
     * returns Max (1, leftTimeout) <br>
     * 1 is used instead of 0 to avoid timeout(0) calls which might means block
     * forever.
     * 
     * @param startTime
     * @param borrowTimeout
     * @param unit
     * @return
     */
    private long computeNewTimeout(long startTime, long borrowTimeout, TimeUnit unit) {
        long timeElapsed = TimeUnit.MILLISECONDS.convert((System.currentTimeMillis() - startTime),
                                                         unit);
        return Math.max(1, borrowTimeout - timeElapsed);
    }

    private void cleanDestroyResource(K key, V resource) throws Exception {
        try {
            objectFactory.destroyObject(key, resource);
        } catch(Exception e) {
            // TODO: LOW : should we propagate this back to Client ??
            logger.error("Exception while destorying invalid resource:", e);
            resource = null;
        } finally {
            resourceSizeMap.get(key).decrementAndGet();
        }
    }

    private V getResource(K key, long timeout, TimeUnit timeunit) throws Exception {
        if(resourceSizeMap.get(key).get() < config.getDefaultPoolSize()) {
            // pool can grow if needed
            V resource = getResourceOrGrowPool(key);
            if(null != resource)
                return resource;
        }
        // pool has reached max size, do blocking get()
        return resourceQueueMap.get(key).poll(timeout, timeunit);
    }

    /**
     * Returns resource if available w/o blocking<br>
     * Tries to grow pool if fails returns null
     * 
     * @param key
     * @return
     * @throws Exception
     */
    private V getResourceOrGrowPool(K key) throws Exception {
        // try getting resource w/o blocking
        V resource = resourceQueueMap.get(key).poll();

        if(null == resource) {
            // need to grow pool
            if(resourceSizeMap.get(key).incrementAndGet() <= config.getDefaultPoolSize()) {
                return objectFactory.createObject(key);
            } else {
                resourceSizeMap.get(key).decrementAndGet();
                return null;
            }
        }

        return resource;
    }

    public void returnResource(K key, V resource) throws Exception {
        if(resourceQueueMap.containsKey(key)) {
            resource = objectFactory.passivateObject(key, resource);

            if(isRunningState.get()) {
                // Running state add resource back to queue
                resourceQueueMap.get(key).offer(resource,
                                                config.getReturnTimeout(),
                                                config.getBorrowTimeoutUnit());
            } else {
                // clean the resource
                cleanDestroyResource(key, resource);
            }
        }
    }

    public void close() throws Exception {
        // change state to false and allow one thread.
        if(isRunningState.compareAndSet(true, false)) {
            for(Entry<K, ArrayBlockingQueue<V>> entry: resourceQueueMap.entrySet()) {
                // destroy each resource in the queue
                while(entry.getValue().size() > 0) {
                    cleanDestroyResource(entry.getKey(), entry.getValue().poll());
                }
                // remove queue/size from hashmaps
                resourceQueueMap.remove(entry.getKey());
                resourceSizeMap.remove(entry.getKey());
            }
        }
    }
}
