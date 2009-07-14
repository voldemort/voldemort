package voldemort.utils.socketpool;

import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli2.validation.InvalidArgumentException;
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

    private static class Resources<V> {

        final BlockingQueue<V> queue;
        final AtomicInteger size = new AtomicInteger(0);

        public Resources(int defaultPoolSize) {
            queue = new ArrayBlockingQueue<V>(defaultPoolSize, true);
        }
    }

    private static final Logger logger = Logger.getLogger(BlockingKeyedResourcePool.class.getName());

    private final PoolableObjectFactory<K, V> objectFactory;
    private final ResourcePoolConfig config;

    private final ConcurrentMap<K, Resources<V>> resourcesMap;
    private final AtomicBoolean isRunningState = new AtomicBoolean(true);

    public BlockingKeyedResourcePool(PoolableObjectFactory<K, V> objectFactory,
                                     ResourcePoolConfig config) {
        this.objectFactory = objectFactory;
        this.config = config;
        resourcesMap = new ConcurrentHashMap<K, Resources<V>>();
    }

    public V borrowResource(K key) throws Exception {
        if(!isRunningState.get()) {
            throw new IllegalStateException("IllegalState: Cannot borrow when resource pool is closing down.");
        }

        long startTime = System.currentTimeMillis();

        Resources<V> resources = resourcesMap.get(key);
        // check and create blocking queue if needed
        if(resources == null) {
            resources = new Resources<V>(config.getDefaultPoolSize());
            Resources<V> prev = resourcesMap.putIfAbsent(key, resources);

            // another thread won the race, so we discard the resources created
            // by this thread
            if(prev != null)
                resources = prev;
        }

        // get a valid resource
        int numTries = 0;
        V resource = getResource(key,
                                 resources,
                                 config.getBorrowTimeout(),
                                 config.getBorrowTimeoutUnit());
        while(!objectFactory.validate(key, resource)) {
            // destroy bad resource
            logger.warn("resource validate() failed for resource:" + resource);
            cleanDestroyResource(key, resources, resource);

            // try get new resource with updated timeout
            if(++numTries < config.getMaxBorrowTries()) {
                logger.info("trying getResource()" + numTries + " time for key:" + key);
                resource = getResource(key,
                                       resources,
                                       computeNewTimeout(startTime,
                                                         config.getBorrowTimeout(),
                                                         config.getBorrowTimeoutUnit()),
                                       config.getBorrowTimeoutUnit());
            } else {
                throw new TimeoutException("Failed to get Resource within "
                                           + config.getMaxBorrowTries() + " tries..");
            }
        }

        // activate the resource and return.
        return objectFactory.activate(key, resource);
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

    private void cleanDestroyResource(K key, Resources<V> resources, V resource) throws Exception {
        try {
            objectFactory.destroy(key, resource);
        } catch(Exception e) {
            logger.error("Exception while destorying invalid resource:", e);
            resource = null;
        } finally {
            resources.size.decrementAndGet();
        }
    }

    private V getResource(K key, Resources<V> resources, long timeout, TimeUnit timeunit)
            throws Exception {
        if(resources.size.get() < config.getDefaultPoolSize()) {
            // pool can grow if needed
            V resource = getResourceOrGrowPool(key, resources);
            if(null != resource)
                return resource;
        }
        // pool has reached max size, do blocking get()
        V resource = resources.queue.poll(timeout, timeunit);
        if(null != resource)
            return resource;
        else {
            throw new TimeoutException("Timeout while blocked on free resource for key:" + key);
        }
    }

    /**
     * Returns resource if available w/o blocking<br>
     * Tries to grow pool if fails returns null
     * 
     * @param key
     * @return
     * @throws Exception
     */
    private V getResourceOrGrowPool(K key, Resources<V> resources) throws Exception {
        // try getting resource w/o blocking
        V resource = resources.queue.poll();

        if(null == resource) {
            // need to grow pool
            if(resources.size.incrementAndGet() <= config.getDefaultPoolSize()) {
                return objectFactory.create(key);
            } else {
                resources.size.decrementAndGet();
                return null;
            }
        }

        return resource;
    }

    public void returnResource(K key, V resource) throws Exception {
        Resources<V> resources = resourcesMap.get(key);
        if(resources != null) {
            resource = objectFactory.passivate(key, resource);

            if(isRunningState.get()) {
                // Running state add resource back to queue
                resources.queue.offer(resource,
                                      config.getReturnTimeout(),
                                      config.getBorrowTimeoutUnit());
            } else {
                // clean the resource
                cleanDestroyResource(key, resources, resource);
            }
        } else {
            throw new InvalidArgumentException("returnResource() called with invalid key:" + key);
        }
    }

    public void close() throws Exception {
        // change state to false and allow one thread.
        if(isRunningState.compareAndSet(true, false)) {
            for(Entry<K, Resources<V>> entry: resourcesMap.entrySet()) {
                Queue<V> queue = entry.getValue().queue;
                // destroy each resource in the queue
                V value;
                while((value = queue.poll()) != null) {
                    cleanDestroyResource(entry.getKey(), entry.getValue(), value);
                }
                resourcesMap.remove(entry.getKey());
            }
        }
    }
}
