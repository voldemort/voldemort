package voldemort.utils.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A simple implementation of a per-key resource pool. <br>
 * <ul>
 * <li>blocks if resource is not available.
 * <li>allocates resources in FIFO order
 * <li>Pools are per key and there is no global maximum pool limit.
 * </ul>
 */
public class KeyedResourcePool<K, V> {

    private static final Logger logger = Logger.getLogger(KeyedResourcePool.class.getName());

    private final ResourceFactory<K, V> objectFactory;
    private final ConcurrentMap<K, Pool<V>> resourcesMap;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final long timeoutNs;
    private final int poolMaxSize;
    private final boolean isFair;

    public KeyedResourcePool(ResourceFactory<K, V> objectFactory, ResourcePoolConfig config) {
        this.objectFactory = Utils.notNull(objectFactory);
        this.timeoutNs = Utils.notNull(config).getTimeout(TimeUnit.NANOSECONDS);
        this.poolMaxSize = config.getMaxPoolSize();
        this.resourcesMap = new ConcurrentHashMap<K, Pool<V>>();
        this.isFair = config.isFair();
    }

    /**
     * Create a new pool
     * 
     * @param <K> The type of the keys
     * @param <V> The type of the values
     * @param factory The factory that creates objects
     * @param config The pool config
     * @return The created pool
     */
    public static <K, V> KeyedResourcePool<K, V> create(ResourceFactory<K, V> factory,
                                                        ResourcePoolConfig config) {
        return new KeyedResourcePool<K, V>(factory, config);
    }

    /**
     * Create a new pool using the defaults
     * 
     * @param <K> The type of the keys
     * @param <V> The type of the values
     * @param factory The factory that creates objects
     * @return The created pool
     */
    public static <K, V> KeyedResourcePool<K, V> create(ResourceFactory<K, V> factory) {
        return create(factory, new ResourcePoolConfig());
    }

    /**
     * Checkout a resource if one is immediately available. If none is available
     * and we have created fewer than the max size resources, then create a new
     * one. If no resources are available and we are already at the max size
     * then block for up to the maximum time specified. When we hit the maximum
     * time, if we still have not retrieved a valid resource throw an exception.
     * 
     * This method is guaranteed to either return a valid resource in the pool
     * timeout + object creation time or throw an exception. If an exception is
     * thrown, resource is guaranteed to be destroyed.
     * 
     * 
     * @param key The key to checkout the resource for
     * @return The resource
     */
    public V checkout(K key) throws Exception {
        checkNotClosed();

        long startNs = System.nanoTime();
        Pool<V> resources = getResourcePoolForKey(key);

        V resource = null;
        try {
            checkNotClosed();
            resource = attemptCheckout(key, resources);

            if(resource == null) {
                long timeRemainingNs = this.timeoutNs - (System.nanoTime() - startNs);
                if(timeRemainingNs < 0)
                    throw new TimeoutException("Could not acquire resource in "
                                               + (this.timeoutNs / Time.NS_PER_MS) + " ms.");

                resource = resources.blockingGet(timeoutNs);
                if(resource == null) {
                    throw new TimeoutException("Timed out wait for resource after "
                                               + (timeoutNs / Time.NS_PER_MS) + " ms.");
                }
            }

            if(!objectFactory.validate(key, resource))
                throw new ExcessiveInvalidResourcesException(1);
        } catch(Exception e) {
            destroyResource(key, resources, resource);
            throw e;
        }
        return resource;
    }

    /*
     * Get a free resource if one exists. If not create one if there is space.
     * If you create one, try again to get a free resource. This method does not
     * block. It either returns null or a resource.
     */
    private V attemptCheckout(K key, Pool<V> pool) throws Exception {
        V resource = pool.nonBlockingGet();
        if(resource != null)
            return resource;

        // Attempt to expand the queue and retry.
        if(pool.size.get() < this.poolMaxSize) {
            if(attemptGrow(key, pool)) {
                resource = pool.nonBlockingGet();
            }
        }

        return resource;
    }

    /*
     * Attempt to create a new object and add it to the pool--this only happens
     * if there is room for the new object. This method does not block.
     */
    private boolean attemptGrow(K key, Pool<V> pool) throws Exception {
        // attempt to increment, and if the incremented value is less
        // than the pool size then create a new resource
        if(pool.size.incrementAndGet() <= this.poolMaxSize) {
            try {
                V resource = objectFactory.create(key);
                pool.nonBlockingPut(resource);
            } catch(Exception e) {
                pool.size.decrementAndGet();
                throw e;
            }
        } else {
            pool.size.decrementAndGet();
            return false;
        }
        return true;
    }

    /*
     * Get the pool for the given key. If no pool exists, create one.
     */
    private Pool<V> getResourcePoolForKey(K key) {
        Pool<V> pool = resourcesMap.get(key);
        if(pool == null) {
            pool = new Pool<V>(this.poolMaxSize, this.isFair);
            resourcesMap.putIfAbsent(key, pool);
            pool = resourcesMap.get(key);
        }
        return pool;
    }

    /*
     * A safe wrapper to destroy the given resource that catches any user
     * exceptions
     */
    private void destroyResource(K key, Pool<V> resources, V resource) {
        if(resource != null) {
            try {
                objectFactory.destroy(key, resource);
            } catch(Exception e) {
                logger.error("Exception while destorying invalid resource:", e);
            } finally {
                resources.size.decrementAndGet();
            }
        }
    }

    /**
     * Check the given resource back into the pool
     * 
     * @param key The key for the resource
     * @param resource The resource
     */
    public void checkin(K key, V resource) throws Exception {
        Pool<V> pool = resourcesMap.get(key);
        if(pool == null)
            throw new IllegalArgumentException("Invalid key '" + key
                                               + "': no resource pool exists for that key.");
        if(isOpen.get() && objectFactory.validate(key, resource)) {
            boolean success = pool.nonBlockingPut(resource);
            if(!success) {
                destroyResource(key, pool, resource);
                throw new IllegalStateException("Checkin failed is the pool already full?");
            }
        } else {
            destroyResource(key, pool, resource);
        }
    }

    /**
     * Close the pool. This will destroy all checked in resource immediately.
     * Once closed all attempts to checkout a new resource will fail. All
     * resources checked in after close is called will be immediately destroyed.
     */
    public void close() {
        // change state to false and allow one thread.
        if(isOpen.compareAndSet(true, false)) {
            for(Entry<K, Pool<V>> entry: resourcesMap.entrySet()) {
                Pool<V> pool = entry.getValue();
                // destroy each resource in the queue
                for(V value = pool.nonBlockingGet(); value != null; value = pool.nonBlockingGet())
                    destroyResource(entry.getKey(), entry.getValue(), value);
                resourcesMap.remove(entry.getKey());
            }
        }
    }

    public void close(K key) {
        Pool<V> pool = resourcesMap.get(key);

        if(pool == null)
            throw new IllegalArgumentException("Invalid key '" + key
                                               + "': no resource pool exists for that key.");

        List<V> list = pool.close();

        // destroy each resource currently in the queue
        for(V value: list)
            destroyResource(key, pool, value);
    }

    /**
     * Return the total number of resources for the given key whether they are
     * currently checked in or checked out.
     * 
     * @param k The key
     * @return The count
     */
    public int getTotalResourceCount(K k) {
        Pool<V> pool = this.resourcesMap.get(k);
        return pool.size.get();
    }

    /**
     * Get the count of all resources for all pools
     * 
     * @return The count of resources
     */
    public int getTotalResourceCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcesMap.entrySet())
            count += entry.getValue().size.get();
        return count;
    }

    /**
     * Return the number of resources for the given key that are currently
     * sitting idle in the pool waiting to be checked out.
     * 
     * @param k The key
     * @return The count
     */
    public int getCheckedInResourcesCount(K k) {
        Pool<V> pool = this.resourcesMap.get(k);
        return pool.queue.size();
    }

    /**
     * Get the count of resources for all pools currently checkedin
     * 
     * @return The count of resources
     */
    public int getCheckedInResourceCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcesMap.entrySet())
            count += entry.getValue().queue.size();
        return count;
    }

    /*
     * Check that the pool is not closed, and throw an IllegalStateException if
     * it is.
     */
    private void checkNotClosed() {
        if(!isOpen.get())
            throw new IllegalStateException("Pool is closed!");
    }

    /**
     * A simple pool that uses an ArrayBlockingQueue
     */
    private static class Pool<V> {

        final BlockingQueue<V> queue;
        final AtomicInteger size = new AtomicInteger(0);

        public Pool(int defaultPoolSize, boolean isFair) {
            queue = new ArrayBlockingQueue<V>(defaultPoolSize, isFair);
        }

        public V nonBlockingGet() {
            return this.queue.poll();
        }

        public V blockingGet(long timeoutNs) throws InterruptedException {
            return this.queue.poll(timeoutNs, TimeUnit.NANOSECONDS);
        }

        public boolean nonBlockingPut(V v) {
            return this.queue.offer(v);
        }

        public List<V> close() {
            List<V> list = new ArrayList<V>();
            queue.drainTo(list);
            return list;
        }

    }
}
