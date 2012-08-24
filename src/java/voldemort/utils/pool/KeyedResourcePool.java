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

    protected final ResourceFactory<K, V> objectFactory;
    private final ConcurrentMap<K, Pool<V>> resourcePoolMap;
    protected final AtomicBoolean isOpen = new AtomicBoolean(true);
    protected final long timeoutNs;
    private final int poolMaxSize;
    private final boolean isFair;

    public KeyedResourcePool(ResourceFactory<K, V> objectFactory, ResourcePoolConfig config) {
        this.objectFactory = Utils.notNull(objectFactory);
        this.timeoutNs = Utils.notNull(config).getTimeout(TimeUnit.NANOSECONDS);
        this.poolMaxSize = config.getMaxPoolSize();
        this.resourcePoolMap = new ConcurrentHashMap<K, Pool<V>>();
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
        Pool<V> resourcePool = getResourcePoolForKey(key);

        V resource = null;
        try {
            checkNotClosed();
            resource = attemptCheckoutGrowCheckout(key, resourcePool);

            if(resource == null) {
                long timeRemainingNs = this.timeoutNs - (System.nanoTime() - startNs);
                if(timeRemainingNs < 0)
                    throw new TimeoutException("Could not acquire resource in "
                                               + (this.timeoutNs / Time.NS_PER_MS) + " ms.");

                resource = resourcePool.blockingGet(timeoutNs);
                if(resource == null) {
                    throw new TimeoutException("Timed out wait for resource after "
                                               + (timeoutNs / Time.NS_PER_MS) + " ms.");
                }
            }

            if(!objectFactory.validate(key, resource))
                throw new ExcessiveInvalidResourcesException(1);
        } catch(Exception e) {
            destroyResource(key, resourcePool, resource);
            System.err.println(e.toString());
            throw e;
        }
        return resource;
    }

    /*
     * Checkout a free resource if one exists. If not, and there is space, try
     * and create one. If you create one, try and checkout again. Returns null
     * or a resource.
     */
    protected V attemptCheckoutGrowCheckout(K key, Pool<V> pool) throws Exception {
        V resource = attemptCheckout(pool);
        if(resource == null) {
            if(pool.size.get() < this.poolMaxSize) {
                if(attemptGrow(key, pool)) {
                    resource = attemptCheckout(pool);
                }
            }
        }

        return resource;
    }

    /*
     * Get a free resource if one exists. This method does not block. It either
     * returns null or a resource.
     */
    protected V attemptCheckout(Pool<V> pool) throws Exception {
        V resource = pool.nonBlockingGet();
        return resource;
    }

    /*
     * Attempt to create a new object and add it to the pool--this only happens
     * if there is room for the new object. This method does not block. This
     * method returns true if it adds a resource to the pool. (Returning true
     * does not guarantee subsequent checkout will succeed because concurrent
     * checkouts may occur.)
     */
    protected boolean attemptGrow(K key, Pool<V> pool) throws Exception {
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
    protected Pool<V> getResourcePoolForKey(K key) {
        Pool<V> resourcePool = resourcePoolMap.get(key);
        if(resourcePool == null) {
            resourcePool = new Pool<V>(this.poolMaxSize, this.isFair);
            resourcePoolMap.putIfAbsent(key, resourcePool);
            resourcePool = resourcePoolMap.get(key);
        }
        return resourcePool;
    }

    /*
     * Get the pool for the given key. If no pool exists, throw an exception.
     */
    protected Pool<V> getResourcePoolForExistingKey(K key) {
        Pool<V> resourcePool = resourcePoolMap.get(key);
        if(resourcePool == null) {
            throw new IllegalArgumentException("Invalid key '" + key
                                               + "': no resource pool exists for that key.");
        }
        return resourcePool;
    }

    /*
     * A safe wrapper to destroy the given resource that catches any user
     * exceptions
     */
    protected void destroyResource(K key, Pool<V> resourcePool, V resource) {
        if(resource != null) {
            try {
                objectFactory.destroy(key, resource);
            } catch(Exception e) {
                logger.error("Exception while destorying invalid resource:", e);
            } finally {
                resourcePool.size.decrementAndGet();
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
        Pool<V> resourcePool = getResourcePoolForExistingKey(key);
        if(isOpen.get() && objectFactory.validate(key, resource)) {
            boolean success = resourcePool.nonBlockingPut(resource);
            if(!success) {
                destroyResource(key, resourcePool, resource);
                throw new IllegalStateException("Checkin failed is the pool already full?");
            }
        } else {
            destroyResource(key, resourcePool, resource);
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
            for(Entry<K, Pool<V>> entry: resourcePoolMap.entrySet()) {
                Pool<V> pool = entry.getValue();
                // destroy each resource in the queue
                for(V value = pool.nonBlockingGet(); value != null; value = pool.nonBlockingGet())
                    destroyResource(entry.getKey(), entry.getValue(), value);
                resourcePoolMap.remove(entry.getKey());
            }
        }
    }

    /**
     * "Close" a specific resource pool by destroying all the resources in the
     * pool. This method does not affect whether any pool is "open" in the sense
     * of permitting new resources to be added to it.
     * 
     * @param key The key for the pool to close.
     */
    public void close(K key) {
        Pool<V> resourcePool = getResourcePoolForExistingKey(key);
        List<V> list = resourcePool.close();
        // destroy each resource currently in the queue
        for(V value: list)
            destroyResource(key, resourcePool, value);
    }

    /**
     * Return the total number of resources for the given key whether they are
     * currently checked in or checked out.
     * 
     * @param key The key
     * @return The count
     */
    public int getTotalResourceCount(K key) {
        Pool<V> resourcePool = getResourcePoolForExistingKey(key);
        return resourcePool.size.get();
    }

    /**
     * Get the count of all resources for all pools.
     * 
     * @return The count of resources.
     */
    public int getTotalResourceCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcePoolMap.entrySet())
            count += entry.getValue().size.get();
        // count is approximate in the case of concurrency since .size.get() for
        // various entries can change while other entries are being counted.
        return count;
    }

    /**
     * Return the number of resources for the given key that are currently
     * sitting idle in the pool waiting to be checked out.
     * 
     * @param key The key
     * @return The count
     */
    public int getCheckedInResourcesCount(K key) {
        Pool<V> resourcePool = getResourcePoolForExistingKey(key);
        return resourcePool.queue.size();
    }

    /**
     * Get the count of resources for all pools currently checkedin
     * 
     * @return The count of resources
     */
    public int getCheckedInResourceCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcePoolMap.entrySet())
            count += entry.getValue().queue.size();
        // count is approximate in the case of concurrency since .queue.size()
        // for
        // various entries can change while other entries are being counted.
        return count;
    }

    /*
     * Check that the pool is not closed, and throw an IllegalStateException if
     * it is.
     */
    protected void checkNotClosed() {
        if(!isOpen.get())
            throw new IllegalStateException("Pool is closed!");
    }

    /**
     * A simple pool that uses an ArrayBlockingQueue
     */
    protected static class Pool<V> {

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
