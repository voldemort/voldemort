package voldemort.utils.pool;

import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

/**
 * Extends simple implementation of a per-key resource pool with a non-blocking
 * interface to enqueue requests for a resource when one becomes available. <br>
 * <ul>
 * <li>allocates resources in FIFO order
 * <li>Pools are per key and there is no global maximum pool limit.
 * </ul>
 */
public class QueuedKeyedResourcePool<K, V> extends KeyedResourcePool<K, V> {

    public interface ResourceRequest<V> {

        // Invoked with checkedout resource; resource guaranteed to be not-null
        void useResource(V resource);

        // Invoked sometime after deadline. Will never invoke useResource.
        void handleTimeout();

        // Invoked upon resource pool exception. Will never invoke useResource.
        void handleException(Exception e);

        // return deadline (in nanoseconds), after which timeoutCheckout()
        // should be invoked.
        long getDeadlineNs();
    }

    private static final Logger logger = Logger.getLogger(QueuedKeyedResourcePool.class.getName());

    private final ConcurrentMap<K, Queue<ResourceRequest<V>>> requestQueueMap;

    public QueuedKeyedResourcePool(ResourceFactory<K, V> objectFactory, ResourcePoolConfig config) {
        super(objectFactory, config);
        requestQueueMap = new ConcurrentHashMap<K, Queue<ResourceRequest<V>>>();
    }

    /**
     * Create a new queued pool
     * 
     * @param <K> The type of the keys
     * @param <R> The type of requests
     * @param <V> The type of the values
     * @param factory The factory that creates objects
     * @param config The pool config
     * @return The created pool
     */
    public static <K, V> QueuedKeyedResourcePool<K, V> create(ResourceFactory<K, V> factory,
                                                              ResourcePoolConfig config) {
        return new QueuedKeyedResourcePool<K, V>(factory, config);
    }

    /**
     * Create a new queued pool using the defaults
     * 
     * @param <K> The type of the keys
     * @param <R> The type of requests
     * @param <V> The type of the values
     * @param factory The factory that creates objects
     * @return The created pool
     */
    public static <K, V> QueuedKeyedResourcePool<K, V> create(ResourceFactory<K, V> factory) {
        return create(factory, new ResourcePoolConfig());
    }

    /**
     * 
     * @param key The key to checkout the resource for
     * @return The resource
     */
    public void requestResource(K key, ResourceRequest<V> resourceRequest) {
        try {
            V resource = super.checkout(key);
            resourceRequest.useResource(resource);
        } catch(Exception e) {
            resourceRequest.handleException(e);
        }

        /*-
        checkNotClosed();

        long startNs = System.nanoTime();
        Pool<V> resources = getResourcePoolForKey(key);

        V resource = null;
        try {
            checkNotClosed();
            resource = attemptCheckoutGrowCheckout(key, resources);

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
            System.err.println(e.toString());
            throw e;
        }
        return resource;
         */
    }

    /**
     * Check the given resource back into the pool
     * 
     * @param key The key for the resource
     * @param resource The resource
     */
    @Override
    public void checkin(K key, V resource) throws Exception {
        super.checkin(key, resource);
        /*-
        Pool<V> pool = getResourcePoolForExistingKey(key);
        if(isOpen.get() && objectFactory.validate(key, resource)) {
            boolean success = pool.nonBlockingPut(resource);
            if(!success) {
                destroyResource(key, pool, resource);
                throw new IllegalStateException("Checkin failed is the pool already full?");
            }
        } else {
            destroyResource(key, pool, resource);
        }
         */
    }

    /*
     * A safe wrapper to destroy the given request that catches any user
     * exceptions
     */
    protected void destroyRequest(K key,
                                  Queue<ResourceRequest<V>> requestQueue,
                                  ResourceRequest<V> resourceRequest) {
        if(resourceRequest != null) {
            try {
                // objectFactory.destroy(key, resource);
                resourceRequest = null;
            } catch(Exception e) {
                logger.error("Exception while destorying invalid request:", e);
            } finally {
                // resourcePool.size.decrementAndGet();
            }
        }
    }

    /*
     * Get the queue of work for the given key. If no queue exists, create one.
     */
    protected Queue<ResourceRequest<V>> getRequestQueueForKey(K key) {
        Queue<ResourceRequest<V>> requestQueue = requestQueueMap.get(key);
        if(requestQueue == null) {
            requestQueue = new ConcurrentLinkedQueue<ResourceRequest<V>>();
            requestQueueMap.putIfAbsent(key, requestQueue);
            requestQueue = requestQueueMap.get(key);
        }
        return requestQueue;
    }

    /*
     * Get the pool for the given key. If no pool exists, throw an exception.
     */
    protected Queue<ResourceRequest<V>> getRequestQueueForExistingKey(K key) {
        Queue<ResourceRequest<V>> requestQueue = requestQueueMap.get(key);
        if(requestQueue == null) {
            throw new IllegalArgumentException("Invalid key '" + key
                                               + "': no request queue exists for that key.");
        }
        return requestQueue;
    }

    /**
     * Return the number of requests queued up for a given pool.
     * 
     * @param key The key
     * @return The count
     */
    public int getQueuedResourceRequestCount(K key) {
        Queue<ResourceRequest<V>> requestQueue = getRequestQueueForExistingKey(key);
        // FYI: .size() is not constant time in the next call. ;)
        return requestQueue.size();
    }

    /**
     * Return the number of resource requests queued up for all pools.
     * 
     * @return The count of resources
     */
    public int getQueuedResourceRequestCount() {
        int count = 0;
        for(Entry<K, Queue<ResourceRequest<V>>> entry: this.requestQueueMap.entrySet()) {
            // FYI: .size() is not constant time in the next call. ;)
            count += entry.getValue().size();
        }
        // count is approximate in the case of concurrency since .queue.size()
        // for various entries can change while other entries are being counted.
        return count;
    }

    /**
     * Close the pool. This will destroy all checked in resource immediately.
     * Once closed all attempts to checkout a new resource will fail. All
     * resources checked in after close is called will be immediately destroyed.
     */
    @Override
    public void close() {
        super.close();
        /*-
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
         */
    }

    /**
     * "Close" a specific resource pool and request queue by destroying all the
     * resources in the pool and all the requests in the queue. This method does
     * not affect whether any pool or queue is "open" in the sense of permitting
     * new resources to be added or requests to be enqueued.
     * 
     * @param key The key for the pool to close.
     */
    @Override
    public void close(K key) {
        // Destroy enqueued requests first.
        Queue<ResourceRequest<V>> requestQueue = getRequestQueueForExistingKey(key);
        ResourceRequest<V> resourceRequest = requestQueue.poll();
        while(resourceRequest != null) {
            destroyRequest(key, requestQueue, resourceRequest);
            resourceRequest = requestQueue.poll();
        }
        // Destroy resources in the pool second.
        super.close(key);
    }
}
