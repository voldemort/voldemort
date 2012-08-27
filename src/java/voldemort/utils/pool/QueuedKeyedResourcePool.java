package voldemort.utils.pool;

import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import voldemort.store.UnreachableStoreException;

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

        // Invoked with checked out resource; resource guaranteed to be
        // not-null.
        void useResource(V resource);

        // Invoked sometime after deadline. Will never invoke useResource.
        void handleTimeout();

        // Invoked upon resource pool exception. Will never invoke useResource.
        void handleException(Exception e);

        // Returns deadline (in nanoseconds), after which handleTimeout()
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
     * This method is the asynchronous (nonblocking) version of
     * KeyedResourcePool.checkout. This method necessarily has a different
     * function declaration (i.e., arguments passed and return type).
     * 
     * This method either checks out a resource and uses that resource or
     * enqueues a request to checkout the resource. I.e., there is a
     * non-blocking fast-path that is tried optimistically.
     * 
     * @param key The key to checkout the resource for
     * @return The resource
     * 
     */
    public void requestResource(K key, ResourceRequest<V> resourceRequest) {
        checkNotClosed();

        // Non-blocking checkout attempt iff requestQueue is empty. If
        // requestQueue is not empty and we attempted non-blocking checkout,
        // then FIFO at risk.
        Queue<ResourceRequest<V>> requestQueue = getRequestQueueForKey(key);
        if(requestQueue.isEmpty()) {
            Pool<V> resourcePool = getResourcePoolForKey(key);
            V resource = null;

            try {
                resource = attemptCheckoutGrowCheckout(key, resourcePool);
            } catch(Exception e) {
                super.destroyResource(key, resourcePool, resource);
                resourceRequest.handleException(e);
            }
            if(resource != null) {
                // TODO: Is another try/catch block needed anywhere to ensure
                // resource is destroyed if/when anything bad happens in
                // useResource method?
                resourceRequest.useResource(resource);
                return;
            }
        }

        requestQueue.add(resourceRequest);
    }

    /**
     * Pops resource requests off the queue until queue is empty or an unexpired
     * resource request is found. Invokes .handleTimeout on all expired resource
     * requests popped off the queue.
     * 
     * @return null or a valid ResourceRequest
     */
    private ResourceRequest<V> getNextUnexpiredResourceRequest(Queue<ResourceRequest<V>> requestQueue) {
        ResourceRequest<V> resourceRequest = requestQueue.poll();
        while(resourceRequest != null) {
            if(resourceRequest.getDeadlineNs() < System.nanoTime()) {
                resourceRequest.handleTimeout();
                resourceRequest = requestQueue.poll();
            } else {
                break;
            }
        }
        return resourceRequest;
    }

    /**
     * Attempts to checkout a resource so that one queued request can be
     * serviced.
     * 
     * @param key The key for which to process the requestQueue
     * @return true iff an item was processed from the Queue.
     */
    private boolean processQueue(K key) throws Exception {
        Queue<ResourceRequest<V>> requestQueue = getRequestQueueForKey(key);
        if(requestQueue.isEmpty()) {
            return false;
        }

        // Attempt to get a resource.
        Pool<V> resourcePool = getResourcePoolForKey(key);
        V resource = null;

        try {
            // Always attempt to grow to deal with destroyed resources.
            attemptGrow(key, resourcePool);
            resource = attemptCheckout(resourcePool);
        } catch(Exception e) {
            super.destroyResource(key, resourcePool, resource);
        }
        if(resource == null) {
            return false;
        }

        // With resource in hand, process the resource requests
        ResourceRequest<V> resourceRequest = getNextUnexpiredResourceRequest(requestQueue);
        if(resourceRequest == null) {
            // Did not use the resource!
            super.checkin(key, resource);
            return false;
        }

        resourceRequest.useResource(resource);
        return true;
    }

    /**
     * Check the given resource back into the pool
     * 
     * @param key The key for the resource
     * @param resource The resource
     */
    @Override
    public void checkin(K key, V resource) throws Exception {
        // TODO: Unclear if invoking checkin and then invoking processQueue is
        // "fair" or is "FIFO". In particular, is super.checkout invoked
        // directly? If so, how should such a blocking checkout interact with
        // non-blocking checkouts?
        super.checkin(key, resource);
        while(processQueue(key)) {}
    }

    /*
     * A safe wrapper to destroy the given resource request.
     */
    protected void destroyRequest(ResourceRequest<V> resourceRequest) {
        if(resourceRequest != null) {
            try {
                Exception e = new UnreachableStoreException("Resource request destroyed before resource checked out.");
                resourceRequest.handleException(e);
            } catch(Exception ex) {
                logger.error("Exception while destroying resource request:", ex);
            }
        }
    }

    /**
     * Destroys all resource requests in requestQueue.
     * 
     * @param requestQueue The queue for which all resource requests are to be
     *        destroyed.
     */
    private void destroyRequestQueue(Queue<ResourceRequest<V>> requestQueue) {
        ResourceRequest<V> resourceRequest = requestQueue.poll();
        while(resourceRequest != null) {
            destroyRequest(resourceRequest);
            resourceRequest = requestQueue.poll();
        }
    }

    @Override
    protected boolean internalClose() {
        // wasOpen ensures only one thread destroys everything.
        boolean wasOpen = super.internalClose();
        if(wasOpen) {
            for(Entry<K, Queue<ResourceRequest<V>>> entry: requestQueueMap.entrySet()) {
                Queue<ResourceRequest<V>> requestQueue = entry.getValue();
                destroyRequestQueue(requestQueue);
                requestQueueMap.remove(entry.getKey());
            }
        }
        return wasOpen;
    }

    /**
     * Close the queue and the pool.
     */
    @Override
    public void close() {
        internalClose();
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
        // TODO: The close method in the super class is not documented at all.
        // super.close(key) is called by ClientRequestExecutorPool.close which
        // is called by SocketStoreclientFactory. Given the super class does not
        // set any closed bit, unclear what the semantics of this.close(key)
        // ought to be.
        //
        // Also, super.close(key) does nothing to protect against multiple
        // threads accessing the method at the same time. And, super.close(key)
        // does not remove the affected pool from super.resourcePoolMap. The
        // semantics of super.close(key) are truly unclear.

        // Destroy enqueued resource requests (if any exist) first.
        Queue<ResourceRequest<V>> requestQueue = requestQueueMap.get(key);
        if(requestQueue != null) {
            destroyRequestQueue(requestQueue);
            // TODO: requestQueueMap.remove(entry.getKey()); ?
        }

        // Destroy resources in the pool second.
        super.close(key);
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

}
