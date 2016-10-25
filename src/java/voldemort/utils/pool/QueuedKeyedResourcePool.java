/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
 * <li>Allocates resources in FIFOish order: blocking requests via checkout are
 * FIFO and non-blocking enqueued requests are FIFO, however, there is no
 * ordering between blocking (checkout) and non-blocking (requestResource).
 * <li>Pools and Queues are per key and there is no global maximum pool or queue
 * limit.
 * </ul>
 * 
 * Beyond the expectations documented in KeyedResourcePool, the following is
 * expected of the user of this class:
 * <ul>
 * <li>A resource acquired via #checkout(K)) or via requestResource(K ,
 * ResourceRequest) requestResource is checked in exactly once.
 * <li>A resource that is checked in was previously checked out or requested.
 * <li>Also, requestResource is never called after close.
 * </ul>
 */
public class QueuedKeyedResourcePool<K, V> extends KeyedResourcePool<K, V> {

    private static final Logger logger = Logger.getLogger(QueuedKeyedResourcePool.class.getName());

    private final ConcurrentMap<K, Queue<AsyncResourceRequest<V>>> requestQueueMap;

    public QueuedKeyedResourcePool(ResourceFactory<K, V> objectFactory, ResourcePoolConfig config) {
        super(objectFactory, config);
        requestQueueMap = new ConcurrentHashMap<K, Queue<AsyncResourceRequest<V>>>();
    }

    /**
     * Create a new queued pool with key type K, request type R, and value type
     * V.
     * 
     * @param factory The factory that creates objects
     * @param config The pool config
     * @return The created pool
     */
    public static <K, V> QueuedKeyedResourcePool<K, V> create(ResourceFactory<K, V> factory,
                                                              ResourcePoolConfig config) {
        return new QueuedKeyedResourcePool<K, V>(factory, config);
    }

    /**
     * Create a new queued pool using the defaults for key of type K, request of
     * type R, and value of Type V.
     * 
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
     */
    public void registerResourceRequest(K key, AsyncResourceRequest<V> resourceRequest) {
        checkNotClosed();

        Queue<AsyncResourceRequest<V>> requestQueue = getRequestQueueForKey(key);
        if(requestQueue.isEmpty()) {
            // Attempt non-blocking checkout iff requestQueue is empty.

            Pool<V> resourcePool = getResourcePoolForKey(key);
            V resource = null;
            try {
                resource = attemptNonBlockingCheckout(key, resourcePool);
            } catch(Exception e) {
                destroyResource(key, resourcePool, resource);
                resource = null;
                resourceRequest.handleException(e);
                return;
            }
            if(resource != null) {
                resourceRequest.useResource(resource);
                return;
            }
        }

        requestQueue.add(resourceRequest);
        // Guard against (potential) races with checkin by invoking
        // processQueueLoop after resource request has been added to the
        // asynchronous queue.
        processQueueLoop(key);
    }

    /**
     * Used only for unit testing. Please do not use this method in other ways.
     * 
     * @param key
     * @return
     * @throws Exception
     */
    public V internalNonBlockingGet(K key) throws Exception {
        Pool<V> resourcePool = getResourcePoolForKey(key);
        return attemptNonBlockingCheckout(key, resourcePool);
    }

    /**
     * Pops resource requests off the queue until queue is empty or an unexpired
     * resource request is found. Invokes .handleTimeout on all expired resource
     * requests popped off the queue.
     * 
     * @return null or a valid ResourceRequest
     */
    private AsyncResourceRequest<V> getNextUnexpiredResourceRequest(Queue<AsyncResourceRequest<V>> requestQueue) {
        AsyncResourceRequest<V> resourceRequest = requestQueue.poll();
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
    private boolean processQueue(K key) {
        Queue<AsyncResourceRequest<V>> requestQueue = getRequestQueueForKey(key);
        if(requestQueue.isEmpty()) {
            return false;
        }

        // Attempt to get a resource.
        Pool<V> resourcePool = getResourcePoolForKey(key);
        V resource = null;
        Exception ex = null;
        try {
            // Must attempt non-blocking checkout to ensure resources are
            // created for the pool.
            resource = attemptNonBlockingCheckout(key, resourcePool);
        } catch(Exception e) {
            destroyResource(key, resourcePool, resource);
            ex = e;
            resource = null;
        }
        // Neither we got a resource, nor an exception. So no requests can be
        // processed return
        if(resource == null && ex == null) {
            return false;
        }

        // With resource in hand, process the resource requests
        AsyncResourceRequest<V> resourceRequest = getNextUnexpiredResourceRequest(requestQueue);
        if(resourceRequest == null) {
            if(resource != null) {
                // Did not use the resource! Directly check in via super to
                // avoid
                // circular call to processQueue().
                try {
                    super.checkin(key, resource);
                } catch(Exception e) {
                    logger.error("Exception checking in resource: ", e);
                }
            } else {
                // Poor exception, no request to tag this exception onto
                // drop it on the floor and continue as usual.
            }
            return false;
        } else {
            // We have a request here.
            if(resource != null) {
                resourceRequest.useResource(resource);
            } else {
                resourceRequest.handleException(ex);
            }
            return true;
        }
    }

    /**
     * TODO: The processQueueLoop is typically invoked from the selector (
     * serial could invoke this as well, but most likely Parallel (Selector
     * returning connection to the pool )is invoking it). When parallel requests
     * does not have connections, they enqueue the requests. The next thread to
     * check in will continue to process these requests, until the queue is
     * drained or connection is exhausted. There is no number bound on this. For
     * example If you bump the ExceededQuotaSlopTest to do more than 500
     * requests it will fail and if you put a bound on this it will pass.
     * Something that requires deeper investigation in the future.
     * 
     * Attempts to repeatedly process enqueued resource requests. Tries until no
     * more progress is possible without blocking.
     * 
     * @param key
     */
    private void processQueueLoop(K key) {
        while(processQueue(key)) {}
    }

    @Override
    public void reportException(K key, Exception e) {
        super.reportException(key, e);
        processQueueLoop(key);
    }

    /**
     * Check the given resource back into the pool
     * 
     * @param key The key for the resource
     * @param resource The resource
     */
    @Override
    public void checkin(K key, V resource) {
        super.checkin(key, resource);
        // NB: Blocking checkout calls for synchronous requests get the resource
        // checked in above before processQueueLoop() attempts checkout below.
        // There is therefore a risk that asynchronous requests will be starved.
        processQueueLoop(key);
    }

    /**
     * A safe wrapper to destroy the given resource request.
     */
    protected void destroyRequest(AsyncResourceRequest<V> resourceRequest) {
        if(resourceRequest != null) {
            try {
                // To hand control back to the owner of the
                // AsyncResourceRequest, treat "destroy" as an exception since
                // there is no resource to pass into useResource, and the
                // timeout has not expired.
                Exception e = new UnreachableStoreException("Client request was terminated while waiting in the queue.");
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
    private void destroyRequestQueue(Queue<AsyncResourceRequest<V>> requestQueue) {
        if(requestQueue != null) {
            AsyncResourceRequest<V> resourceRequest = requestQueue.poll();
            while(resourceRequest != null) {
                destroyRequest(resourceRequest);
                resourceRequest = requestQueue.poll();
            }
        }
    }

    @Override
    protected boolean internalClose() {
        // wasOpen ensures only one thread destroys everything.
        boolean wasOpen = super.internalClose();
        if(wasOpen) {
            for(Entry<K, Queue<AsyncResourceRequest<V>>> entry: requestQueueMap.entrySet()) {
                Queue<AsyncResourceRequest<V>> requestQueue = entry.getValue();
                destroyRequestQueue(requestQueue);
                requestQueueMap.remove(entry.getKey());
            }
        }
        return wasOpen;
    }

    @Override
    public void reset(K key) {
        try {
            Queue<AsyncResourceRequest<V>> requestQueue = getRequestQueueForExistingKey(key);
            if(requestQueue != null) {
                destroyRequestQueue(requestQueue);
            }
        } finally {
            super.reset(key);
        }
    }

    /**
     * Close the queue and the pool.
     */
    @Override
    public void close() {
        internalClose();
    }

    /*
     * Get the queue of work for the given key. If no queue exists, create one.
     */
    protected Queue<AsyncResourceRequest<V>> getRequestQueueForKey(K key) {
        Queue<AsyncResourceRequest<V>> requestQueue = requestQueueMap.get(key);
        if(requestQueue == null) {
            Queue<AsyncResourceRequest<V>> newRequestQueue = new ConcurrentLinkedQueue<AsyncResourceRequest<V>>();
            requestQueue = requestQueueMap.putIfAbsent(key, newRequestQueue);
            if(requestQueue == null) {
                requestQueue = newRequestQueue;
            }
        }
        return requestQueue;
    }

    /*
     * Get the pool for the given key. If no pool exists, returns null.
     */
    protected Queue<AsyncResourceRequest<V>> getRequestQueueForExistingKey(K key) {
        Queue<AsyncResourceRequest<V>> requestQueue = requestQueueMap.get(key);
        return requestQueue;
    }

    /**
     * Count the number of queued resource requests for a specific pool.
     * 
     * @param key The key
     * @return The count of queued resource requests. Returns 0 if no queue
     *         exists for given key.
     */
    public int getRegisteredResourceRequestCount(K key) {
        if(requestQueueMap.containsKey(key)) {
            Queue<AsyncResourceRequest<V>> requestQueue = getRequestQueueForExistingKey(key);
            // FYI: .size() is not constant time in the next call. ;)
            if(requestQueue != null) {
                return requestQueue.size();
            }
        }
        return 0;
    }

    /**
     * Count the total number of queued resource requests for all queues. The
     * result is "approximate" in the face of concurrency since individual
     * queues can change size during the aggregate count.
     * 
     * @return The (approximate) aggregate count of queued resource requests.
     */
    public int getRegisteredResourceRequestCount() {
        int count = 0;
        for(Entry<K, Queue<AsyncResourceRequest<V>>> entry: this.requestQueueMap.entrySet()) {
            // FYI: .size() is not constant time in the next call. ;)
            count += entry.getValue().size();
        }
        return count;
    }
}
