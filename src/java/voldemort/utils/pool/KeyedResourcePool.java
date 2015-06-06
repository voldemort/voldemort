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

import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A simple implementation of a per-key resource pool. <br>
 * <ul>
 * <li>blocks if resource is not available.
 * <li>allocates resources in FIFO order
 * <li>Pools are per key and there is no global maximum pool limit.
 * </ul>
 *
 * Invariants that this implementation does not guarantee:
 * <ul>
 * <li>A checked in resource was previously checked out. (I.e., user can use
 * ResourceFactory and then check in a resource that this pool did not create.)
 * <li>A checked out resource is checked in at most once. (I.e., a user does not
 * call check in on a checked out resource more than once.)
 * <li>User no longer has a reference to a checked in resource. (I.e., user can
 * keep using the resource after it invokes check in.)
 * <li>A resource that is checked out is eventually either checked in or
 * destroyed via objectFactory.destroy(). (I.e., a user can squat on a resource
 * or let its reference to the resource lapse without checking the resource in
 * or destroying the resource.)
 * </ul>
 *
 * Phrased differently, the following is expected of the user of this class:
 * <ul>
 * <li>A checked out resource is checked in exactly once.
 * <li>A resource that is checked in was previously checked out.
 * <li>A resource that is checked in is never used again. / No reference is
 * retained to a checked in resource.
 * <li>Also, checkout is never called after close.
 * </ul>
 */
public class KeyedResourcePool<K, V> {

    private static final Logger logger = Logger.getLogger(KeyedResourcePool.class.getName());

    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final ResourceFactory<K, V> objectFactory;
    private final ResourcePoolConfig resourcePoolConfig;
    private final ConcurrentMap<K, Pool<V>> resourcePoolMap;
    private final AtomicInteger connectionsInProgress = new AtomicInteger(0);

    public KeyedResourcePool(ResourceFactory<K, V> objectFactory,
                             ResourcePoolConfig resourcePoolConfig) {
        this.objectFactory = Utils.notNull(objectFactory);
        this.resourcePoolConfig = Utils.notNull(resourcePoolConfig);
        this.resourcePoolMap = new ConcurrentHashMap<K, Pool<V>>();
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
     * @param key The key to checkout the resource for
     * @return The resource
     */
    public V checkout(K key) throws Exception {
        checkNotClosed();

        long startNs = System.nanoTime();
        long timeoutNs = resourcePoolConfig.getTimeout(TimeUnit.NANOSECONDS);
        long endNs = startNs + timeoutNs;
        Pool<V> resourcePool = getResourcePoolForKey(key);
        V resource = null;
        try {
            checkNotClosed();
            long totalNonBlockingElapsedNs = 0;
            long iterStartTime = 0;
            long totalBlockingElapsedNs = 0;
            final long MAX_WAIT_TIME = 300 * Time.NS_PER_MS;

            while(resource == null && (iterStartTime = System.nanoTime()) < endNs) {
                // Must attempt a non blocking checkout before blockingGet to
                // ensure resources are created for the pool.
                resource = attemptNonBlockingCheckout(key, resourcePool);

                if(resource != null)
                    break;

                // Non blocking operation is done, compute the non blocking time
                // it took in this iteration and add it to overall.
                long nonBlockingFinishTime = System.nanoTime();
                totalNonBlockingElapsedNs += (nonBlockingFinishTime - iterStartTime);
                long timeRemainingNs = endNs - nonBlockingFinishTime;

                long waitNs = timeRemainingNs;
                // If the pool is not at the maximum size, wait and then try to
                // grow the pool.
                if(resourcePool.size.get() < resourcePool.maxPoolSize) {
                    waitNs = Math.min(timeRemainingNs, MAX_WAIT_TIME);
                }

                if(waitNs > 0) {
                    resource = resourcePool.blockingGet(waitNs);
                    totalBlockingElapsedNs += (System.nanoTime() - nonBlockingFinishTime);
                }
            }

            if(resource == null) {
                String errorMessage = String.format("Timeout while checking out resource (%s). Configured time (%d) ns NonBlocking time (%d) ns Blocking time (%d) ns ",
                                                    key,
                                                    timeoutNs,
                                                    totalNonBlockingElapsedNs,
                                                    totalBlockingElapsedNs);
                throw new TimeoutException(errorMessage);
            }

            if(!objectFactory.validate(key, resource))
                throw new ExcessiveInvalidResourcesException(1);
        } catch(Exception e) {
            destroyResource(key, resourcePool, resource);
            throw e;
        }
        return resource;
    }

    /**
     * Get a free resource if one exists. If there are no free resources,
     * attempt to create a new resource (up to the max allowed for the pool).
     * This method does not block per se. However, creating a resource may be
     * (relatively) expensive. This method either returns null or a resource.
     *
     * This method is the only way in which new resources are created for the
     * pool. So, non blocking checkouts must be attempted to populate the
     * resource pool.
     */
    protected V attemptNonBlockingCheckout(K key, Pool<V> pool) throws Exception {
        V resource = null;
        // Each thread gets the connection from the existing pool. If no
        // connection is available, it requests one and waits for it to be
        // created. But a thread that requests the connection and before it
        // could wait, other thread could steal that connection. The
        // connectionsInProgress tries to avoid these edge cases by not
        // requesting a connection from the pool when other thread is
        // requesting one.
        if(connectionsInProgress.get() == 0) {
            resource = pool.nonBlockingGet();
        }
        if(resource == null) {
            connectionsInProgress.incrementAndGet();
            try {

                attemptGrow(key, this.objectFactory, pool);
                resource = pool.nonBlockingGet();
            } finally {
                connectionsInProgress.decrementAndGet();
            }
        }
        return resource;
    }

    /**
     * If there is room in the pool, attempt to to create a new resource and add
     * it to the pool. This method is cheap to call even if the pool is full
     * (i.e., the first thing it does is looks a the current size of the pool
     * relative to the max pool size.
     *
     * @param key
     * @param objectFactory
     * @return True if and only if a resource was successfully added to the
     *         pool.
     * @throws Exception if there are issues creating a new object, or
     *         destroying newly created object that could not be added to the
     *         pool.
     *
     */
    private boolean attemptGrow(K key, ResourceFactory<K, V> objectFactory, Pool<V> pool)
            throws Exception {
        if(pool.size.get() >= pool.maxPoolSize) {
            return false;
        }

        if(pool.size.incrementAndGet() <= pool.maxPoolSize) {
            try {
                objectFactory.createAsync(key, this);
            } catch(Exception e) {
                // If nonBlockingPut throws an exception, then we could leak
                // the resource created by objectFactory.create().
                pool.size.decrementAndGet();
                throw e;
            }
        } else {
            pool.size.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * Get the pool for the given key. If no pool exists, create one.
     */
    protected Pool<V> getResourcePoolForKey(K key) {
        Pool<V> resourcePool = resourcePoolMap.get(key);
        if(resourcePool == null) {
            Pool<V> newResourcePool = new Pool<V>(this.resourcePoolConfig);
            resourcePool = resourcePoolMap.putIfAbsent(key, newResourcePool);
            if(resourcePool == null) {
                resourcePool = newResourcePool;
            }
        }
        return resourcePool;
    }

    /**
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
     * A "safe" wrapper to destroy the given resource that catches any user
     * exceptions. This wrapper is safe in that it does not throw any
     * exceptions. However, this wrapper updates the size of the resource pool
     * and so should be called no more than once for any resource.
     */
    protected void destroyResource(K key, Pool<V> resourcePool, V resource) {
        if(resource != null) {
            try {
                objectFactory.destroy(key, resource);
            } catch(Exception e) {
                logger.error("Exception while destroying invalid resource: ", e);
            } finally {
                // Assumes destroyed resource was in fact checked out of the
                // pool. Also assumes that this method will be called no more
                // than once for any given checked out resource.
                resourcePool.size.decrementAndGet();
            }
        }
    }

    public void reportException(K key, Exception e) {
        Pool<V> resourcePool = getResourcePoolForKey(key);
        resourcePool.reportException(e);
    }

    /**
     * Check the given resource back into the pool
     *
     * @param key The key for the resource
     * @param resource The resource
     */
    public void checkin(K key, V resource) {
        if(isOpenAndValid(key, resource)) {
            Pool<V> resourcePool = getResourcePoolForExistingKey(key);
            boolean success = resourcePool.nonBlockingPut(resource);
            if(!success) {
                destroyResource(key, resourcePool, resource);
                throw new IllegalStateException("Checkin failed. Is the pool already full? (NB: see if KeyedResourcePool::destroyResource is being called multiple times.)");
            }
        }
    }

    protected boolean isOpenAndValid(K key, V resource) {
        if(isOpen.get() && objectFactory.validate(key, resource)) {
            return true;
        } else {
            Pool<V> resourcePool = getResourcePoolForExistingKey(key);
            destroyResource(key, resourcePool, resource);
            return false;
        }
    }

    protected boolean internalClose() {
        boolean wasOpen = isOpen.compareAndSet(true, false);
        // change state to false and allow one thread.
        if(wasOpen) {
            for(Entry<K, Pool<V>> entry: resourcePoolMap.entrySet()) {
                Pool<V> pool = entry.getValue();
                // destroy each resource in the queue
                List<V> values = pool.close();
                for(V value: values)
                    destroyResource(entry.getKey(), entry.getValue(), value);
                resourcePoolMap.remove(entry.getKey());
            }
        }
        return wasOpen;
    }

    /**
     * Close the pool. This will destroy all checked in resource immediately.
     * Once closed all attempts to checkout a new resource will fail. All
     * resources checked in after close is called will be immediately destroyed.
     */
    public void close() {
        internalClose();
    }

    /**
     * Reset a specific resource pool. Destroys all of the idle resources in the
     * pool. This method does not affect whether the pool is "open" in the sense
     * of permitting new resources to be added to it.
     *
     * @param key The key for the pool to reset.
     */
    public void reset(K key) {
        if(resourcePoolMap.containsKey(key)) {
            Pool<V> resourcePool = getResourcePoolForExistingKey(key);
            List<V> list = resourcePool.close();
            for(V value: list)
                destroyResource(key, resourcePool, value);
        }
    }

    /**
     * Count the number of existing resources for a specific pool.
     *
     * @param key The key
     * @return The count of existing resources. Returns 0 if no pool exists for
     *         given key.
     */
    public int getTotalResourceCount(K key) {
        if(resourcePoolMap.containsKey(key)) {
            try {
                Pool<V> resourcePool = getResourcePoolForExistingKey(key);
                return resourcePool.size.get();
            } catch(IllegalArgumentException iae) {
                if(logger.isDebugEnabled()) {
                    logger.debug("getTotalResourceCount called on invalid key: ", iae);
                }
            }
        }
        return 0;
    }

    /**
     * Count the total number of existing resources for all pools. The result is
     * "approximate" in the face of concurrency since individual pools can
     * change size during the aggregate count.
     *
     * @return The (approximate) aggregate count of existing resources.
     */
    public int getTotalResourceCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcePoolMap.entrySet())
            count += entry.getValue().size.get();
        return count;
    }

    /**
     * Count the number of checked in (idle) resources for a specific pool.
     *
     * @param key The key
     * @return The count of checked in resources. Returns 0 if no pool exists
     *         for given key.
     */
    public int getCheckedInResourcesCount(K key) {
        if(resourcePoolMap.containsKey(key)) {
            try {
                Pool<V> resourcePool = getResourcePoolForExistingKey(key);
                return resourcePool.queue.size();
            } catch(IllegalArgumentException iae) {
                if(logger.isDebugEnabled()) {
                    logger.debug("getCheckedInResourceCount called on invalid key: ", iae);
                }
            }
        }
        return 0;
    }

    /**
     * Count the total number of checked in (idle) resources across all pools.
     * The result is "approximate" in the face of concurrency since individual
     * pools can have resources checked in, or out, during the aggregate count.
     *
     * @return The (approximate) aggregate count of checked in resources.
     */
    public int getCheckedInResourceCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcePoolMap.entrySet())
            count += entry.getValue().queue.size();
        return count;
    }

    /**
     * Count the number of blocking gets for a specific key.
     *
     * @param key The key
     * @return The count of blocking gets. Returns 0 if no pool exists for given
     *         key.
     */
    public int getBlockingGetsCount(K key) {
        if(resourcePoolMap.containsKey(key)) {
            try {
                Pool<V> resourcePool = getResourcePoolForExistingKey(key);
                return resourcePool.blockingGets.get();
            } catch(IllegalArgumentException iae) {
                if(logger.isDebugEnabled()) {
                    logger.debug("getBlockingGetsCount called on invalid key: ", iae);
                }
            }
        }
        return 0;
    }

    /**
     * Count the total number of blocking gets across all pools. The result is
     * "approximate" in the face of concurrency since blocking gets for
     * individual pools can be issued or serviced during the aggregate count.
     *
     * @return The (approximate) aggregate count of blocking gets.
     */
    public int getBlockingGetsCount() {
        int count = 0;
        for(Entry<K, Pool<V>> entry: this.resourcePoolMap.entrySet())
            count += entry.getValue().blockingGets.get();
        return count;
    }

    /**
     * Check that the pool is not closed, and throw an IllegalStateException if
     * it is.
     */
    protected void checkNotClosed() {
        if(!isOpen.get())
            throw new IllegalStateException("Pool is closed!");
    }

    /**
     * A fixed size pool that uses an ArrayBlockingQueue. The pool grows to no
     * more than some specified maxPoolSize. The pool creates new resources in
     * the face of existing resources being destroyed.
     *
     */
    protected static class Pool<V> {

        final private AtomicInteger size = new AtomicInteger(0);
        final private AtomicInteger blockingGets = new AtomicInteger(0);
        final private int maxPoolSize;
        final private BlockingQueue<V> queue;
        private final BlockingQueue<Pair<Long, Exception>> asyncExceptions;

        private final long excpetionReportTimeMS;
        final int EXCEPTION_COUNT_MAX = 300;

        public Pool(ResourcePoolConfig resourcePoolConfig) {
            this.maxPoolSize = resourcePoolConfig.getMaxPoolSize();
            queue = new ArrayBlockingQueue<V>(this.maxPoolSize, resourcePoolConfig.isFair());
            this.asyncExceptions = new ArrayBlockingQueue<Pair<Long, Exception>>(EXCEPTION_COUNT_MAX);
            long configExceptionReportTime = resourcePoolConfig.getTimeout(TimeUnit.MILLISECONDS) * 2;
            long MAX_EXCEPTION_REPORT_TIME = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
            excpetionReportTimeMS = Math.min(configExceptionReportTime, MAX_EXCEPTION_REPORT_TIME);
        }

        public void reportException(Exception e) {
            asyncExceptions.offer(new Pair<Long, Exception>(System.currentTimeMillis(), e));
        }

        private void throwReportedExceptions() throws Exception {
            Pair<Long, Exception> entry;
            int skippedExceptionCount = 0;
            while(true) {
                entry = asyncExceptions.poll();
                if(entry == null) {
                    if(skippedExceptionCount > 0) {
                        logger.info(" All Exceptions were expired exceptions. Count "
                                    + skippedExceptionCount);
                    }
                    return;
                }

                long elapsedTime = System.currentTimeMillis() - entry.getFirst();
                skippedExceptionCount++;
                if(elapsedTime <= excpetionReportTimeMS) {
                    Exception e = entry.getSecond();
                    logger.info(" Throwing remembered exception. time elapsed (ms) " + elapsedTime
                                + ". Exception : "
                                + e.getMessage());
                    throw e;
                }
            }
        }

        public V nonBlockingGet() throws Exception {
            throwReportedExceptions();
            return this.queue.poll();
        }

        public V blockingGet(long timeoutNs) throws Exception {
            throwReportedExceptions();
            V v;
            try {
                blockingGets.incrementAndGet();
                v = this.queue.poll(timeoutNs, TimeUnit.NANOSECONDS);
            } finally {
                blockingGets.decrementAndGet();
            }

            if(v == null) {
                throwReportedExceptions();
            }
            return v;
        }

        public boolean nonBlockingPut(V v) {
            return this.queue.offer(v);
        }

        public List<V> close() {
            asyncExceptions.clear();
            List<V> list = new ArrayList<V>();
            queue.drainTo(list);
            return list;
        }

    }
}
