/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.stats;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.utils.JmxUtils;
import voldemort.utils.pool.QueuedKeyedResourcePool;

/**
 * Some convenient statistics to track about the client requests
 * 
 * 
 */
// TODO: This approach to stats tracking seems scary. All of the getter methods
// query current counters/histograms that are being updated. If you happen to
// use a getter soon after the monitoringInterval has rolled over, then your
// answer is likely statistically insignificant and potentially totally whacked
// out (not a technical term, sorry). Either of the following approaches seem
// like an improvement to me:
//
// (1) Effectively have two copies of all stats tracking "current" and "prev".
// All the getters would access "last". This means the responses are
// statistically meaningful, but potentially stale. reset() would copy
// "current" to "prev" and then resets "current".
//
// (2) A more general variant of (1) is to have n copies of all stats tracking.
// The getters would aggregated over all n copies of stats tracking. This
// provides a "sliding" window of statistically valid responses. reset() would
// create a new stats tracking object and delete the oldest stats trackig
// object.
public class ClientSocketStats {

    private final ClientSocketStats parent;
    private final ConcurrentMap<SocketDestination, ClientSocketStats> statsMap;
    private final SocketDestination destination;
    private QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;

    // monitoringInterval <= connectionCheckouts + resourceRequests
    // 1 qps => monitoring interval of just over a day (~27 hours)
    // 1000 qps => monitoring interval of 1 minute and 40 seconds
    private final AtomicInteger monitoringInterval = new AtomicInteger(100000);
    // Connection lifecycle
    private final AtomicInteger connectionsCreated = new AtomicInteger(0);
    private final AtomicInteger connectionsDestroyed = new AtomicInteger(0);
    // "Sync checkouts" / KeyedResourcePool::checkout
    private final Histogram checkoutTimeUsHistogram = new Histogram(20000, 100);
    private final AtomicLong totalCheckoutTimeUs = new AtomicLong(0);
    private final AtomicInteger checkoutCount = new AtomicInteger(0);
    private final Histogram checkoutQueueLengthHistogram = new Histogram(250, 1);
    // "Async checkouts" / QueuedKeyedResourcePool::registerResourceRequest
    private final Histogram resourceRequestTimeUsHistogram = new Histogram(20000, 100);
    private final AtomicLong totalResourceRequestTimeUs = new AtomicLong(0);
    private final AtomicInteger resourceRequestCount = new AtomicInteger(0);
    private final Histogram resourceRequestQueueLengthHistogram = new Histogram(250, 1);

    private final int jmxId;
    private static final Logger logger = Logger.getLogger(ClientSocketStats.class.getName());

    /**
     * To construct a per node stats object
     * 
     * @param parent An optional parent stats object that will maintain
     *        aggregate data across many sockets
     * @param destination The destination object that defines the node
     * @param pool The socket pool that will give out connection information
     */
    public ClientSocketStats(ClientSocketStats parent,
                             SocketDestination destination,
                             QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor> pool,
                             int jmxId) {
        this.parent = parent;
        this.statsMap = null;
        this.destination = destination;
        this.pool = pool;
        this.jmxId = jmxId;

        if(logger.isDebugEnabled()) {
            logger.debug("Constructed ClientSocketStatsStats object ("
                         + System.identityHashCode(this) + ") with parent object("
                         + System.identityHashCode(parent) + ")");
        }
    }

    /**
     * Construction of a new aggregate stats object
     * 
     * @param pool The socket pool that will give out connection information
     */
    public ClientSocketStats(int jmxId) {
        this.parent = null;
        this.statsMap = new ConcurrentHashMap<SocketDestination, ClientSocketStats>();
        this.destination = null;
        this.pool = null;
        this.jmxId = jmxId;

        if(logger.isDebugEnabled()) {
            logger.debug("Constructed ClientSocketStatsStats object ("
                         + System.identityHashCode(this) + ") with parent object("
                         + System.identityHashCode(parent) + ")");
        }
    }

    /* get per node stats, create one if not exist */
    private ClientSocketStats getOrCreateNodeStats(SocketDestination destination) {
        if(destination == null) {
            return null;
        }
        ClientSocketStats stats = statsMap.get(destination);
        if(stats == null) {
            stats = new ClientSocketStats(this, destination, pool, jmxId);
            statsMap.putIfAbsent(destination, stats);
            stats = statsMap.get(destination);
            JmxUtils.registerMbean(new ClientSocketStatsJmx(stats),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(ClientRequestExecutor.class),
                                                             "stats_"
                                                                     + destination.toString()
                                                                                  .replace(':', '_')
                                                                     + JmxUtils.getJmxId(jmxId)));
        }
        return stats;
    }

    /**
     * Record the checkout wait time in us
     * 
     * @param dest Destination of the socket to checkout. Will actually record
     *        if null. Otherwise will call this on self and corresponding child
     *        with this param null.
     * @param checkoutTimeUs The number of us to wait before getting a socket
     */
    public void recordCheckoutTimeUs(SocketDestination dest, long checkoutTimeUs) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordCheckoutTimeUs(null, checkoutTimeUs);
            recordCheckoutTimeUs(null, checkoutTimeUs);
        } else {
            this.totalCheckoutTimeUs.getAndAdd(checkoutTimeUs);
            this.checkoutTimeUsHistogram.insert(checkoutTimeUs);
            this.checkoutCount.getAndIncrement();

            checkMonitoringInterval();
        }
    }

    /**
     * Record the checkout queue length
     * 
     * @param dest Destination of the socket to checkout. Will actually record
     *        if null. Otherwise will call this on self and corresponding child
     *        with this param null.
     * @param queueLength The number of entries in the "synchronous" checkout
     *        queue.
     */
    public void recordCheckoutQueueLength(SocketDestination dest, int queueLength) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordCheckoutQueueLength(null, queueLength);
            recordCheckoutQueueLength(null, queueLength);
        } else {
            this.checkoutQueueLengthHistogram.insert(queueLength);
        }
    }

    /**
     * Record the resource request wait time in us
     * 
     * @param dest Destination of the socket for which the resource was
     *        requested. Will actually record if null. Otherwise will call this
     *        on self and corresponding child with this param null.
     * @param resourceRequestTimeUs The number of us to wait before getting a
     *        socket
     */
    public void recordResourceRequestTimeUs(SocketDestination dest, long resourceRequestTimeUs) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordResourceRequestTimeUs(null, resourceRequestTimeUs);
            recordResourceRequestTimeUs(null, resourceRequestTimeUs);
        } else {
            this.totalResourceRequestTimeUs.getAndAdd(resourceRequestTimeUs);
            this.resourceRequestTimeUsHistogram.insert(resourceRequestTimeUs);
            this.resourceRequestCount.getAndIncrement();

            checkMonitoringInterval();
        }
    }

    /**
     * Record the resource request queue length
     * 
     * @param dest Destination of the socket for which resource request is
     *        enqueued. Will actually record if null. Otherwise will call this
     *        on self and corresponding child with this param null.
     * @param queueLength The number of entries in the "asynchronous" resource
     *        request queue.
     */
    public void recordResourceRequestQueueLength(SocketDestination dest, int queueLength) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordResourceRequestQueueLength(null, queueLength);
            recordResourceRequestQueueLength(null, queueLength);
        } else {
            this.resourceRequestQueueLengthHistogram.insert(queueLength);
        }
    }

    public void connectionCreate(SocketDestination dest) {
        if(dest != null) {
            getOrCreateNodeStats(dest).connectionCreate(null);
            connectionCreate(null);
        } else {
            this.connectionsCreated.getAndIncrement();
        }
    }

    public void connectionDestroy(SocketDestination dest) {
        if(dest != null) {
            getOrCreateNodeStats(dest).connectionDestroy(null);
            connectionDestroy(null);
        } else {
            this.connectionsDestroyed.getAndIncrement();
        }
    }

    // Getters for connection life cycle stats

    public int getConnectionsCreated() {
        return connectionsCreated.intValue();
    }

    public int getConnectionsDestroyed() {
        return connectionsDestroyed.intValue();
    }

    // Getters for checkout stats

    public int getCheckoutCount() {
        return checkoutCount.intValue();
    }

    public Histogram getCheckoutWaitUsHistogram() {
        return this.checkoutTimeUsHistogram;
    }

    /**
     * @return 0 if there have been no checkout invocations
     */
    public long getAvgCheckoutWaitUs() {
        long count = checkoutCount.get();
        if(count > 0)
            return totalCheckoutTimeUs.get() / count;
        return 0;
    }

    public Histogram getCheckoutQueueLengthHistogram() {
        return this.checkoutQueueLengthHistogram;
    }

    // Getters for resourceRequest stats

    public int resourceRequestCount() {
        return resourceRequestCount.intValue();
    }

    public Histogram getResourceRequestWaitUsHistogram() {
        return this.resourceRequestTimeUsHistogram;
    }

    /**
     * @return 0 if there have been no resourceRequest invocations
     */
    public long getAvgResourceRequestWaitUs() {
        long count = resourceRequestCount.get();
        if(count > 0)
            return totalResourceRequestTimeUs.get() / count;
        return 0;
    }

    public Histogram getResourceRequestQueueLengthHistogram() {
        return this.resourceRequestQueueLengthHistogram;
    }

    // Getters for (queued)pool stats
    public int getConnectionsActive(SocketDestination destination) {
        if(destination == null) {
            return pool.getTotalResourceCount();
        } else {
            return pool.getTotalResourceCount(destination);
        }
    }

    public int getConnectionsInPool(SocketDestination destination) {
        if(destination == null) {
            return pool.getCheckedInResourceCount();
        } else {
            return pool.getCheckedInResourcesCount(destination);
        }
    }

    // Config & administrivia interfaces

    public void setMonitoringInterval(int count) {
        this.monitoringInterval.set(count);
    }

    public int getMonitoringInterval() {
        return this.monitoringInterval.get();
    }

    protected void checkMonitoringInterval() {
        int monitoringCount = this.checkoutCount.get() + this.resourceRequestCount.get();

        // reset aggregated stats and all the node stats for new interval
        if(parent == null && statsMap != null) {
            int monitoringInterval = this.monitoringInterval.get();
            if(monitoringCount % (monitoringInterval + 1) == monitoringInterval) {
                // timing instrumentation (debug only)
                long startTimeNs = 0;
                if(logger.isDebugEnabled()) {
                    startTimeNs = System.nanoTime();
                }

                // reset all children
                Iterator<SocketDestination> it = statsMap.keySet().iterator();
                while(it.hasNext()) {
                    ClientSocketStats stats = statsMap.get(it.next());
                    stats.resetForInterval();
                }
                // reset itself
                resetForInterval();

                // timing instrumentation (debug only)
                if(logger.isDebugEnabled()) {
                    logger.debug("ClientSocketStats(" + System.identityHashCode(this)
                                 + ")::checkMonitoringInterval: reset self and all children in "
                                 + (System.nanoTime() - startTimeNs) + " ns.");
                }
            }
        }
    }

    /**
     * Reset all of the stats counters
     */
    protected void resetForInterval() {
        // harmless race conditions amongst all of this counter resetting:
        this.totalCheckoutTimeUs.set(0);
        this.checkoutCount.set(0);
        this.checkoutTimeUsHistogram.reset();
        this.checkoutQueueLengthHistogram.reset();

        this.totalResourceRequestTimeUs.set(0);
        this.resourceRequestCount.set(0);
        this.resourceRequestTimeUsHistogram.reset();
        this.resourceRequestQueueLengthHistogram.reset();
    }

    public void setPool(QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor> pool) {
        this.pool = pool;
    }

    public ConcurrentMap<SocketDestination, ClientSocketStats> getStatsMap() {
        return statsMap;
    }

    SocketDestination getDestination() {
        return destination;
    }

    /**
     * Unregister all MBeans
     */
    public void close() {
        Iterator<SocketDestination> it = getStatsMap().keySet().iterator();
        while(it.hasNext()) {
            try {
                SocketDestination destination = it.next();
                JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(ClientRequestExecutor.class),
                                                                   "stats_"
                                                                           + destination.toString()
                                                                                        .replace(':',
                                                                                                 '_')
                                                                           + JmxUtils.getJmxId(jmxId)));
            } catch(Exception e) {}
        }
    }
}
