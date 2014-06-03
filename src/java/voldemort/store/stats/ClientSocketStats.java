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

import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.utils.JmxUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
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

    // "Sync checkouts" / KeyedResourcePool::checkout
    private final RequestCounter checkoutTimeRequestCounter = new RequestCounter(60000, true);
    // "Async checkouts" / QueuedKeyedResourcePool::registerResourceRequest
    private final RequestCounter resourceRequestTimeRequestCounter = new RequestCounter(60000, true);
    // Connection establishment time. The counter will be reset every 60 seconds
    private final RequestCounter connectionEstablishmentRequestCounter = new RequestCounter(60000,
                                                                                            true);

    // Sync operation time stats. The counter will be reset every 60 seconds
    private final RequestCounter syncOpTimeRequestCounter = new RequestCounter(60000, true);
    // Async operation time stats. The counter will be reset every 60 seconds
    private final RequestCounter asynOpTimeRequestCounter = new RequestCounter(60000, true);

    // The histograms will be reset after monitoringInterval
    private final AtomicInteger monitoringInterval = new AtomicInteger(60000);
    private long startMs;
    private final Histogram checkoutQueueLengthHistogram = new Histogram(250, 1);
    private final Histogram resourceRequestQueueLengthHistogram = new Histogram(250, 1);

    private final String identifierString;
    private static final Logger logger = Logger.getLogger(ClientSocketStats.class.getName());

    private final Map<Tracked, AtomicInteger> counters;

    public static enum Tracked {
        CONNECTION_CREATED_EVENT("connectionCreated"),
        CONNECTION_DESTROYED_EVENT("connectionDestroyed"),
        CONNECTION_EXCEPTION_EVENT("connectionException");

        private final String name;

        private Tracked(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * To construct a per node stats object
     * 
     * @param parent An optional parent stats object that will maintain
     *        aggregate data across many sockets
     * @param destination The destination object that defines the node
     * @param pool The socket pool that will give out connection information
     * @param identifierString The string of identifier
     */
    public ClientSocketStats(ClientSocketStats parent,
                             SocketDestination destination,
                             QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor> pool,
                             String identifierString) {
        this.parent = parent;
        this.statsMap = null;
        this.destination = destination;
        this.pool = pool;
        this.identifierString = identifierString;
        this.startMs = SystemTime.INSTANCE.getMilliseconds();
        counters = new EnumMap<Tracked, AtomicInteger>(Tracked.class);
        for(Tracked tracked: Tracked.values()) {
            counters.put(tracked, new AtomicInteger(0));
        }

        if(logger.isDebugEnabled()) {
            logger.debug("Constructed ClientSocketStatsStats object ("
                         + System.identityHashCode(this) + ") with parent object("
                         + System.identityHashCode(parent) + ")");
        }
    }

    /**
     * Construction of a new aggregate stats object
     * 
     * @param identifierString The string of identifier
     */
    public ClientSocketStats(String identifierString) {
        this.parent = null;
        this.statsMap = new ConcurrentHashMap<SocketDestination, ClientSocketStats>();
        this.destination = null;
        this.pool = null;
        this.identifierString = identifierString;
        this.startMs = SystemTime.INSTANCE.getMilliseconds();
        counters = new EnumMap<Tracked, AtomicInteger>(Tracked.class);
        for(Tracked tracked: Tracked.values()) {
            counters.put(tracked, new AtomicInteger(0));
        }

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
            ClientSocketStats socketStats = new ClientSocketStats(this,
                                                                  destination,
                                                                  pool,
                                                                  identifierString);
            // The idea here is to avoid registering the bean multiple times.
            // This can happen when
            // two threads, both read the stats object as null (three lines
            // above) and then try to
            // populate the map with their own socketStats object. putifabsent
            // returns the existing
            // value if the key exists or returns null. The thread that does the
            // first put will
            // get a null return vale. That thread will then go on to get the
            // stats object and register
            // the bean. All the other threads will have their stats object
            // populated by the return
            // value of the putIfAbsent call that will return the existing value
            // in the map.
            stats = statsMap.putIfAbsent(destination, socketStats);
            if(stats == null) {
                stats = socketStats;
                JmxUtils.registerMbean(new ClientSocketStatsJmx(stats),
                                       JmxUtils.createObjectName(JmxUtils.getPackageName(ClientRequestExecutor.class),
                                                                 "stats_"
                                                                         + destination.toString()
                                                                                      .replace(':',
                                                                                               '_')
                                                                         + identifierString));
            }
        }
        return stats;
    }

    /**
     * Record operation for sync ops time
     * 
     * @param dest Destination of the socket to connect to. Will actually record
     *        if null. Otherwise will call this on self and corresponding child
     *        with this param null.
     * @param opTimeUs The number of us for the op to finish
     */
    public void recordSyncOpTimeNs(SocketDestination dest, long opTimeNs) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordSyncOpTimeNs(null, opTimeNs);
            recordSyncOpTimeNs(null, opTimeNs);
        } else {
            this.syncOpTimeRequestCounter.addRequest(opTimeNs);
        }
    }

    /**
     * Record operation for async ops time
     * 
     * @param dest Destination of the socket to connect to. Will actually record
     *        if null. Otherwise will call this on self and corresponding child
     *        with this param null.
     * @param opTimeUs The number of us for the op to finish
     */
    public void recordAsyncOpTimeNs(SocketDestination dest, long opTimeNs) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordAsyncOpTimeNs(null, opTimeNs);
            recordAsyncOpTimeNs(null, opTimeNs);
        } else {
            this.asynOpTimeRequestCounter.addRequest(opTimeNs);
        }
    }

    /**
     * Record the connection establishment time
     * 
     * @param dest Destination of the socket to connect to. Will actually record
     *        if null. Otherwise will call this on self and corresponding child
     *        with this param null.
     * @param connEstTimeUs The number of us to wait before establishing a
     *        connection
     */
    public void recordConnectionEstablishmentTimeUs(SocketDestination dest, long connEstTimeUs) {
        if(dest != null) {
            getOrCreateNodeStats(dest).recordConnectionEstablishmentTimeUs(null, connEstTimeUs);
            recordConnectionEstablishmentTimeUs(null, connEstTimeUs);
        } else {
            this.connectionEstablishmentRequestCounter.addRequest(connEstTimeUs * Time.NS_PER_US);
        }
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
            this.checkoutTimeRequestCounter.addRequest(checkoutTimeUs * Time.NS_PER_US);
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
            checkMonitoringInterval();
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
            this.resourceRequestTimeRequestCounter.addRequest(resourceRequestTimeUs
                                                              * Time.NS_PER_US);
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
            checkMonitoringInterval();
        }
    }

    public void incrementCount(SocketDestination dest, Tracked metric) {
        if(dest != null) {
            getOrCreateNodeStats(dest).incrementCount(null, metric);
            incrementCount(null, metric);
        } else {
            this.counters.get(metric).getAndIncrement();
        }
    }

    public int getCount(Tracked metric) {
        return counters.get(metric).get();
    }

    // Getters for checkout stats
    public int getCheckoutCount() {
        return (int) checkoutTimeRequestCounter.getCount();
    }

    public double getAvgCheckoutWaitMs() {
        return checkoutTimeRequestCounter.getAverageTimeInMs();
    }

    public double getCheckoutTimeMsQ10th() {
        return checkoutTimeRequestCounter.getQ10LatencyMs();
    }

    public double getCheckoutTimeMsQ50th() {
        return checkoutTimeRequestCounter.getQ50LatencyMs();
    }

    public double getCheckoutTimeMsQ99th() {
        return checkoutTimeRequestCounter.getQ99LatencyMs();
    }

    public Histogram getCheckoutQueueLengthHistogram() {
        checkMonitoringInterval();
        return this.checkoutQueueLengthHistogram;
    }

    // Getters for resourceRequest stats
    public int resourceRequestCount() {
        return (int) resourceRequestTimeRequestCounter.getCount();
    }

    /**
     * @return 0 if there have been no resourceRequest invocations
     */
    public double getAvgResourceRequestTimeMs() {
        return resourceRequestTimeRequestCounter.getAverageTimeInMs();
    }

    public double getResourceRequestTimeMsQ10th() {
        return resourceRequestTimeRequestCounter.getQ10LatencyMs();
    }

    public double getResourceRequestTimeMsQ50th() {
        return resourceRequestTimeRequestCounter.getQ50LatencyMs();
    }

    public double getResourceRequestTimeMsQ99th() {
        return resourceRequestTimeRequestCounter.getQ99LatencyMs();
    }

    public Histogram getResourceRequestQueueLengthHistogram() {
        checkMonitoringInterval();
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

    // Getters for connection establishment stats
    public double getAvgConnectionEstablishmentMs() {
        return this.connectionEstablishmentRequestCounter.getAverageTimeInMs();
    }

    public double getConnectionEstablishmentQ99LatencyMs() {
        return this.connectionEstablishmentRequestCounter.getQ99LatencyMs();
    }

    // Getters for sync op time
    public double getSyncOpTimeMsAvg() {
        return this.syncOpTimeRequestCounter.getAverageTimeInMs();
    }

    public double getSyncOpTimeMsQ95th() {
        return this.syncOpTimeRequestCounter.getQ95LatencyMs();
    }

    public double getSyncOpTimeMsQ99th() {
        return this.syncOpTimeRequestCounter.getQ99LatencyMs();
    }

    // Getters for async op time
    public double getAsyncOpTimeMsAvg() {
        return this.asynOpTimeRequestCounter.getAverageTimeInMs();
    }

    public double getAsyncOpTimeMsQ95th() {
        return this.asynOpTimeRequestCounter.getQ95LatencyMs();
    }

    public double getAsyncOpTimeMsQ99th() {
        return this.asynOpTimeRequestCounter.getQ99LatencyMs();
    }

    // Config & administrivia interfaces

    public void setMonitoringInterval(int count) {
        this.monitoringInterval.set(count);
    }

    public int getMonitoringInterval() {
        return this.monitoringInterval.get();
    }

    protected void checkMonitoringInterval() {
        int durationMs = this.monitoringInterval.get();
        // reset aggregated stats and all the node stats for new interval
        if(parent == null && statsMap != null) {
            long now = SystemTime.INSTANCE.getMilliseconds();
            if(now - this.startMs > durationMs) {
                // reset all children
                Iterator<SocketDestination> it = statsMap.keySet().iterator();
                while(it.hasNext()) {
                    ClientSocketStats stats = statsMap.get(it.next());
                    stats.resetForInterval();
                }
                // reset itself
                resetForInterval();
            }
        }
    }

    /**
     * Reset all of the stats counters
     */
    protected void resetForInterval() {
        // harmless race conditions amongst all of this counter resetting:
        this.checkoutQueueLengthHistogram.reset();
        this.resourceRequestQueueLengthHistogram.reset();
        this.startMs = SystemTime.INSTANCE.getMilliseconds();
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
                                                                           + identifierString));
            } catch(Exception e) {}
        }
    }
}
