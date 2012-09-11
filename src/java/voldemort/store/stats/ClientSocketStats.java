/*
 * Copyright 2008-2011 LinkedIn, Inc
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

import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.utils.JmxUtils;
import voldemort.utils.pool.KeyedResourcePool;

/**
 * Some convenient statistics to track about the client requests
 * 
 * 
 */
public class ClientSocketStats {

    private final ClientSocketStats parent;
    private final ConcurrentMap<SocketDestination, ClientSocketStats> statsMap;
    private final SocketDestination destination;
    private KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;

    private final AtomicInteger monitoringInterval = new AtomicInteger(10000);
    private final Histogram checkoutTimeUsHistogram = new Histogram(20000, 100);
    private final AtomicLong totalCheckoutTimeUs = new AtomicLong(0);
    private final AtomicLong avgCheckoutTimeUs = new AtomicLong(0);
    private final AtomicInteger connectionsCreated = new AtomicInteger(0);
    private final AtomicInteger connectionsDestroyed = new AtomicInteger(0);
    private final AtomicInteger connectionsCheckedout = new AtomicInteger(0);

    private final int jmxId;

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
                             KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool,
                             int jmxId) {
        this.parent = parent;
        this.statsMap = null;
        this.destination = destination;
        this.pool = pool;
        this.jmxId = jmxId;
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
            checkoutTimeUsHistogram.insert(checkoutTimeUs);
            int checkouts = this.connectionsCheckedout.getAndIncrement();

            // reset aggregated stats and all the node stats for new interval
            if(parent == null && statsMap != null) {
                int interval = this.monitoringInterval.get();
                if(checkouts % interval == interval - 1) {
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
    }

    /**
     * Calculate the average and reset the stats
     */
    public void resetForInterval() {
        // harmless race condition:
        this.totalCheckoutTimeUs.set(0);
        this.connectionsCheckedout.set(0);
        checkoutTimeUsHistogram.reset();
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

    /* getters */

    public int getConnectionsCreated() {
        return connectionsCreated.intValue();
    }

    public int getConnectionsDestroyed() {
        return connectionsDestroyed.intValue();
    }

    public int getConnectionsCheckedout() {
        return connectionsCheckedout.intValue();
    }

    public Histogram getWaitHistogram() {
        return this.checkoutTimeUsHistogram;
    }

    public long getAveWaitUs() {
        long ns = totalCheckoutTimeUs.get();
        int count = connectionsCheckedout.get();
        this.avgCheckoutTimeUs.set(count > 0 ? (ns / count) : -1);
        return this.avgCheckoutTimeUs.longValue();
    }

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

    public void setMonitoringInterval(int count) {
        this.monitoringInterval.set(count);
    }

    public int getMonitoringInterval() {
        return this.monitoringInterval.get();
    }

    public void setPool(KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool) {
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
