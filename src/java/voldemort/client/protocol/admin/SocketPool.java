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

package voldemort.client.protocol.admin;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxSetter;
import voldemort.store.UnreachableStoreException;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.Time;
import voldemort.utils.pool.KeyedResourcePool;
import voldemort.utils.pool.ResourcePoolConfig;

/**
 * A pool of sockets keyed off the socket destination. This wrapper just
 * translates exceptions and delegates to apache commons pool as well as
 * providing some JMX access.
 * 
 * 
 */
@JmxManaged(description = "Voldemort socket pool.")
public class SocketPool {

    private final AtomicInteger monitoringInterval = new AtomicInteger(10000);
    private final AtomicInteger checkouts;
    private final AtomicLong waitNs;
    private final AtomicLong avgWaitNs;
    private final KeyedResourcePool<SocketDestination, SocketAndStreams> pool;
    private final SocketResourceFactory socketFactory;

    public SocketPool(int maxConnectionsPerNode,
                      int connectionTimeoutMs,
                      int soTimeoutMs,
                      int socketBufferSize,
                      boolean socketKeepAlive) {
        ResourcePoolConfig config = new ResourcePoolConfig().setIsFair(true)
                                                            .setMaxPoolSize(maxConnectionsPerNode)
                                                            .setMaxInvalidAttempts(maxConnectionsPerNode)
                                                            .setTimeout(connectionTimeoutMs,
                                                                        TimeUnit.MILLISECONDS);
        this.socketFactory = new SocketResourceFactory(soTimeoutMs,
                                                       socketBufferSize,
                                                       socketKeepAlive);
        this.pool = new KeyedResourcePool<SocketDestination, SocketAndStreams>(socketFactory,
                                                                               config);
        this.checkouts = new AtomicInteger(0);
        this.waitNs = new AtomicLong(0);
        this.avgWaitNs = new AtomicLong(0);
    }

    public SocketPool(int maxConnectionsPerNode,
                      int connectionTimeoutMs,
                      int soTimeoutMs,
                      int socketBufferSize) {
        // maintain backward compatibility of API
        this(maxConnectionsPerNode, connectionTimeoutMs, soTimeoutMs, socketBufferSize, false);
    }

    /**
     * Checkout a socket from the pool
     * 
     * @param destination The socket destination you want to connect to
     * @return The socket
     */
    public SocketAndStreams checkout(SocketDestination destination) {
        try {
            // time checkout
            long start = System.nanoTime();
            SocketAndStreams sas = pool.checkout(destination);
            updateStats(System.nanoTime() - start);

            return sas;
        } catch(Exception e) {
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        }
    }

    private void updateStats(long checkoutTimeNs) {
        long wait = waitNs.getAndAdd(checkoutTimeNs);
        int count = checkouts.getAndIncrement();

        // reset reporting inverval if we have used up the current interval
        int interval = this.monitoringInterval.get();
        if(count % interval == interval - 1) {
            // harmless race condition:
            waitNs.set(0);
            checkouts.set(0);
            avgWaitNs.set(wait / count);
        }
    }

    /**
     * Check the socket back into the pool.
     * 
     * @param destination The socket destination of the socket
     * @param socket The socket to check back in
     */
    public void checkin(SocketDestination destination, SocketAndStreams socket) {
        try {
            pool.checkin(destination, socket);
        } catch(Exception e) {
            throw new VoldemortException("Failure while checking in socket for " + destination
                                         + ": ", e);
        }
    }

    public void close(SocketDestination destination) {
        socketFactory.setLastClosedTimestamp(destination);
        pool.reset(destination);
    }

    /**
     * Close the socket pool
     */
    public void close() {
        pool.close();
    }

    @JmxGetter(name = "socketsCreated", description = "The total number of sockets created by this pool.")
    public int getNumberSocketsCreated() {
        return this.socketFactory.getNumberCreated();
    }

    @JmxGetter(name = "socketsDestroyed", description = "The total number of sockets destroyed by this pool.")
    public int getNumberSocketsDestroyed() {
        return this.socketFactory.getNumberDestroyed();
    }

    @JmxGetter(name = "numberOfConnections", description = "The number of active connections.")
    public int getNumberOfActiveConnections() {
        return this.pool.getTotalResourceCount();
    }

    @JmxGetter(name = "numberOfIdleConnections", description = "The number of active connections.")
    public int getNumberOfCheckedInConnections() {
        return this.pool.getCheckedInResourceCount();
    }

    @JmxGetter(name = "avgWaitTimeMs", description = "The avg. ms of wait time to acquire a connection.")
    public double getAvgWaitTimeMs() {
        return this.avgWaitNs.doubleValue() / Time.NS_PER_MS;
    }

    @JmxSetter(name = "monitoringInterval", description = "The number of checkouts over which performance statistics are calculated.")
    public void setMonitoringInterval(int count) {
        if(count <= 0)
            throw new IllegalArgumentException("Monitoring interval must be a positive number.");
        this.monitoringInterval.set(count);
    }

}
