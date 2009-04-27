/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.socket;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;

/**
 * A pool of sockets keyed off the socket destination. This wrapper just
 * translates exceptions and delegates to apache commons pool as well as
 * providing some JMX access.
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "Voldemort socket pool.")
public class SocketPool {

    private static final Logger logger = Logger.getLogger(SocketPool.class);
    private static int WAIT_MONITORING_INTERVAL = 10000;

    private final AtomicInteger checkouts;
    private final AtomicLong waitNs;
    private final AtomicLong avgWaitNs;
    private final KeyedObjectPool pool;
    private final SocketPoolableObjectFactory objFactory;

    public SocketPool(int maxConnectionsPerNode,
                      int maxTotalConnections,
                      int connectionTimeoutMs,
                      int soTimeoutMs,
                      int socketBufferSize) {
        GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
        config.maxActive = maxConnectionsPerNode;
        config.maxTotal = maxTotalConnections;
        config.maxIdle = maxTotalConnections;
        config.maxWait = connectionTimeoutMs;
        config.testOnBorrow = true;
        config.testOnReturn = true;
        config.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
        this.objFactory = new SocketPoolableObjectFactory(soTimeoutMs, socketBufferSize);
        this.pool = new GenericKeyedObjectPool(objFactory, config);
        this.checkouts = new AtomicInteger(0);
        this.waitNs = new AtomicLong(0);
        this.avgWaitNs = new AtomicLong(0);
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
            SocketAndStreams sas = (SocketAndStreams) pool.borrowObject(destination);
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
        if(count % WAIT_MONITORING_INTERVAL == WAIT_MONITORING_INTERVAL - 1) {
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
            pool.returnObject(destination, socket);
        } catch(Exception e) {
            try {
                pool.invalidateObject(destination, socket);
            } catch(Exception e2) {
                logger.error("Error while destroying socket:", e2);
            }
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        }
    }

    /**
     * Close the socket pool
     */
    public void close() {
        try {
            pool.clear();
            pool.close();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

    @JmxGetter(name = "socketsCreated", description = "The number of sockets created by this pool.")
    public int getNumberSocketsCreated() {
        return this.objFactory.getNumberCreated();
    }

    @JmxGetter(name = "socketsDestroyed", description = "The number of sockets destroyed by this pool.")
    public int getNumberSocketsDestroyed() {
        return this.objFactory.getNumberDestroyed();
    }

    @JmxGetter(name = "numberOfActiveConnections", description = "The number of active connections.")
    public int getNumberOfActiveConnections() {
        return this.pool.getNumActive();
    }

    @JmxGetter(name = "numberOfIdleConnections", description = "The number of active connections.")
    public int getNumberOfIdleConnections() {
        return this.pool.getNumIdle();
    }

    @JmxGetter(name = "avgWaitTimeMs", description = "The avg. ms of wait time to acquire a connection.")
    public double getAvgWaitTimeMs() {
        return this.avgWaitNs.doubleValue() / Time.NS_PER_MS;
    }

}
