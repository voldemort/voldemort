/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.store.socket.clientrequest;

import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxSetter;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.store.UnreachableStoreException;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.utils.SelectorManager;
import voldemort.utils.Time;
import voldemort.utils.Utils;
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
public class ClientRequestExecutorPool extends SelectorManager implements SocketStoreFactory {

    private final AtomicInteger monitoringInterval = new AtomicInteger(10000);
    private final AtomicInteger checkouts;
    private final AtomicLong waitNs;
    private final AtomicLong avgWaitNs;
    private final Queue<ClientRequestExecutor> registrationQueue;
    private final KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;
    private final ClientRequestExecutorFactory factory;

    public ClientRequestExecutorPool(int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize,
                                     boolean socketKeepAlive) {
        ResourcePoolConfig config = new ResourcePoolConfig().setIsFair(true)
                                                            .setMaxPoolSize(maxConnectionsPerNode)
                                                            .setMaxInvalidAttempts(maxConnectionsPerNode)
                                                            .setTimeout(connectionTimeoutMs,
                                                                        TimeUnit.MILLISECONDS);
        this.registrationQueue = new ConcurrentLinkedQueue<ClientRequestExecutor>();
        this.factory = new ClientRequestExecutorFactory(selector,
                                                        registrationQueue,
                                                        soTimeoutMs,
                                                        socketBufferSize,
                                                        socketKeepAlive);

        this.pool = new KeyedResourcePool<SocketDestination, ClientRequestExecutor>(factory, config);
        this.checkouts = new AtomicInteger(0);
        this.waitNs = new AtomicLong(0);
        this.avgWaitNs = new AtomicLong(0);

        new Thread(this, "ClientRequestExecutorPool").start();
    }

    public ClientRequestExecutorPool(int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize) {
        // maintain backward compatibility of API
        this(maxConnectionsPerNode, connectionTimeoutMs, soTimeoutMs, socketBufferSize, false);
    }

    public ClientRequestExecutorFactory getFactory() {
        return factory;
    }

    public SocketStore create(String storeName,
                              String hostName,
                              int port,
                              RequestFormatType requestFormatType,
                              RequestRoutingType requestRoutingType) {
        SocketDestination dest = new SocketDestination(Utils.notNull(hostName),
                                                       port,
                                                       requestFormatType);
        return new SocketStore(Utils.notNull(storeName), dest, this, requestRoutingType);
    }

    /**
     * Checkout a socket from the pool
     * 
     * @param destination The socket destination you want to connect to
     * @return The socket
     */

    public ClientRequestExecutor checkout(SocketDestination destination) {
        try {
            // time checkout
            long start = System.nanoTime();
            ClientRequestExecutor clientRequestExecutor = pool.checkout(destination);
            updateStats(System.nanoTime() - start);

            return clientRequestExecutor;
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
    public void checkin(SocketDestination destination, ClientRequestExecutor clientRequestExecutor) {
        try {
            pool.checkin(destination, clientRequestExecutor);
        } catch(Exception e) {
            throw new VoldemortException("Failure while checking in socket for " + destination
                                         + ": ", e);
        }
    }

    public void close(SocketDestination destination) {
        destination.setLastClosedTimestamp();
        pool.close(destination);
    }

    @Override
    protected void processEvents() {
        try {
            ClientRequestExecutor clientRequestExecutor = null;

            while((clientRequestExecutor = registrationQueue.poll()) != null) {
                if(isClosed.get()) {
                    if(logger.isDebugEnabled())
                        logger.debug("Closed, exiting");

                    break;
                }

                SocketChannel socketChannel = clientRequestExecutor.getSocketChannel();

                try {
                    if(logger.isDebugEnabled())
                        logger.debug("Registering connection from "
                                     + socketChannel.socket().getPort());

                    socketChannel.register(selector, SelectionKey.OP_WRITE, clientRequestExecutor);
                } catch(ClosedSelectorException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Selector is closed, exiting");

                    close();

                    break;
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(e.getMessage(), e);
                }
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(e.getMessage(), e);
        }
    }

    /**
     * Close the socket pool
     */
    @Override
    public void close() {
        super.close();
        pool.close();
    }

    @JmxGetter(name = "socketsCreated", description = "The total number of sockets created by this pool.")
    public int getNumberSocketsCreated() {
        return this.factory.getNumberCreated();
    }

    @JmxGetter(name = "socketsDestroyed", description = "The total number of sockets destroyed by this pool.")
    public int getNumberSocketsDestroyed() {
        return this.factory.getNumberDestroyed();
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
