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

package voldemort.store.socket.clientrequest;

import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.store.UnreachableStoreException;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.stats.ClientSocketStats;
import voldemort.store.stats.ClientSocketStatsJmx;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.utils.pool.KeyedResourcePool;
import voldemort.utils.pool.ResourcePoolConfig;

/**
 * A pool of {@link ClientRequestExecutor} keyed off the
 * {@link SocketDestination}. This is a wrapper around {@link KeyedResourcePool}
 * that translates exceptions as well as providing some JMX access.
 * 
 * <p/>
 * 
 * Upon successful construction of this object, a new Thread is started. It is
 * terminated upon calling {@link #close()}.
 */

public class ClientRequestExecutorPool implements SocketStoreFactory {

    private final KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;
    private final ClientRequestExecutorFactory factory;
    private final ClientSocketStats stats;
    private final boolean jmxEnabled;
    private final int jmxId;

    public ClientRequestExecutorPool(int selectors,
                                     int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize,
                                     boolean socketKeepAlive,
                                     boolean jmxEnabled,
                                     int jmxId) {
        ResourcePoolConfig config = new ResourcePoolConfig().setIsFair(true)
                                                            .setMaxPoolSize(maxConnectionsPerNode)
                                                            .setMaxInvalidAttempts(maxConnectionsPerNode)
                                                            .setTimeout(connectionTimeoutMs,
                                                                        TimeUnit.MILLISECONDS);
        this.jmxEnabled = jmxEnabled;
        this.jmxId = jmxId;
        if(this.jmxEnabled) {
            stats = new ClientSocketStats(jmxId);
            JmxUtils.registerMbean(new ClientSocketStatsJmx(stats),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                             "aggregated"
                                                                     + JmxUtils.getJmxId(this.jmxId)));
        } else {
            stats = null;
        }
        this.factory = new ClientRequestExecutorFactory(selectors,
                                                        connectionTimeoutMs,
                                                        soTimeoutMs,
                                                        socketBufferSize,
                                                        socketKeepAlive,
                                                        stats);
        this.pool = new KeyedResourcePool<SocketDestination, ClientRequestExecutor>(factory, config);
        if(stats != null) {
            this.stats.setPool(pool);
        }
    }

    public ClientRequestExecutorPool(int selectors,
                                     int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize,
                                     boolean socketKeepAlive) {
        // JMX bean is disabled by default
        this(selectors,
             maxConnectionsPerNode,
             connectionTimeoutMs,
             soTimeoutMs,
             socketBufferSize,
             socketKeepAlive,
             false,
             0);
    }

    public ClientRequestExecutorPool(int maxConnectionsPerNode,
                                     int connectionTimeoutMs,
                                     int soTimeoutMs,
                                     int socketBufferSize) {
        // maintain backward compatibility of API
        this(2, maxConnectionsPerNode, connectionTimeoutMs, soTimeoutMs, socketBufferSize, false);
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
        return new SocketStore(Utils.notNull(storeName),
                               factory.getTimeout(),
                               dest,
                               this,
                               requestRoutingType);
    }

    /**
     * Checkout a socket from the pool
     * 
     * @param destination The socket destination you want to connect to
     * @return The socket
     */

    public ClientRequestExecutor checkout(SocketDestination destination) {
        // time checkout
        long start = System.nanoTime();
        ClientRequestExecutor clientRequestExecutor;
        try {
            clientRequestExecutor = pool.checkout(destination);
        } catch(Exception e) {
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        } finally {
            long end = System.nanoTime();
            if(stats != null) {
                stats.recordCheckoutTimeUs(destination, (end - start) / Time.NS_PER_US);
            }
        }
        return clientRequestExecutor;
    }

    /**
     * Check the socket back into the pool.
     * 
     * @param destination The socket destination of the socket
     * @param clientRequestExecutor The request executor wrapper
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
        factory.setLastClosedTimestamp(destination);
        pool.close(destination);
    }

    /**
     * Close the socket pool
     */
    public void close() {
        // unregister MBeans
        if(stats != null) {
            try {
                if(this.jmxEnabled)
                    JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                                       "aggregated"
                                                                               + JmxUtils.getJmxId(this.jmxId)));
            } catch(Exception e) {}
            stats.close();
        }
        factory.close();
        pool.close();
    }

    public ClientSocketStats getStats() {
        return stats;
    }

}
