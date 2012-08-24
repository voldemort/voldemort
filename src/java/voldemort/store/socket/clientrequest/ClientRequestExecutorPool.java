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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.store.StoreTimeoutException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
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

    private final Logger logger = Logger.getLogger(ClientRequestExecutorPool.class);

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

    /**
     * Context necessary to setup async request for SocketDestination
     * destination and tracking details of this request.
     */
    private static class AsyncRequestContext<T> {

        public final ClientRequest<T> delegate;
        public final NonblockingStoreCallback callback;
        public final long timeoutMs;
        public final String operationName;

        // private final long startTimeNs;
        private ClientRequestExecutor clientRequestExecutor;

        public AsyncRequestContext(ClientRequest<T> delegate,
                                   NonblockingStoreCallback callback,
                                   long timeoutMs,
                                   String operationName) {
            this.delegate = delegate;
            this.callback = callback;
            this.timeoutMs = timeoutMs;
            this.operationName = operationName;

            // this.startTimeNs = System.nanoTime();
            this.clientRequestExecutor = null;
        }

    }

    public <T> void submitAsync(SocketDestination destination,
                                ClientRequest<T> delegate,
                                NonblockingStoreCallback callback,
                                long timeoutMs,
                                String operationName) {
        AsyncRequestContext<T> requestContext = new AsyncRequestContext<T>(delegate,
                                                                           callback,
                                                                           timeoutMs,
                                                                           operationName);

        submitAsyncRequest(destination, requestContext);
        return;
    }

    /**
     * Actually submit the enqueued async request with the checked out
     * destination.
     */
    private <T> void submitAsyncRequest(SocketDestination destination,
                                        AsyncRequestContext<T> context) {
        try {
            context.clientRequestExecutor = pool.checkout(destination);

            if(logger.isDebugEnabled()) {
                logger.debug("Async request start; type: "
                             + context.operationName
                             + " requestRef: "
                             + System.identityHashCode(context.delegate)
                             + " time: "
                             + System.currentTimeMillis()
                             + " server: "
                             + context.clientRequestExecutor.getSocketChannel()
                                                            .socket()
                                                            .getRemoteSocketAddress()
                             + " local socket: "
                             + context.clientRequestExecutor.getSocketChannel()
                                                            .socket()
                                                            .getLocalAddress()
                             + ":"
                             + context.clientRequestExecutor.getSocketChannel()
                                                            .socket()
                                                            .getLocalPort());
            }

        } catch(Exception e) {
            // If we can't check out a socket from the pool, we'll usually get
            // either an IOException (subclass) or an UnreachableStoreException
            // error. However, in the case of asynchronous calls, we want the
            // error to be reported via our callback, not returned to the caller
            // directly.
            if(!(e instanceof UnreachableStoreException))
                e = new UnreachableStoreException("Failure in " + context.operationName + ": "
                                                  + e.getMessage(), e);

            try {
                context.callback.requestComplete(e, 0);
            } catch(Exception ex) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(ex, ex);
            }

            return;
        }

        NonblockingStoreCallbackClientRequest<T> clientRequest = new NonblockingStoreCallbackClientRequest<T>(destination,
                                                                                                              context.delegate,
                                                                                                              context.clientRequestExecutor,
                                                                                                              context.callback);
        context.clientRequestExecutor.addClientRequest(clientRequest, context.timeoutMs);
        return;
    }

    private class NonblockingStoreCallbackClientRequest<T> implements ClientRequest<T> {

        private final SocketDestination destination;
        private final ClientRequest<T> clientRequest;
        private final ClientRequestExecutor clientRequestExecutor;
        private final NonblockingStoreCallback callback;
        private final long startNs;

        private volatile boolean isComplete;

        public NonblockingStoreCallbackClientRequest(SocketDestination destination,
                                                     ClientRequest<T> clientRequest,
                                                     ClientRequestExecutor clientRequestExecutor,
                                                     NonblockingStoreCallback callback) {
            this.destination = destination;
            this.clientRequest = clientRequest;
            this.clientRequestExecutor = clientRequestExecutor;
            this.callback = callback;
            this.startNs = System.nanoTime();
        }

        private void invokeCallback(Object o, long requestTime) {
            if(callback != null) {
                try {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Async request end; requestRef: "
                                     + System.identityHashCode(clientRequest)
                                     + " time: "
                                     + System.currentTimeMillis()
                                     + " server: "
                                     + clientRequestExecutor.getSocketChannel()
                                                            .socket()
                                                            .getRemoteSocketAddress()
                                     + " local socket: "
                                     + clientRequestExecutor.getSocketChannel()
                                                            .socket()
                                                            .getLocalAddress()
                                     + ":"
                                     + clientRequestExecutor.getSocketChannel()
                                                            .socket()
                                                            .getLocalPort() + " result: " + o);
                    }

                    callback.requestComplete(o, requestTime);
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }
            }
        }

        public void complete() {
            try {
                clientRequest.complete();
                Object result = clientRequest.getResult();

                invokeCallback(result, (System.nanoTime() - startNs) / Time.NS_PER_MS);
            } catch(Exception e) {
                invokeCallback(e, (System.nanoTime() - startNs) / Time.NS_PER_MS);
            } finally {
                checkin(destination, clientRequestExecutor);
                isComplete = true;
            }
        }

        public boolean isComplete() {
            return isComplete;
        }

        public boolean formatRequest(DataOutputStream outputStream) {
            return clientRequest.formatRequest(outputStream);
        }

        public T getResult() throws VoldemortException, IOException {
            return clientRequest.getResult();
        }

        public boolean isCompleteResponse(ByteBuffer buffer) {
            return clientRequest.isCompleteResponse(buffer);
        }

        public void parseResponse(DataInputStream inputStream) {
            clientRequest.parseResponse(inputStream);
        }

        public void timeOut() {
            clientRequest.timeOut();
            invokeCallback(new StoreTimeoutException("ClientRequestExecutor timed out. Cannot complete request."),
                           (System.nanoTime() - startNs) / Time.NS_PER_MS);
            checkin(destination, clientRequestExecutor);
        }

        public boolean isTimedOut() {
            return clientRequest.isTimedOut();
        }
    }

}
