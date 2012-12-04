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
import java.util.concurrent.TimeoutException;

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
import voldemort.utils.pool.AsyncResourceRequest;
import voldemort.utils.pool.QueuedKeyedResourcePool;
import voldemort.utils.pool.ResourcePoolConfig;

/**
 * A pool of {@link ClientRequestExecutor} keyed off the
 * {@link SocketDestination}. This is a wrapper around
 * {@link QueuedKeyedResourcePool} that translates exceptions, provides some JMX
 * access, and handles asynchronous requests for SocketDestinations.
 * 
 * <p/>
 * 
 * Upon successful construction of this object, a new Thread is started. It is
 * terminated upon calling {@link #close()}.
 */
public class ClientRequestExecutorPool implements SocketStoreFactory {

    private final QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor> queuedPool;
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
        this.queuedPool = new QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor>(factory,
                                                                                                config);
        if(stats != null) {
            this.stats.setPool(queuedPool);
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

    @Override
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
        // timing instrumentation (stats only)
        long startTimeNs = 0;
        if(stats != null) {
            startTimeNs = System.nanoTime();
        }

        ClientRequestExecutor clientRequestExecutor;
        try {
            clientRequestExecutor = queuedPool.checkout(destination);
        } catch(Exception e) {
            // If this exception caught here is from the nonBlockingPut call
            // within KeyedResourcePool.attemptGrow(), then there is the chance
            // a ClientRequestExector resource will be leaked. Cannot safely
            // deal with this here since clientRequestExecutor is not assigned
            // in this catch. Even if it was, clientRequestExecutore.close()
            // checks in the SocketDestination resource and so is not safe to
            // call.
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        } finally {
            if(stats != null) {
                stats.recordCheckoutTimeUs(destination, (System.nanoTime() - startTimeNs)
                                                        / Time.NS_PER_US);
                stats.recordCheckoutQueueLength(destination,
                                                queuedPool.getBlockingGetsCount(destination));
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
            queuedPool.checkin(destination, clientRequestExecutor);
        } catch(Exception e) {
            throw new VoldemortException("Failure while checking in socket for " + destination
                                         + ": ", e);
        }
    }

    /**
     * Reset the pool of resources for a specific destination. Idle resources
     * will be destroyed. Checked out resources that are subsequently checked in
     * will be destroyed. Newly created resources can be checked in to
     * reestablish resources for the specific destination.
     */
    @Override
    public void close(SocketDestination destination) {
        factory.setLastClosedTimestamp(destination);
        queuedPool.reset(destination);
    }

    /**
     * Permanently close the ClientRequestExecutor pool. Resources subsequently
     * checked in will be destroyed.
     */
    @Override
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
        queuedPool.close();
    }

    public ClientSocketStats getStats() {
        return stats;
    }

    public <T> void submitAsync(SocketDestination destination,
                                ClientRequest<T> delegate,
                                NonblockingStoreCallback callback,
                                long timeoutMs,
                                String operationName) {

        AsyncSocketDestinationRequest<T> asyncSocketDestinationRequest = new AsyncSocketDestinationRequest<T>(destination,
                                                                                                              delegate,
                                                                                                              callback,
                                                                                                              timeoutMs,
                                                                                                              operationName);
        queuedPool.registerResourceRequest(destination, asyncSocketDestinationRequest);
        return;
    }

    /**
     * Wrap up an asynchronous request and actually issue it once a
     * SocketDestination is checked out.
     */
    private class AsyncSocketDestinationRequest<T> implements
            AsyncResourceRequest<ClientRequestExecutor> {

        private final SocketDestination destination;
        public final ClientRequest<T> delegate;
        public final NonblockingStoreCallback callback;
        public final long timeoutMs;
        public final String operationName;

        private final long startTimeNs;

        public AsyncSocketDestinationRequest(SocketDestination destination,
                                             ClientRequest<T> delegate,
                                             NonblockingStoreCallback callback,
                                             long timeoutMs,
                                             String operationName) {
            this.destination = destination;
            this.delegate = delegate;
            this.callback = callback;
            this.timeoutMs = timeoutMs;
            this.operationName = operationName;

            this.startTimeNs = System.nanoTime();
        }

        protected void updateStats() {
            if(stats != null) {
                stats.recordResourceRequestTimeUs(destination, (System.nanoTime() - startTimeNs)
                                                               / Time.NS_PER_US);
                stats.recordResourceRequestQueueLength(destination,
                                                       queuedPool.getRegisteredResourceRequestCount(destination));
            }
        }

        @Override
        public void useResource(ClientRequestExecutor clientRequestExecutor) {
            updateStats();
            if(logger.isDebugEnabled()) {
                logger.debug("Async request start; type: "
                             + operationName
                             + " requestRef: "
                             + System.identityHashCode(delegate)
                             + " time: "
                             // Output time (ms) includes queueing delay (i.e.,
                             // time between when registerResourceRequest is
                             // called and time when useResource is invoked).
                             + (this.startTimeNs / Time.NS_PER_MS)
                             + " server: "
                             + clientRequestExecutor.getSocketChannel()
                                                    .socket()
                                                    .getRemoteSocketAddress() + " local socket: "
                             + clientRequestExecutor.getSocketChannel().socket().getLocalAddress()
                             + ":"
                             + clientRequestExecutor.getSocketChannel().socket().getLocalPort());
            }

            NonblockingStoreCallbackClientRequest<T> clientRequest = new NonblockingStoreCallbackClientRequest<T>(destination,
                                                                                                                  delegate,
                                                                                                                  clientRequestExecutor,
                                                                                                                  callback);
            clientRequestExecutor.addClientRequest(clientRequest, timeoutMs, System.nanoTime()
                                                                             - startTimeNs);
        }

        @Override
        public void handleTimeout() {
            // Do *not* invoke updateStats since handleException does so.
            long durationNs = System.nanoTime() - startTimeNs;
            handleException(new TimeoutException("Could not acquire resource in " + timeoutMs
                                                 + " ms. (Took " + durationNs + " ns.)"));
        }

        @Override
        public void handleException(Exception e) {
            updateStats();
            if(!(e instanceof UnreachableStoreException))
                e = new UnreachableStoreException("Failure in " + operationName + ": "
                                                  + e.getMessage(), e);
            try {
                // Because PerformParallel(Put||Delete|GetAll)Requests define
                // 'callback' via an anonymous class, callback can be null if
                // the client factory closes down and some other thread invokes
                // this code. This can cause NullPointerExceptions during
                // shutdown if async resource requests are queued up.
                callback.requestComplete(e, 0);
            } catch(Exception ex) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(ex, ex);
            }
        }

        @Override
        public long getDeadlineNs() {
            return startTimeNs + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        }
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

        @Override
        public void complete() {
            try {
                clientRequest.complete();
                Object result = clientRequest.getResult();

                invokeCallback(result, (System.nanoTime() - startNs) / Time.NS_PER_MS);
            } catch(Exception e) {
                invokeCallback(e, (System.nanoTime() - startNs) / Time.NS_PER_MS);
            } finally {
                isComplete = true;
                // checkin may throw a (new) exception. Any prior exception
                // has been passed off via invokeCallback.
                checkin(destination, clientRequestExecutor);
            }
        }

        @Override
        public boolean isComplete() {
            return isComplete;
        }

        @Override
        public boolean formatRequest(DataOutputStream outputStream) {
            return clientRequest.formatRequest(outputStream);
        }

        @Override
        public T getResult() throws VoldemortException, IOException {
            return clientRequest.getResult();
        }

        @Override
        public boolean isCompleteResponse(ByteBuffer buffer) {
            return clientRequest.isCompleteResponse(buffer);
        }

        @Override
        public void parseResponse(DataInputStream inputStream) {
            clientRequest.parseResponse(inputStream);
        }

        @Override
        public void timeOut() {
            clientRequest.timeOut();
            invokeCallback(new StoreTimeoutException("ClientRequestExecutor timed out. Cannot complete request."),
                           (System.nanoTime() - startNs) / Time.NS_PER_MS);
            checkin(destination, clientRequestExecutor);
        }

        @Override
        public boolean isTimedOut() {
            return clientRequest.isTimedOut();
        }
    }

}
