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

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.common.nio.SelectorManager;
import voldemort.store.socket.SocketDestination;
import voldemort.store.stats.ClientSocketStats;
import voldemort.utils.DaemonThreadFactory;
import voldemort.utils.Time;
import voldemort.utils.pool.ResourceFactory;

/**
 * A Factory for creating ClientRequestExecutor instances.
 */

public class ClientRequestExecutorFactory implements
        ResourceFactory<SocketDestination, ClientRequestExecutor> {

    private static final int SHUTDOWN_TIMEOUT_MS = 15000;
    private final int connectTimeoutMs;
    private final int soTimeoutMs;
    private final int socketBufferSize;
    private final AtomicInteger created;
    private final AtomicInteger destroyed;
    private final boolean socketKeepAlive;
    private final ClientRequestSelectorManager[] selectorManagers;
    private final ExecutorService selectorManagerThreadPool;
    private final AtomicInteger counter = new AtomicInteger();
    private final Map<SocketDestination, Long> lastClosedTimestamps;
    private final Logger logger = Logger.getLogger(getClass());
    private final ClientSocketStats stats;

    public ClientRequestExecutorFactory(int selectors,
                                        int connectTimeoutMs,
                                        int soTimeoutMs,
                                        int socketBufferSize,
                                        boolean socketKeepAlive,
                                        ClientSocketStats stats) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.soTimeoutMs = soTimeoutMs;
        this.created = new AtomicInteger(0);
        this.destroyed = new AtomicInteger(0);
        this.socketBufferSize = socketBufferSize;
        this.socketKeepAlive = socketKeepAlive;
        this.stats = stats;

        this.selectorManagers = new ClientRequestSelectorManager[selectors];
        this.selectorManagerThreadPool = Executors.newFixedThreadPool(selectorManagers.length,
                                                                      new DaemonThreadFactory("voldemort-niosocket-client-"));

        for(int i = 0; i < selectorManagers.length; i++) {
            selectorManagers[i] = new ClientRequestSelectorManager();
            selectorManagerThreadPool.execute(selectorManagers[i]);
        }

        this.lastClosedTimestamps = new ConcurrentHashMap<SocketDestination, Long>();
    }

    /**
     * Close the ClientRequestExecutor.
     */

    public void destroy(SocketDestination dest, ClientRequestExecutor clientRequestExecutor)
            throws Exception {
        clientRequestExecutor.close();
        int numDestroyed = destroyed.incrementAndGet();
        if(stats != null) {
            stats.connectionDestroy(dest);
        }

        if(logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    /**
     * Create a ClientRequestExecutor for the given {@link SocketDestination}.
     * 
     * @param dest {@link SocketDestination}
     */

    public ClientRequestExecutor create(SocketDestination dest) throws Exception {
        int numCreated = created.incrementAndGet();
        if(logger.isDebugEnabled())
            logger.debug("Creating socket " + numCreated + " for " + dest.getHost() + ":"
                         + dest.getPort() + " using protocol "
                         + dest.getRequestFormatType().getCode());

        SocketChannel socketChannel = null;
        ClientRequestExecutor clientRequestExecutor = null;

        try {
            socketChannel = SocketChannel.open();
            socketChannel.socket().setReceiveBufferSize(this.socketBufferSize);
            socketChannel.socket().setSendBufferSize(this.socketBufferSize);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSoTimeout(soTimeoutMs);
            socketChannel.socket().setKeepAlive(this.socketKeepAlive);
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(dest.getHost(), dest.getPort()));

            long startTimeMs = System.currentTimeMillis();
            long durationMs = 0;
            long currWaitTimeMs = 1;
            long prevWaitTimeMS = 1;

            // Since we're non-blocking and it takes a non-zero amount of time
            // to connect, invoke finishConnect and loop.
            while(!socketChannel.finishConnect()) {
                durationMs = System.currentTimeMillis() - startTimeMs;
                long remaining = this.connectTimeoutMs - durationMs;

                if(remaining < 0) {
                    throw new ConnectException("Cannot connect socket " + numCreated + " for "
                                               + dest.getHost() + ":" + dest.getPort() + " after "
                                               + durationMs + " ms");
                }

                if(logger.isTraceEnabled())
                    logger.trace("Still creating socket " + numCreated + " for " + dest.getHost()
                                 + ":" + dest.getPort() + ", " + remaining
                                 + " ms. remaining to connect");

                try {
                    // Break up the connection timeout into smaller units,
                    // employing a Fibonacci-style back-off (1, 2, 3, 5, 8, ...)
                    Thread.sleep(Math.min(remaining, currWaitTimeMs));
                    currWaitTimeMs = Math.min(currWaitTimeMs + prevWaitTimeMS, 50);
                    prevWaitTimeMS = currWaitTimeMs - prevWaitTimeMS;
                } catch(InterruptedException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }
            }
            durationMs = System.currentTimeMillis() - startTimeMs;

            if(logger.isDebugEnabled())
                logger.debug("Created socket " + numCreated + " for " + dest.getHost() + ":"
                             + dest.getPort() + " using protocol "
                             + dest.getRequestFormatType().getCode() + " after " + durationMs
                             + " ms.");

            // check buffer sizes--you often don't get out what you put in!
            if(socketChannel.socket().getReceiveBufferSize() != this.socketBufferSize)
                logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                             + " bytes but actual size is "
                             + socketChannel.socket().getReceiveBufferSize() + " bytes.");

            if(socketChannel.socket().getSendBufferSize() != this.socketBufferSize)
                logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                             + " bytes but actual size is "
                             + socketChannel.socket().getSendBufferSize() + " bytes.");

            ClientRequestSelectorManager selectorManager = selectorManagers[counter.getAndIncrement()
                                                                            % selectorManagers.length];

            Selector selector = selectorManager.getSelector();
            clientRequestExecutor = new ClientRequestExecutor(selector,
                                                              socketChannel,
                                                              socketBufferSize);
            BlockingClientRequest<String> clientRequest = new BlockingClientRequest<String>(new ProtocolNegotiatorClientRequest(dest.getRequestFormatType()),
                                                                                            this.getTimeout());
            clientRequestExecutor.addClientRequest(clientRequest);

            selectorManager.registrationQueue.add(clientRequestExecutor);
            selector.wakeup();

            // Block while we wait for protocol negotiation to complete. May
            // throw interrupted exception
            clientRequest.await();

            // Either returns uninteresting token, or throws exception if
            // protocol negotiation failed.
            clientRequest.getResult();
        } catch(Exception e) {
            // Make sure not to leak socketChannels
            if(socketChannel != null) {
                try {
                    socketChannel.close();
                } catch(Exception ex) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(ex, ex);
                }
            }
            // If clientRequestExector is not null, some additional clean up may
            // be warranted. However, clientRequestExecutor.close(), the
            // "obvious" clean up, is not safe to call here. This is because
            // .close() checks in a resource to the KeyedResourcePool that was
            // never checked out.

            throw e;
        }

        if(stats != null) {
            stats.connectionCreate(dest);
        }

        return clientRequestExecutor;
    }

    public boolean validate(SocketDestination dest, ClientRequestExecutor clientRequestExecutor) {
        /**
         * Keep track of the last time that we closed the sockets for a specific
         * SocketDestination. That way we know which sockets were created
         * *before* the SocketDestination was closed. For any sockets in the
         * pool at time of closure of the SocketDestination, these are shut down
         * immediately. For in-flight sockets that aren't in the pool at time of
         * closure of the SocketDestination, these are caught when they're
         * checked in via validate noting the relation of the timestamps.
         * 
         * See bug #222.
         */
        long lastClosedTimestamp = getLastClosedTimestamp(dest);

        if(clientRequestExecutor.getCreateTimestamp() <= lastClosedTimestamp) {
            if(logger.isDebugEnabled())
                logger.debug("Socket connection "
                             + clientRequestExecutor
                             + " was created on "
                             + new Date(clientRequestExecutor.getCreateTimestamp() / Time.NS_PER_MS)
                             + " before socket pool was closed and re-created (on "
                             + new Date(lastClosedTimestamp / Time.NS_PER_MS) + ")");
            return false;
        }

        boolean isValid = clientRequestExecutor.isValid();

        if(!isValid && logger.isDebugEnabled())
            logger.debug("Client request executor connection " + clientRequestExecutor
                         + " is no longer valid, closing.");

        return isValid;
    }

    public int getTimeout() {
        return this.soTimeoutMs;
    }

    public int getNumberCreated() {
        return this.created.get();
    }

    public int getNumberDestroyed() {
        return this.destroyed.get();
    }

    public void close() {
        try {
            // We close instead of interrupting the thread pool. Why? Because as
            // of 0.70, the SelectorManager services RequestHandler in the same
            // thread as itself. So, if we interrupt the SelectorManager in the
            // thread pool, we interrupt the request. In some RequestHandler
            // implementations interruptions are not handled gracefully and/or
            // indicate other errors which cause odd side effects. So we
            // implement a non-interrupt-based shutdown via close.
            for(int i = 0; i < selectorManagers.length; i++) {
                try {
                    selectorManagers[i].close();
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }

            // As per the above comment - we use shutdown and *not* shutdownNow
            // to avoid using interrupts to signal shutdown.
            selectorManagerThreadPool.shutdown();

            if(logger.isTraceEnabled())
                logger.trace("Shut down SelectorManager thread pool acceptor, waiting "
                             + SHUTDOWN_TIMEOUT_MS + " ms for termination");

            boolean terminated = selectorManagerThreadPool.awaitTermination(SHUTDOWN_TIMEOUT_MS,
                                                                            TimeUnit.MILLISECONDS);

            if(!terminated) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("SelectorManager thread pool did not stop cleanly after "
                                + SHUTDOWN_TIMEOUT_MS + " ms");
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }
    }

    private class ClientRequestSelectorManager extends SelectorManager {

        private final Queue<ClientRequestExecutor> registrationQueue = new ConcurrentLinkedQueue<ClientRequestExecutor>();

        public Selector getSelector() {
            return selector;
        }

        /**
         * Process the {@link ClientRequestExecutor} registrations which are
         * made inside {@link ClientRequestExecutorFactory} on creation of a new
         * {@link ClientRequestExecutor}.
         */

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
                            logger.debug("Registering connection from " + socketChannel.socket());

                        socketChannel.register(selector,
                                               SelectionKey.OP_WRITE,
                                               clientRequestExecutor);

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

            // In blocking I/O, the higher level code can interrupt the thread
            // if a timeout has been exceeded, triggering an IOException and
            // stopping the read/write. However, for our asynchronous I/O, we
            // don't have any threads blocking on I/O. So we resort to polling
            // to check if the timeout has been exceeded. So loop over all of
            // the keys that are registered and call checkTimeout. (That method
            // will handle the canceling of the SelectionKey if need be.)
            try {
                Iterator<SelectionKey> i = selector.keys().iterator();

                while(i.hasNext()) {
                    SelectionKey selectionKey = i.next();
                    ClientRequestExecutor clientRequestExecutor = (ClientRequestExecutor) selectionKey.attachment();

                    // A race condition can occur wherein our SelectionKey is
                    // still registered but the attachment has be nulled out on
                    // its way to being canceled.
                    if(clientRequestExecutor != null) {
                        try {
                            clientRequestExecutor.checkTimeout();
                        } catch(Exception e) {
                            if(logger.isEnabledFor(Level.ERROR))
                                logger.error(e.getMessage(), e);
                        }
                    }
                }
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Returns the nanosecond-based timestamp of when this socket destination
     * was last closed. SocketDestination objects can be closed when their node
     * is marked as unavailable if the node goes down (temporarily or
     * otherwise). This timestamp is used to determine when sockets related to
     * the SocketDestination should be closed.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via setLastClosedTimestamp each
     * time the node is marked as unavailable.
     * 
     * @return Nanosecond-based timestamp of last close
     */

    private long getLastClosedTimestamp(SocketDestination socketDestination) {
        Long lastClosedTimestamp = lastClosedTimestamps.get(socketDestination);
        return lastClosedTimestamp != null ? lastClosedTimestamp.longValue() : 0;
    }

    /**
     * Assigns the last closed timestamp based on the current time in
     * nanoseconds.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via this method each time the
     * node is marked as unavailable.
     */

    public void setLastClosedTimestamp(SocketDestination socketDestination) {
        lastClosedTimestamps.put(socketDestination, System.nanoTime());
    }
}
