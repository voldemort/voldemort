/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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

package voldemort.server.niosocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.protocol.RequestHandlerFactory;

/**
 * SelectorManager handles the non-blocking polling of IO events using the
 * Selector/SelectionKey APIs from NIO.
 * <p/>
 * This is probably not the way to write NIO code, but it's much faster than the
 * documented way. All the documentation on NIO suggested that a single Selector
 * be used for all open sockets and then individual IO requests for selected
 * keys be stuck in a thread pool and executed asynchronously. This seems
 * logical and works fine. However, it was very slow, for two reasons.
 * <p>
 * First, the thread processing the event calls interestOps() on the
 * SelectionKey to update what types of events it's interested in. In fact, it
 * does this twice - first before any processing occurs it disables all events
 * (so that the same channel isn't selected concurrently (similar to disabling
 * interrupts)) and secondly after processing is completed to re-enable interest
 * in events. Understandably, interestOps() has some internal state that it
 * needs to update, and so the thread must grab a lock on the Selector to do
 * internal interest state modifications. With hundreds/thousands of threads,
 * this lock is very heavily contended as backed up by profiling and empirical
 * testing.
 * <p/>
 * The second reason the thread pool approach was slow was that after calling
 * interestOps() to re-enable events, the threads in the thread pool had to
 * invoke the Selector API's wakeup() method or else the state change would go
 * unnoticed (it's similar to notifyAll for basic thread synchronization). This
 * causes the select() method to return immediately and process whatever
 * requests are immediately available. However, with so many threads in play,
 * this lead to a near constant spinning of the select()/wakeup() cycling.
 * <p>
 * Astonishingly it was found to be about 25% faster to simply execute all IO
 * synchronously/serially as it eliminated the context switching, lock
 * contention, etc. However, we actually have N simultaneous SelectorManager
 * instances in play, which are round-robin-ed by the caller (NioSocketService).
 * <p>
 * In terms of the number of SelectorManager instances to use in parallel, the
 * configuration defaults to the number of active CPUs (multi-cores count). This
 * helps to balance out the load a little and help with the serial nature of
 * processing.
 * <p>
 * Of course, potential problems exist.
 * <p>
 * First of all, I still can't believe my eyes that processing these serially is
 * faster than in parallel. There may be something about my environment that is
 * causing inaccurate reporting. At some point, with enough requests I would
 * imagine this will start to slow down.
 * <p/>
 * Another potential problem is that a given SelectorManager could become
 * overloaded. As new socket channels are established, they're distributed to a
 * SelectorManager in a round-robin fashion. However, there's no re-balancing
 * logic in case a disproportionate number of clients on one SelectorManager
 * disconnect.
 * <p/>
 * For instance, let's say we have two SelectorManager instances and four
 * connections. Connection 1 goes to SelectorManager A, connection 2 to
 * SelectorManager B, 3 to A, and 4 to B. However, later on let's say that both
 * connections 1 and 3 disconnect. This leaves SelectorManager B with two
 * connections and SelectorManager A with none. There's no provision to
 * re-balance the remaining requests evenly.
 * 
 * @author Kirk True
 */

public class SelectorManager implements Runnable {

    private final InetSocketAddress endpoint;

    private final Selector selector;

    private final Queue<SocketChannel> socketChannelQueue;

    private final RequestHandlerFactory requestHandlerFactory;

    private final int socketBufferSize;

    private volatile boolean isClosed;

    private final Logger logger = Logger.getLogger(getClass());

    public SelectorManager(InetSocketAddress endpoint,
                           RequestHandlerFactory requestHandlerFactory,
                           int socketBufferSize) throws IOException {
        this.endpoint = endpoint;
        this.selector = Selector.open();
        this.socketChannelQueue = new ConcurrentLinkedQueue<SocketChannel>();
        this.requestHandlerFactory = requestHandlerFactory;
        this.socketBufferSize = socketBufferSize;
        isClosed = false;
    }

    public void accept(SocketChannel socketChannel) {
        if(isClosed)
            throw new IllegalStateException("Cannot accept more channels, selector manager closed");

        socketChannelQueue.add(socketChannel);
        selector.wakeup();
    }

    public void close() {
        if(isClosed)
            return;

        isClosed = true;

        try {
            for(SelectionKey sk: selector.keys()) {
                try {
                    if(logger.isTraceEnabled())
                        logger.trace("Closing SelectionKey's channel " + sk + " for " + endpoint);

                    sk.channel().close();
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }

                try {
                    if(logger.isTraceEnabled())
                        logger.trace("Cancelling SelectionKey " + sk + " for " + endpoint);

                    sk.cancel();
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            selector.close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }
    }

    public void run() {
        try {
            while(true) {
                if(isClosed) {
                    if(logger.isInfoEnabled())
                        logger.info("Closed, exiting" + " for " + endpoint);

                    break;
                }

                processSockets();

                try {
                    int selected = selector.select();

                    if(isClosed) {
                        if(logger.isInfoEnabled())
                            logger.info("Closed, exiting for " + endpoint);

                        break;
                    }

                    if(selected > 0) {
                        Iterator<SelectionKey> i = selector.selectedKeys().iterator();

                        while(i.hasNext()) {
                            SelectionKey selectionKey = i.next();
                            i.remove();

                            if(selectionKey.isReadable() || selectionKey.isWritable()) {
                                Runnable worker = (Runnable) selectionKey.attachment();
                                worker.run();
                            }
                        }
                    }
                } catch(ClosedSelectorException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Selector is closed, exiting for " + endpoint);

                    break;
                } catch(Throwable t) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(t.getMessage(), t);
                }
            }
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
        } finally {
            try {
                close();
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e.getMessage(), e);
            }
        }
    }

    private void processSockets() {
        try {
            SocketChannel socketChannel = null;

            while((socketChannel = socketChannelQueue.poll()) != null) {
                if(isClosed) {
                    if(logger.isInfoEnabled())
                        logger.debug("Closed, exiting for " + endpoint);

                    break;
                }

                try {
                    if(logger.isDebugEnabled())
                        logger.debug("Registering connection from "
                                     + socketChannel.socket().getPort());

                    socketChannel.socket().setTcpNoDelay(true);
                    socketChannel.socket().setReuseAddress(true);
                    socketChannel.socket().setSendBufferSize(socketBufferSize);

                    if(socketChannel.socket().getReceiveBufferSize() != this.socketBufferSize)
                        if(logger.isDebugEnabled())
                            logger.debug("Requested socket receive buffer size was "
                                         + this.socketBufferSize + " bytes but actual size is "
                                         + socketChannel.socket().getReceiveBufferSize()
                                         + " bytes.");

                    if(socketChannel.socket().getSendBufferSize() != this.socketBufferSize)
                        if(logger.isDebugEnabled())
                            logger.debug("Requested socket send buffer size was "
                                         + this.socketBufferSize + " bytes but actual size is "
                                         + socketChannel.socket().getSendBufferSize() + " bytes.");

                    socketChannel.configureBlocking(false);
                    AsyncRequestHandler attachment = new AsyncRequestHandler(selector,
                                                                             socketChannel,
                                                                             requestHandlerFactory,
                                                                             socketBufferSize);

                    if(!isClosed)
                        socketChannel.register(selector, SelectionKey.OP_READ, attachment);
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

}
