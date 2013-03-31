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

package voldemort.common.nio;

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;

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
 */

public class SelectorManager implements Runnable {

    public static final int SELECTOR_POLL_MS = 500;

    protected final Selector selector;

    protected final AtomicBoolean isClosed;

    protected final Logger logger = Logger.getLogger(getClass());

    // statistics about the current select loop
    /**
     * Number of connections selected (meaning they have some data to be
     * read/written) in the current processing loop
     */
    protected int selectCount = -1;
    /**
     * Amount of time taken to process all the connections selected in this
     * processing loop
     */
    protected long processingTimeMs = -1;
    /**
     * Amount of time spent in the select() call. This is an indicator of how
     * busy the thread is
     */
    protected long selectTimeMs = -1;

    public SelectorManager() {
        try {
            this.selector = Selector.open();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        this.isClosed = new AtomicBoolean(false);
    }

    public void close() {
        // Attempt to close, but if already closed, then we've been beaten to
        // the punch...
        if(!isClosed.compareAndSet(false, true))
            return;

        try {
            for(SelectionKey sk: selector.keys()) {
                try {
                    if(logger.isTraceEnabled())
                        logger.trace("Closing SelectionKey's channel");

                    sk.channel().close();
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }

                try {
                    if(logger.isTraceEnabled())
                        logger.trace("Cancelling SelectionKey");

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

    /**
     * This is a stub method to process any "events" before we go back to
     * select-ing again. This is the place to process queues for registering new
     * Channel instances, for example.
     */

    protected void processEvents() {

    }

    public void run() {
        try {
            while(true) {
                if(isClosed.get()) {
                    if(logger.isInfoEnabled())
                        logger.info("Closed, exiting");

                    break;
                }

                processEvents();

                try {
                    selectTimeMs = System.currentTimeMillis();
                    int selected = selector.select(SELECTOR_POLL_MS);
                    selectTimeMs = System.currentTimeMillis() - selectTimeMs;
                    selectCount = selected;

                    if(isClosed.get()) {
                        if(logger.isInfoEnabled())
                            logger.info("Closed, exiting");

                        break;
                    }

                    if(selected > 0) {
                        processingTimeMs = System.currentTimeMillis();
                        Iterator<SelectionKey> i = selector.selectedKeys().iterator();

                        while(i.hasNext()) {
                            SelectionKey selectionKey = i.next();
                            i.remove();

                            if(selectionKey.isValid()
                               && (selectionKey.isReadable() || selectionKey.isWritable())) {
                                Runnable worker = (Runnable) selectionKey.attachment();
                                worker.run();
                            }
                        }
                        processingTimeMs = System.currentTimeMillis() - processingTimeMs;
                    }
                } catch(ClosedSelectorException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Selector is closed, exiting");

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
}
