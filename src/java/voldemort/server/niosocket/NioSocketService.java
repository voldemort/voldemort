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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.common.service.ServiceType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StatusManager;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.utils.DaemonThreadFactory;

/**
 * NioSocketService is an NIO-based socket service, comparable to the
 * blocking-IO-based socket service.
 * <p/>
 * The NIO server is enabled in the server.properties file by setting the
 * "enable.nio.connector" property to "true". If you want to adjust the number
 * of SelectorManager instances that are used, change "nio.connector.selectors"
 * to a positive integer value. Otherwise, the number of selectors will be equal
 * to the number of CPUs visible to the JVM.
 * <p/>
 * This code uses the NIO APIs directly. It would be a good idea to consider
 * some of the NIO frameworks to handle this more cleanly, efficiently, and to
 * handle corner cases.
 * 
 * 
 * @see voldemort.server.socket.SocketService
 */

public class NioSocketService extends AbstractSocketService {

    private static final int SHUTDOWN_TIMEOUT_MS = 15000;

    private final RequestHandlerFactory requestHandlerFactory;

    private final ServerSocketChannel serverSocketChannel;

    private final InetSocketAddress endpoint;

    private final NioSelectorManager[] selectorManagers;

    private final ExecutorService selectorManagerThreadPool;

    private final int socketBufferSize;

    private final int acceptorBacklog;

    private final StatusManager statusManager;

    private final Thread acceptorThread;

    private final Logger logger = Logger.getLogger(getClass());

    public NioSocketService(RequestHandlerFactory requestHandlerFactory,
                            int port,
                            int socketBufferSize,
                            int selectors,
                            String serviceName,
                            boolean enableJmx,
                            int acceptorBacklog) {
        super(ServiceType.SOCKET, port, serviceName, enableJmx);
        this.requestHandlerFactory = requestHandlerFactory;
        this.socketBufferSize = socketBufferSize;
        this.acceptorBacklog = acceptorBacklog;

        try {
            this.serverSocketChannel = ServerSocketChannel.open();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        this.endpoint = new InetSocketAddress(port);

        this.selectorManagers = new NioSelectorManager[selectors];
        this.selectorManagerThreadPool = Executors.newFixedThreadPool(selectorManagers.length,
                                                                      new DaemonThreadFactory("voldemort-niosocket-server"));
        this.statusManager = new StatusManager((ThreadPoolExecutor) this.selectorManagerThreadPool);
        this.acceptorThread = new Thread(new Acceptor(), "NioSocketService.Acceptor");
    }

    @Override
    public StatusManager getStatusManager() {
        return statusManager;
    }

    @Override
    protected void startInner() {
        if(logger.isEnabledFor(Level.INFO))
            logger.info("Starting Voldemort NIO socket server (" + serviceName + ") on port "
                        + port);

        try {
            for(int i = 0; i < selectorManagers.length; i++) {
                selectorManagers[i] = new NioSelectorManager(endpoint,
                                                             requestHandlerFactory,
                                                             socketBufferSize);
                selectorManagerThreadPool.execute(selectorManagers[i]);
            }

            serverSocketChannel.socket().bind(endpoint, acceptorBacklog);
            serverSocketChannel.socket().setReceiveBufferSize(socketBufferSize);
            serverSocketChannel.socket().setReuseAddress(true);

            acceptorThread.start();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }

        enableJmx(this);
    }

    @Override
    protected void stopInner() {
        if(logger.isEnabledFor(Level.INFO))
            logger.info("Stopping Voldemort NIO socket server (" + serviceName + ") on port "
                        + port);

        try {
            // Signal the thread to stop accepting new connections...
            if(logger.isTraceEnabled())
                logger.trace("Interrupted acceptor thread, waiting " + SHUTDOWN_TIMEOUT_MS
                             + " ms for termination");

            acceptorThread.interrupt();
            acceptorThread.join(SHUTDOWN_TIMEOUT_MS);

            if(acceptorThread.isAlive()) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Acceptor thread pool did not stop cleanly after "
                                + SHUTDOWN_TIMEOUT_MS + " ms");
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

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

        try {
            serverSocketChannel.socket().close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            serverSocketChannel.close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }
    }

    private class Acceptor implements Runnable {

        public void run() {
            if(logger.isInfoEnabled())
                logger.info("Server now listening for connections on port " + port);

            AtomicInteger counter = new AtomicInteger();

            while(true) {
                if(Thread.currentThread().isInterrupted()) {
                    if(logger.isInfoEnabled())
                        logger.info("Acceptor thread interrupted");

                    break;
                }

                try {
                    SocketChannel socketChannel = serverSocketChannel.accept();

                    if(socketChannel == null) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn("Claimed accept but nothing to select");

                        continue;
                    }

                    NioSelectorManager selectorManager = selectorManagers[counter.getAndIncrement()
                                                                          % selectorManagers.length];
                    selectorManager.accept(socketChannel);
                } catch(ClosedByInterruptException e) {
                    // If you're *really* interested...
                    if(logger.isTraceEnabled())
                        logger.trace("Acceptor thread interrupted, closing");

                    break;
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }

            if(logger.isInfoEnabled())
                logger.info("Server has stopped listening for connections on port " + port);
        }

    }

    @JmxGetter(name = "numActiveConnections", description = "total number of active connections across selector managers")
    public final int getNumActiveConnections() {
        int sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getNumActiveConnections();
        }
        return sum;
    }

    @JmxGetter(name = "numQueuedConnections", description = "total number of connections pending for registration with selector managers")
    public final int getNumQueuedConnections() {
        int sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getNumQueuedConnections();
        }
        return sum;
    }

    @JmxGetter(name = "selectCountAvg", description = "average number of connections selected in each select() call")
    public final double getSelectCountAvg() {
        double sum = 0.0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getSelectCountHistogram().getAverage();
        }
        return sum / selectorManagers.length;
    }

    @JmxGetter(name = "selectCount99th", description = "99th percentile of number of connections selected in each select() call")
    public final double getSelectCount99th() {
        double sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getSelectCountHistogram().getQuantile(0.99);
        }
        return sum / selectorManagers.length;
    }

    @JmxGetter(name = "selectTimeMsAvg", description = "average time spent in the select() call")
    public final double getSelectTimeMsAvg() {
        double sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getSelectTimeMsHistogram().getAverage();
        }
        return sum / selectorManagers.length;
    }

    @JmxGetter(name = "selectTimeMs99th", description = "99th percentile of time spent in the select() call")
    public final double getSelectTimeMs99th() {
        double sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getSelectTimeMsHistogram().getQuantile(0.99);
        }
        return sum / selectorManagers.length;
    }

    @JmxGetter(name = "processingTimeMsAvg", description = "average time spent processing all read/write requests, in a select() loop")
    public final double getProcessingTimeMsAvg() {
        double sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getProcessingTimeMsHistogram().getAverage();
        }
        return sum / selectorManagers.length;
    }

    @JmxGetter(name = "processingTimeMs99th", description = "99th percentile of time spent processing all the read/write requests, in a select() loop")
    public final double getprocessingTimeMs99th() {
        double sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getProcessingTimeMsHistogram().getQuantile(0.99);
        }
        return sum / selectorManagers.length;
    }

    @JmxGetter(name = "commReadBufferSize", description = "total amount of memory consumed by all the communication read buffers, in bytes")
    public final double getCommReadBufferSize() {
        long sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getCommBufferSizeStats().getCommReadBufferSizeTracker().longValue();
        }
        return sum;
    }

    @JmxGetter(name = "commWriteBufferSize", description = "total amount of memory consumed by all the communication write buffers, in bytes")
    public final double getCommWriteBufferSize() {
        long sum = 0;
        for(NioSelectorManager manager: selectorManagers) {
            sum += manager.getCommBufferSizeStats().getCommWriteBufferSizeTracker().longValue();
        }
        return sum;
    }
}
