/*
 * Copyright 2009 Mustard Grain, Inc.
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
import voldemort.server.AbstractSocketService;
import voldemort.server.ServiceType;
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
 * @author Kirk True
 * 
 * @see voldemort.server.socket.SocketService
 */

public class NioSocketService extends AbstractSocketService {

    private final RequestHandlerFactory requestHandlerFactory;

    private ServerSocketChannel serverSocketChannel;

    private final InetSocketAddress endpoint;

    private final SelectorManager[] selectorManagers;

    private final ExecutorService selectorManagerThreadPool;

    private final int socketBufferSize;

    private final StatusManager statusManager;

    private Thread acceptorThread;

    private final Logger logger = Logger.getLogger(getClass());

    public NioSocketService(RequestHandlerFactory requestHandlerFactory,
                            int port,
                            int socketBufferSize,
                            int selectors,
                            String serviceName,
                            boolean enableJmx) {
        super(ServiceType.SOCKET, port, serviceName, enableJmx);
        this.requestHandlerFactory = requestHandlerFactory;
        this.socketBufferSize = socketBufferSize;

        try {
            this.serverSocketChannel = ServerSocketChannel.open();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        this.endpoint = new InetSocketAddress(port);

        this.selectorManagers = new SelectorManager[selectors];
        this.selectorManagerThreadPool = Executors.newFixedThreadPool(selectorManagers.length,
                                                                      new DaemonThreadFactory("voldemort-niosocket-server"));
        this.statusManager = new StatusManager((ThreadPoolExecutor) this.selectorManagerThreadPool);
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
                selectorManagers[i] = new SelectorManager(endpoint,
                                                          requestHandlerFactory,
                                                          socketBufferSize);
                selectorManagerThreadPool.execute(selectorManagers[i]);
            }

            serverSocketChannel.socket().bind(endpoint);
            serverSocketChannel.socket().setReceiveBufferSize(socketBufferSize);
            serverSocketChannel.socket().setReuseAddress(true);

            acceptorThread = new Thread(new Acceptor());
            acceptorThread.start();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(e.getMessage(), e);

            throw new VoldemortException(e);
        }

        enableJmx(this);
    }

    @Override
    protected void stopInner() {
        if(logger.isEnabledFor(Level.INFO))
            logger.info("NIO socket service shutting down");

        try {
            // Signal the thread to stop accepting new connections...
            acceptorThread.interrupt();
            acceptorThread.join(15000);

            if(acceptorThread.isAlive()) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Acceptor thread pool did not stop");
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            for(int i = 0; i < selectorManagers.length; i++) {
                try {
                    selectorManagers[i].close();
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }

            selectorManagerThreadPool.shutdown();
            boolean terminated = selectorManagerThreadPool.awaitTermination(15, TimeUnit.SECONDS);

            if(!terminated) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Selector manager thread pool terminated abnormally");
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
                logger.info("Server now listening for connections");

            AtomicInteger counter = new AtomicInteger();

            while(true) {
                if(Thread.currentThread().isInterrupted()) {
                    if(logger.isInfoEnabled())
                        logger.info("Thread interrupted");

                    break;
                }

                try {
                    SocketChannel socketChannel = serverSocketChannel.accept();

                    if(socketChannel == null) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn("Claimed accept but nothing to select");

                        continue;
                    }

                    SelectorManager selectorManager = selectorManagers[counter.getAndIncrement()
                                                                       % selectorManagers.length];
                    selectorManager.accept(socketChannel);
                } catch(ClosedByInterruptException e) {
                    // If you're *really* interested...
                    if(logger.isTraceEnabled())
                        logger.trace("Interrupted, closing");

                    break;
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }

            if(logger.isInfoEnabled())
                logger.info("Server has stopped listening for connections");
        }

    }

}
