/*
 * Copyright 2009 LinkedIn, Inc
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

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortService;
import voldemort.server.protocol.RequestHandler;
import voldemort.utils.DaemonThreadFactory;

@JmxManaged(description = "A server that handles remote operations on stores via TCP/IP.")
public class NioSocketService extends AbstractService implements VoldemortService {

    private final RequestHandler requestHandler;

    private final int port;

    private final int socketBufferSize;

    private ServerSocketChannel serverSocketChannel;

    private Selector selector;

    private final ExecutorService threadPool;

    private Thread selectorManagerThread;

    private final Logger logger = Logger.getLogger(getClass());

    public NioSocketService(RequestHandler requestHandler,
                            int port,
                            int coreConnections,
                            int maxConnections,
                            int socketBufferSize) {
        super(ServiceType.SOCKET);

        this.requestHandler = requestHandler;
        this.port = port;
        this.socketBufferSize = socketBufferSize;

        RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {

            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                AsyncRequestHandler asyncRequestHandler = (AsyncRequestHandler) r;

                if(logger.isEnabledFor(Level.ERROR))
                    logger.error("Too many requests, " + executor.getActiveCount() + " of "
                                 + executor.getLargestPoolSize()
                                 + " threads in use, denying connection from "
                                 + asyncRequestHandler.getPort());
            }
        };

        ThreadFactory threadFactory = new DaemonThreadFactory("voldemort-niosocket-server");

        this.threadPool = new ThreadPoolExecutor(coreConnections,
                                                 maxConnections,
                                                 0,
                                                 TimeUnit.MILLISECONDS,
                                                 new SynchronousQueue<Runnable>(),
                                                 threadFactory,
                                                 rejectedExecutionHandler);
    }

    @Override
    protected void startInner() {
        try {
            selector = Selector.open();

            InetSocketAddress address = new InetSocketAddress(port);
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(address);
            serverSocketChannel.socket().setReceiveBufferSize(socketBufferSize);

            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            selectorManagerThread = new Thread(new SelectorManager(selector,
                                                                   threadPool,
                                                                   requestHandler,
                                                                   socketBufferSize));
            selectorManagerThread.start();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(e.getMessage(), e);
        }
    }

    @Override
    protected void stopInner() {
        try {
            threadPool.shutdownNow();
            boolean terminated = threadPool.awaitTermination(15, TimeUnit.SECONDS);

            if(!terminated) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Thread pool terminated abnormally");
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Thread pool terminated abnormally", e);
        }

        try {
            selectorManagerThread.interrupt();
            selectorManagerThread.join(15 * 1000);

            if(selectorManagerThread.isAlive()) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Selector manager thread not terminated");
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Selector manager thread error", e);
        }

        try {
            selector.close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        } finally {
            selector = null;
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
        } finally {
            serverSocketChannel = null;
        }
    }

    @JmxGetter(name = "port", description = "The port on which the server is accepting connections.")
    public int getPort() {
        return port;
    }

}
