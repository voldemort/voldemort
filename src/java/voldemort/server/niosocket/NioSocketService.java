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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
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

    private final SelectorManager[] selectorManagers;

    private final ExecutorService selectorManagerThreadPool;

    private final Logger logger = Logger.getLogger(getClass());

    public NioSocketService(RequestHandler requestHandler,
                            int port,
                            int socketBufferSize,
                            int selectors) {
        super(ServiceType.SOCKET);
        this.requestHandler = requestHandler;
        this.port = port;
        this.socketBufferSize = socketBufferSize;

        try {
            this.serverSocketChannel = ServerSocketChannel.open();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        this.selectorManagers = new SelectorManager[selectors];
        this.selectorManagerThreadPool = Executors.newFixedThreadPool(selectorManagers.length,
                                                                      new DaemonThreadFactory("voldemort-niosocket-server"));
    }

    @Override
    protected void startInner() {
        try {
            for(int i = 0; i < selectorManagers.length; i++) {
                selectorManagers[i] = new SelectorManager(requestHandler, socketBufferSize);
                selectorManagerThreadPool.execute(selectorManagers[i]);
            }

            InetSocketAddress endpoint = new InetSocketAddress(port);
            serverSocketChannel.socket().bind(endpoint);
            serverSocketChannel.socket().setReceiveBufferSize(socketBufferSize);
            serverSocketChannel.socket().setReuseAddress(true);

            if(logger.isInfoEnabled())
                logger.info("Server now listening for connections on " + endpoint);

            AtomicInteger counter = new AtomicInteger();

            while(!Thread.currentThread().isInterrupted()) {
                SocketChannel socketChannel = serverSocketChannel.accept();

                if(socketChannel == null) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn("Claimed accept but nothing to select");

                    continue;
                }

                SelectorManager selectorManager = selectorManagers[counter.getAndIncrement()
                                                                   % selectorManagers.length];
                selectorManager.accept(socketChannel);
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(e.getMessage(), e);
        }
    }

    @Override
    protected void stopInner() {
        try {
            selectorManagerThreadPool.shutdownNow();
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

    @JmxGetter(name = "port", description = "The port on which the server is accepting connections.")
    public int getPort() {
        return port;
    }

}
