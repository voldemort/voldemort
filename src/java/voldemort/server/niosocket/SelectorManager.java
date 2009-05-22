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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.Executor;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.protocol.RequestHandler;

public class SelectorManager implements Runnable {

    private final Selector selector;

    private final Executor threadPool;

    private final RequestHandler requestHandler;

    private final int socketBufferSize;

    private final Logger logger = Logger.getLogger(getClass());

    public SelectorManager(Selector selector,
                           Executor threadPool,
                           RequestHandler requestHandler,
                           int socketBufferSize) {
        this.selector = selector;
        this.threadPool = threadPool;
        this.requestHandler = requestHandler;
        this.socketBufferSize = socketBufferSize;
    }

    public void run() {
        try {
            while(true) {
                if(Thread.currentThread().isInterrupted()) {
                    if(logger.isInfoEnabled())
                        logger.info("Interrupted");

                    break;
                }

                try {
                    int selected = selector.select();

                    if(selected > 0) {
                        Iterator<SelectionKey> i = selector.selectedKeys().iterator();

                        while(i.hasNext()) {
                            SelectionKey selectionKey = i.next();

                            try {
                                if(selectionKey.isAcceptable()) {
                                    accept(selectionKey);
                                } else if(selectionKey.isReadable()) {
                                    read(selectionKey);
                                }
                            } catch(Exception e) {
                                if(logger.isEnabledFor(Level.ERROR))
                                    logger.error(e.getMessage(), e);
                            }

                            i.remove();
                        }
                    }
                } catch(Throwable t) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(t.getMessage(), t);
                }
            }
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
        } finally {
            if(logger.isInfoEnabled())
                logger.info("Complete");
        }
    }

    private void accept(SelectionKey selectionKey) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();

        if(socketChannel == null) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Claimed accept readiness but nothing to select");

            return;
        }

        if(logger.isInfoEnabled())
            logger.info("Accepting remote connection from " + socketChannel.socket().getPort());

        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setSendBufferSize(socketBufferSize);

        if(socketChannel.socket().getReceiveBufferSize() != this.socketBufferSize)
            if(logger.isDebugEnabled())
                logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                             + " bytes but actual size is "
                             + socketChannel.socket().getReceiveBufferSize() + " bytes.");

        if(socketChannel.socket().getSendBufferSize() != this.socketBufferSize)
            if(logger.isDebugEnabled())
                logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                             + " bytes but actual size is "
                             + socketChannel.socket().getSendBufferSize() + " bytes.");

        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey selectionKey) {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        if(socketChannel == null) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Claimed read readiness but nothing to select");

            return;
        }

        threadPool.execute(new AsyncRequestHandler(selectionKey, requestHandler));
    }

}
