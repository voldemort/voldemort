/*
 * Copyright 2009 Mustard Grain, Inc
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
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.protocol.RequestHandler;

public class SelectorManager implements Runnable {

    private final Selector selector;

    private final BlockingQueue<SocketChannel> socketChannelQueue;

    private final RequestHandler requestHandler;

    private final int socketBufferSize;

    private final Logger logger = Logger.getLogger(getClass());

    public SelectorManager(RequestHandler requestHandler, int socketBufferSize) throws IOException {
        this.selector = Selector.open();
        this.socketChannelQueue = new LinkedBlockingDeque<SocketChannel>();
        this.requestHandler = requestHandler;
        this.socketBufferSize = socketBufferSize;
    }

    public void accept(SocketChannel socketChannel) {
        socketChannelQueue.add(socketChannel);
        selector.wakeup();
    }

    public void run() {
        try {
            while(true) {
                if(Thread.currentThread().isInterrupted()) {
                    if(logger.isDebugEnabled())
                        logger.debug("Interrupted");

                    break;
                }

                processSockets();

                try {
                    int selected = selector.select();

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
                selector.close();
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
                                                                             requestHandler,
                                                                             socketBufferSize);
                    socketChannel.register(selector, SelectionKey.OP_READ, attachment);
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
