/*
 * Copyright 2010 LinkedIn, Inc.
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

package voldemort.store.socket;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Level;

import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.utils.SelectorManager;

public class ClientSelectorManager extends SelectorManager {

    private final Queue<ClientRequestExecutor> requestQueue;

    public ClientSelectorManager() {
        this.requestQueue = new ConcurrentLinkedQueue<ClientRequestExecutor>();
    }

    public void request(ClientRequestExecutor clientRequestExecutor) {
        if(isClosed.get())
            throw new IllegalStateException("Cannot accept more requests, selector manager closed");

        if(logger.isTraceEnabled())
            logger.trace("Adding request for "
                         + clientRequestExecutor.getSocketChannel()
                                                .socket()
                                                .getRemoteSocketAddress());

        requestQueue.add(clientRequestExecutor);
        selector.wakeup();
    }

    @Override
    protected void processEvents() {
        try {
            ClientRequestExecutor clientRequestExecutor = null;

            while((clientRequestExecutor = requestQueue.poll()) != null) {
                if(isClosed.get()) {
                    if(logger.isInfoEnabled())
                        logger.debug("Closed, exiting");

                    break;
                }

                try {
                    SocketChannel socketChannel = clientRequestExecutor.getSocketChannel();
                    SelectionKey selectionKey = socketChannel.keyFor(selector);

                    if(selectionKey == null) {
                        selectionKey = socketChannel.register(selector,
                                                              SelectionKey.OP_WRITE,
                                                              clientRequestExecutor);

                        if(logger.isTraceEnabled())
                            logger.trace("Registering "
                                         + socketChannel.socket().getRemoteSocketAddress()
                                         + " with selector");
                    }

                    clientRequestExecutor.reset(selector);
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
