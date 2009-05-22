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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.protocol.RequestHandler;

/**
 * AsyncRequestHandler manages a SelectionKey and RequestHandler implementation.
 * At the point that the run method is invoked, the Selector with which the
 * (socket) Channel has been registered has notified us that the socket has data
 * to read. The implementation basically wraps the Channel in InputStream and
 * OutputStream objects required by the RequestHandler interface.
 * 
 * @author Kirk True
 * 
 * @see voldemort.server.protocol.RequestHandler
 */

public class AsyncRequestHandler implements Runnable {

    // This buffer size comes from voldemort.server.socket.SocketServerSession.
    // It's hard-coded there too. Perhaps it shouldn't be - I don't know...
    private static final int BUFFER_SIZE = 64000;

    private final SelectionKey selectionKey;

    private final RequestHandler requestHandler;

    private final Logger logger = Logger.getLogger(getClass());

    public AsyncRequestHandler(SelectionKey selectionKey, RequestHandler requestHandler) {
        this.selectionKey = selectionKey;
        this.requestHandler = requestHandler;

        // We disable read interest while we're handling the read in the run
        // method. Otherwise the same channel would receive a 'thundering herd'
        // of requests. We re-enable it once run completes.
        selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_READ);
    }

    int getPort() {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        return socketChannel.socket().getPort();
    }

    public void run() {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new ChannelBackedInputStream(socketChannel),
                                                                                      BUFFER_SIZE));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(new ChannelBackedOutputStream(socketChannel),
                                                                                          64000));

            requestHandler.handleRequest(inputStream, outputStream);
            outputStream.flush();
        } catch(EOFException e) {
            close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(e.getMessage(), e);
        } finally {
            if(selectionKey.isValid()) {
                // In the constructor we disabled read interest, here we
                // re-enable it and wake up the selector in case the socket has
                // more to do.
                selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_READ);
                selectionKey.selector().wakeup();
            }
        }
    }

    private void close() {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        if(logger.isInfoEnabled())
            logger.info("Closing remote connection from " + socketChannel.socket().getPort());

        try {
            socketChannel.socket().close();
        } catch(IOException ex) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(ex, ex);
        }

        try {
            socketChannel.close();
        } catch(IOException ex) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(ex, ex);
        }

        selectionKey.cancel();
    }

}
