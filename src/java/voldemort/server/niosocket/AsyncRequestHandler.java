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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.protocol.RequestHandler;

/**
 * AsyncRequestHandler manages a Selector, SocketChannel, and RequestHandler
 * implementation. At the point that the run method is invoked, the Selector
 * with which the (socket) Channel has been registered has notified us that the
 * socket has data to read. The implementation basically wraps the Channel in
 * InputStream and OutputStream objects required by the RequestHandler
 * interface.
 * 
 * @author Kirk True
 * 
 * @see voldemort.server.protocol.RequestHandler
 */

public class AsyncRequestHandler implements Runnable {

    private final Selector selector;

    private final SocketChannel socketChannel;

    private final RequestHandler requestHandler;

    private ByteBuffer inputBuffer;

    private ByteBuffer outputBuffer;

    private final DataInputStream inputStream;

    private final DataOutputStream outputStream;

    private final Logger logger = Logger.getLogger(getClass());

    public AsyncRequestHandler(Selector selector,
                               SocketChannel socketChannel,
                               RequestHandler requestHandler,
                               int socketBufferSize) {
        this.selector = selector;
        this.socketChannel = socketChannel;
        this.requestHandler = requestHandler;

        inputBuffer = ByteBuffer.allocateDirect(socketBufferSize);
        outputBuffer = ByteBuffer.allocateDirect(socketBufferSize);

        inputStream = new DataInputStream(new ByteBufferBackedInputStream());
        outputStream = new DataOutputStream(new ByteBufferBackedOutputStream());

        if(logger.isInfoEnabled())
            logger.info("Accepting remote connection from "
                        + socketChannel.socket().getRemoteSocketAddress());
    }

    public void run() {
        SelectionKey selectionKey = socketChannel.keyFor(selector);

        try {
            if(selectionKey.isReadable()) {
                read(selectionKey);
            } else if(selectionKey.isWritable()) {
                write(selectionKey);
            } else if(!selectionKey.isValid()) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Selection key not valid");

                close(selectionKey);
            } else {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Unknown state, not readable, writable, or valid...");
            }
        } catch(EOFException e) {
            close(selectionKey);
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
        }
    }

    private void read(SelectionKey selectionKey) throws IOException {
        int count = 0;

        if((count = socketChannel.read(inputBuffer)) == -1)
            throw new EOFException();

        if(count == 0) {
            if(logger.isDebugEnabled())
                logger.debug("Read of zero bytes for "
                             + socketChannel.socket().getRemoteSocketAddress());

            return;
        }

        final int position = inputBuffer.position();

        inputBuffer.flip();
        boolean isCompleteRequest = requestHandler.isCompleteRequest(inputBuffer);

        if(isCompleteRequest) {
            // If we have the full request, flip the buffer for reading
            // and execute the request
            inputBuffer.rewind();

            if(logger.isDebugEnabled())
                logger.debug("Starting execution for "
                             + socketChannel.socket().getRemoteSocketAddress());

            requestHandler.handleRequest(inputStream, outputStream);

            if(logger.isDebugEnabled())
                logger.debug("Finished execution for "
                             + socketChannel.socket().getRemoteSocketAddress());

            // We're done with the input buffer, so clear it out for the
            // next request...
            inputBuffer.clear();

            // We've written to the buffer in the handleRequest
            // invocation, so now flip the output buffer.
            outputBuffer.flip();

            // We've got our output buffer ready, so let's let the
            // Selector know.
            selectionKey.interestOps(SelectionKey.OP_WRITE);
        } else {
            inputBuffer.position(position);
            inputBuffer.limit(inputBuffer.capacity());

            if(!inputBuffer.hasRemaining())
                inputBuffer = expand(inputBuffer, inputBuffer.capacity() * 2);
        }
    }

    private void write(SelectionKey selectionKey) throws IOException {
        // Write what we can now...
        socketChannel.write(outputBuffer);

        if(!outputBuffer.hasRemaining()) {
            // If we don't have anything else to write, that means we're done
            // with the request! So clear the output buffer so that we're ready
            // for the next one.
            outputBuffer.clear();

            // The request is (finally) done, so let's signal the
            // Selector that we're ready to take the next request.
            selectionKey.interestOps(SelectionKey.OP_READ);
        }
    }

    int getPort() {
        return socketChannel.socket().getPort();
    }

    private void close(SelectionKey selectionKey) {
        if(logger.isInfoEnabled())
            logger.info("Closing remote connection from "
                        + socketChannel.socket().getRemoteSocketAddress());

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

        selectionKey.attach(null);
        selectionKey.cancel();
    }

    /**
     * If we have no more room in the current buffer, then double our capacity
     * and copy the current buffer to the new one.
     */

    private ByteBuffer expand(ByteBuffer buffer, int newCapacity) {
        int currentCapacity = buffer.capacity();

        if(logger.isTraceEnabled()) {
            String direction = null;

            if(buffer == inputBuffer)
                direction = "reading";
            else if(buffer == outputBuffer)
                direction = "writing";

            logger.trace("The buffer used for " + direction + " data for "
                         + socketChannel.socket().getRemoteSocketAddress()
                         + " requires we increase our internal buffer from " + currentCapacity
                         + " to " + newCapacity);
        }

        ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);

        int position = buffer.position();

        buffer.rewind();
        newBuffer.put(buffer);
        newBuffer.position(position);
        return newBuffer;
    }

    class ByteBufferBackedInputStream extends InputStream {

        @Override
        public int read() throws IOException {
            if(!inputBuffer.hasRemaining())
                return -1;

            return inputBuffer.get();
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            if(!inputBuffer.hasRemaining())
                return -1;

            len = Math.min(len, inputBuffer.remaining());
            inputBuffer.get(bytes, off, len);
            return len;
        }

    }

    class ByteBufferBackedOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            expandIfNeeded(1);
            outputBuffer.put((byte) b);
        }

        @Override
        public void write(byte[] bytes, int off, int len) throws IOException {
            expandIfNeeded(len);
            outputBuffer.put(bytes, off, len);
        }

        private void expandIfNeeded(int len) {
            int need = len - outputBuffer.remaining();

            if(need <= 0)
                return;

            if(logger.isTraceEnabled())
                logger.trace("The output buffer for "
                             + socketChannel.socket().getRemoteSocketAddress()
                             + " has a capacity of " + outputBuffer.capacity() + " bytes with "
                             + outputBuffer.remaining() + " bytes remaining and is about to write "
                             + len + " more bytes");

            int newCapacity = outputBuffer.capacity() + need;
            outputBuffer = expand(outputBuffer, newCapacity);

            if(logger.isTraceEnabled())
                logger.trace("The output buffer for "
                             + socketChannel.socket().getRemoteSocketAddress()
                             + " now has a capacity of " + outputBuffer.capacity() + " bytes with "
                             + outputBuffer.remaining()
                             + " bytes remaining, sufficient for writing " + len + " more bytes");
        }

    }

}
