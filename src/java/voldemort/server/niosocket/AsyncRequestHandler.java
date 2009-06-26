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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.utils.ByteBufferBackedOutputStream;
import voldemort.utils.ByteUtils;

/**
 * AsyncRequestHandler manages a Selector, SocketChannel, and RequestHandler
 * implementation. At the point that the run method is invoked, the Selector
 * with which the (socket) Channel has been registered has notified us that the
 * socket has data to read or write.
 * <p/>
 * The bulk of the complexity in this class surrounds partial reads and writes,
 * as well as determining when all the data needed for the request has been
 * read.
 * 
 * @author Kirk True
 * 
 * @see voldemort.server.protocol.RequestHandler
 */

public class AsyncRequestHandler implements Runnable {

    private final Selector selector;

    private final SocketChannel socketChannel;

    private final RequestHandlerFactory requestHandlerFactory;

    private final int socketBufferSize;

    private final int resizeThreshold;

    private final ByteBufferBackedInputStream inputStream;

    private final ByteBufferBackedOutputStream outputStream;

    private RequestHandler requestHandler;

    private final Logger logger = Logger.getLogger(getClass());

    public AsyncRequestHandler(Selector selector,
                               SocketChannel socketChannel,
                               RequestHandlerFactory requestHandlerFactory,
                               int socketBufferSize) {
        this.selector = selector;
        this.socketChannel = socketChannel;
        this.requestHandlerFactory = requestHandlerFactory;
        this.socketBufferSize = socketBufferSize;
        this.resizeThreshold = socketBufferSize * 2;

        inputStream = new ByteBufferBackedInputStream(ByteBuffer.allocate(socketBufferSize));
        outputStream = new ByteBufferBackedOutputStream(ByteBuffer.allocate(socketBufferSize));

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
        } catch(ClosedByInterruptException e) {
            close(selectionKey);
        } catch(CancelledKeyException e) {
            close(selectionKey);
        } catch(EOFException e) {
            close(selectionKey);
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
        }
    }

    int getPort() {
        return socketChannel.socket().getPort();
    }

    private void read(SelectionKey selectionKey) throws IOException {
        int count = 0;

        ByteBuffer inputBuffer = inputStream.getBuffer();

        if((count = socketChannel.read(inputBuffer)) == -1)
            throw new EOFException();

        if(logger.isTraceEnabled())
            logger.trace("Read " + count + "bytes, total: " + inputBuffer.position() + " for "
                         + socketChannel.socket().getRemoteSocketAddress());

        if(count == 0) {
            if(logger.isDebugEnabled())
                logger.debug("Read of zero bytes for "
                             + socketChannel.socket().getRemoteSocketAddress());

            return;
        }

        final int position = inputBuffer.position();

        inputBuffer.flip();

        // We have to do this on the first request.
        if(requestHandler == null) {
            if(!initRequestHandler(selectionKey)) {
                return;
            }
        }

        if(requestHandler != null && requestHandler.isCompleteRequest(inputBuffer)) {
            // If we have the full request, flip the buffer for reading
            // and execute the request
            inputBuffer.rewind();

            if(logger.isDebugEnabled())
                logger.debug("Starting execution for "
                             + socketChannel.socket().getRemoteSocketAddress());

            requestHandler.handleRequest(new DataInputStream(inputStream),
                                         new DataOutputStream(outputStream));

            if(logger.isDebugEnabled())
                logger.debug("Finished execution for "
                             + socketChannel.socket().getRemoteSocketAddress());

            // We've written to the buffer in the handleRequest invocation, so
            // we're done with the input and can reset/resize, flip the output
            // buffer, and let the Selector know we're ready to write.
            if(inputBuffer.capacity() >= resizeThreshold) {
                inputBuffer = ByteBuffer.allocate(socketBufferSize);
                inputStream.setBuffer(inputBuffer);
            } else {
                inputBuffer.clear();
            }

            outputStream.getBuffer().flip();
            selectionKey.interestOps(SelectionKey.OP_WRITE);
        } else {
            inputBuffer.position(position);
            inputBuffer.limit(inputBuffer.capacity());

            if(!inputBuffer.hasRemaining()) {
                // We haven't read all the data needed for the request AND we
                // don't have enough data in our buffer. So expand it. Note:
                // doubling the current buffer size is arbitrary.
                inputBuffer = ByteUtils.expand(inputBuffer, inputBuffer.capacity() * 2);
                inputStream.setBuffer(inputBuffer);
            }
        }
    }

    private void write(SelectionKey selectionKey) throws IOException {
        ByteBuffer outputBuffer = outputStream.getBuffer();

        // Write what we can now...
        int count = socketChannel.write(outputBuffer);

        if(logger.isTraceEnabled())
            logger.trace("Wrote " + count + "bytes, remaining: " + outputBuffer.remaining()
                         + " for " + socketChannel.socket().getRemoteSocketAddress());

        if(!outputBuffer.hasRemaining()) {
            // If we don't have anything else to write, that means we're done
            // with the request! So clear the buffers (resizing if necessary)
            // and signal the Selector that we're ready to take the next
            // request.
            if(outputBuffer.capacity() >= resizeThreshold) {
                outputBuffer = ByteBuffer.allocate(socketBufferSize);
                outputStream.setBuffer(outputBuffer);
            } else {
                outputBuffer.clear();
            }

            selectionKey.interestOps(SelectionKey.OP_READ);
        }
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

        try {
            selectionKey.attach(null);
            selectionKey.cancel();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }
    }

    /**
     * Returns true if the request should continue.
     * 
     * @return
     */

    private boolean initRequestHandler(SelectionKey selectionKey) {
        ByteBuffer inputBuffer = inputStream.getBuffer();
        int remaining = inputBuffer.remaining();

        // Don't have enough bytes to determine the protocol yet...
        if(remaining < 3)
            return true;

        byte[] protoBytes = { inputBuffer.get(0), inputBuffer.get(1), inputBuffer.get(2) };

        try {
            String proto = ByteUtils.getString(protoBytes, "UTF-8");
            RequestFormatType requestFormatType = RequestFormatType.fromCode(proto);
            requestHandler = requestHandlerFactory.getRequestHandler(requestFormatType);

            // The protocol negotiation is a meta request, so respond by
            // sticking the bytes in the output buffer, signaling the Selector,
            // and returning false to denote no further processing is needed.
            outputStream.getBuffer().put(ByteUtils.getBytes("ok", "UTF-8"));
            outputStream.getBuffer().flip();
            selectionKey.interestOps(SelectionKey.OP_WRITE);

            return false;
        } catch(IllegalArgumentException e) {
            // okay we got some nonsense. For backwards compatibility,
            // assume this is an old client who does not know how to negotiate
            RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT_V0;
            requestHandler = requestHandlerFactory.getRequestHandler(requestFormatType);

            if(logger.isInfoEnabled())
                logger.info("No protocol proposal given, assuming "
                            + requestFormatType.getDisplayName());

            return true;
        }
    }

}
