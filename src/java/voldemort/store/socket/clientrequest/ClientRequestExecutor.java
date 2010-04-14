/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.niosocket.AsyncRequestHandler;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.utils.ByteBufferBackedOutputStream;
import voldemort.utils.ByteUtils;

/**
 * ClientRequestExecutor represents a persistent link between a client and
 * server and is used by the {@link ClientSelectorManager} to execute
 * {@link ClientRequest requests} for the client.
 * 
 * Instances are maintained in a pool by {@link ClientRequestExecutorPool} using
 * a checkout/checkin pattern. When an instance is checked out, the calling code
 * has exclusive access to that instance. Then the
 * {@link #setClientRequest(ClientRequest) request can be set} and the instance
 * can be {@link ClientSelectorManager#submitRequest(ClientRequestExecutor)
 * submitted} to be executed.
 * 
 * @see AsyncRequestHandler
 * @see ClientSelectorManager
 */

public class ClientRequestExecutor implements Runnable {

    private final Selector selector;

    private final SocketChannel socketChannel;

    private final int socketBufferSize;

    private final int resizeThreshold;

    private final ByteBufferBackedInputStream inputStream;

    private final ByteBufferBackedOutputStream outputStream;

    private final long createTimestamp;

    private ClientRequest<?> clientRequest;

    private final Logger logger = Logger.getLogger(getClass());

    public ClientRequestExecutor(Selector selector,
                                 SocketChannel socketChannel,
                                 int socketBufferSize) {
        this.selector = selector;
        this.socketChannel = socketChannel;
        this.socketBufferSize = socketBufferSize;
        this.resizeThreshold = socketBufferSize * 2; // This is arbitrary...

        this.inputStream = new ByteBufferBackedInputStream(ByteBuffer.allocate(socketBufferSize));
        this.outputStream = new ByteBufferBackedOutputStream(ByteBuffer.allocate(socketBufferSize));
        this.createTimestamp = System.nanoTime();
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * Returns the nanosecond-based timestamp of when this socket was created.
     * 
     * @return Nanosecond-based timestamp of socket creation
     * 
     * @see ClientRequestExecutorFactory#validate(SocketDestination,
     *      ClientRequestExecutor)
     */

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public boolean isValid() {
        Socket s = socketChannel.socket();
        return !s.isClosed() && s.isBound() && s.isConnected();
    }

    public void addClientRequest(ClientRequest<?> clientRequest) {
        this.clientRequest = clientRequest;

        if(logger.isTraceEnabled())
            traceInputBufferState("About to clear read buffer");

        if(inputStream.getBuffer().capacity() >= resizeThreshold)
            inputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
        else
            inputStream.getBuffer().clear();

        if(logger.isTraceEnabled())
            traceInputBufferState("Cleared read buffer");

        outputStream.getBuffer().clear();

        boolean wasSuccessful = clientRequest.formatRequest(new DataOutputStream(outputStream));

        outputStream.getBuffer().flip();

        if(wasSuccessful) {
            SelectionKey selectionKey = socketChannel.keyFor(selector);

            if(selectionKey != null) {
                selectionKey.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        } else {
            clientRequest.complete();
        }
    }

    public void run() {
        try {
            SelectionKey selectionKey = socketChannel.keyFor(selector);

            if(selectionKey.isReadable())
                read(selectionKey);
            else if(selectionKey.isWritable())
                write(selectionKey);
            else if(!selectionKey.isValid())
                throw new IllegalStateException("Selection key not valid for "
                                                + socketChannel.socket().getRemoteSocketAddress());
            else
                throw new IllegalStateException("Unknown state, not readable, writable, or valid for "
                                                + socketChannel.socket().getRemoteSocketAddress());
        } catch(ClosedByInterruptException e) {
            close();
        } catch(CancelledKeyException e) {
            close();
        } catch(EOFException e) {
            close();
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);

            close();
        }
    }

    private void read(SelectionKey selectionKey) throws IOException {
        int count = 0;

        if((count = socketChannel.read(inputStream.getBuffer())) == -1)
            throw new EOFException("EOF for " + socketChannel.socket().getRemoteSocketAddress());

        if(logger.isTraceEnabled())
            traceInputBufferState("Read " + count + " bytes");

        if(count == 0)
            return;

        // Take note of the position after we read the bytes. We'll need it in
        // case of incomplete reads later on down the method.
        final int position = inputStream.getBuffer().position();

        // Flip the buffer, set our limit to the current position and then set
        // the position to 0 in preparation for reading in the RequestHandler.
        inputStream.getBuffer().flip();

        if(!clientRequest.isCompleteResponse(inputStream.getBuffer())) {
            // Ouch - we're missing some data for a full request, so handle that
            // and return.
            handleIncompleteRequest(position);
            return;
        }

        // At this point we have the full request (and it's not streaming), so
        // rewind the buffer for reading and execute the request.
        inputStream.getBuffer().rewind();

        if(logger.isTraceEnabled())
            logger.trace("Starting read for " + socketChannel.socket().getRemoteSocketAddress());

        clientRequest.parseResponse(new DataInputStream(inputStream));

        // At this point we've completed a full stand-alone request. So clear
        // our input buffer and prepare for outputting back to the client.
        if(logger.isTraceEnabled())
            logger.trace("Finished read for " + socketChannel.socket().getRemoteSocketAddress());

        selectionKey.interestOps(0);
        clientRequest.complete();
    }

    private void write(SelectionKey selectionKey) throws IOException {
        if(outputStream.getBuffer().hasRemaining()) {
            // If we have data, write what we can now...
            int count = socketChannel.write(outputStream.getBuffer());

            if(logger.isTraceEnabled())
                logger.trace("Wrote " + count + " bytes, remaining: "
                             + outputStream.getBuffer().remaining() + " for "
                             + socketChannel.socket().getRemoteSocketAddress());
        } else {
            if(logger.isTraceEnabled())
                logger.trace("Wrote no bytes for "
                             + socketChannel.socket().getRemoteSocketAddress());
        }

        // If there's more to write but we didn't write it, we'll take that to
        // mean that we're done here. We don't clear or reset anything. We leave
        // our buffer state where it is and try our luck next time.
        if(outputStream.getBuffer().hasRemaining())
            return;

        // If we don't have anything else to write, that means we're done with
        // the request! So clear the buffers (resizing if necessary).
        if(outputStream.getBuffer().capacity() >= resizeThreshold)
            outputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
        else
            outputStream.getBuffer().clear();

        // If we're not streaming writes, signal the Selector that we're
        // ready to read the next request.
        selectionKey.interestOps(SelectionKey.OP_READ);
    }

    private void handleIncompleteRequest(int newPosition) {
        if(logger.isTraceEnabled())
            traceInputBufferState("Incomplete read request detected, before update");

        inputStream.getBuffer().position(newPosition);
        inputStream.getBuffer().limit(inputStream.getBuffer().capacity());

        if(logger.isTraceEnabled())
            traceInputBufferState("Incomplete read request detected, after update");

        if(!inputStream.getBuffer().hasRemaining()) {
            // We haven't read all the data needed for the request AND we
            // don't have enough data in our buffer. So expand it. Note:
            // doubling the current buffer size is arbitrary.
            inputStream.setBuffer(ByteUtils.expand(inputStream.getBuffer(),
                                                   inputStream.getBuffer().capacity() * 2));

            if(logger.isTraceEnabled())
                traceInputBufferState("Expanded input buffer");
        }
    }

    public void close() {
        SelectionKey selectionKey = socketChannel.keyFor(selector);

        if(logger.isInfoEnabled())
            logger.info("Closing remote connection from "
                        + socketChannel.socket().getRemoteSocketAddress());

        try {
            socketChannel.socket().close();
        } catch(IOException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            socketChannel.close();
        } catch(IOException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        if(selectionKey != null) {
            try {
                selectionKey.attach(null);
                selectionKey.cancel();
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e.getMessage(), e);
            }
        }

        clientRequest.complete();
    }

    private void traceInputBufferState(String preamble) {
        logger.trace(preamble + " - position: " + inputStream.getBuffer().position() + ", limit: "
                     + inputStream.getBuffer().limit() + ", remaining: "
                     + inputStream.getBuffer().remaining() + ", capacity: "
                     + inputStream.getBuffer().capacity() + " - for "
                     + socketChannel.socket().getRemoteSocketAddress());
    }

}
