/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.common.nio.SelectorManagerWorker;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestDirection;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestHandlerState;
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
 * 
 * @see voldemort.server.protocol.RequestHandler
 */

public class AsyncRequestHandler extends SelectorManagerWorker {

    private final RequestHandlerFactory requestHandlerFactory;

    private RequestHandler requestHandler;

    private StreamRequestHandler streamRequestHandler;

    private NioSelectorManagerStats nioStats;

    public AsyncRequestHandler(Selector selector,
                               SocketChannel socketChannel,
                               RequestHandlerFactory requestHandlerFactory,
                               int socketBufferSize,
                               NioSelectorManagerStats nioStats) {
        super(selector, socketChannel, socketBufferSize, nioStats.getServerCommBufferStats());
        this.requestHandlerFactory = requestHandlerFactory;
        this.nioStats = nioStats;
    }

    @Override
    protected void read(SelectionKey selectionKey) throws IOException {
        int count = 0;

        long startNs = -1;

        if(logger.isDebugEnabled())
            startNs = System.nanoTime();

        if((count = socketChannel.read(inputStream.getBuffer())) == -1)
            throw new EOFException("EOF for " + socketChannel.socket());

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

        // We have to do this on the first request as we don't know the protocol
        // yet.
        if(requestHandler == null) {
            if(!initRequestHandler(selectionKey)) {
                return;
            }
        }

        if(streamRequestHandler != null) {
            // We're continuing an existing streaming request from our last pass
            // through. So handle it and return.
            handleStreamRequest(selectionKey);
            return;
        }

        if(!requestHandler.isCompleteRequest(inputStream.getBuffer())) {
            // Ouch - we're missing some data for a full request, so handle that
            // and return.
            handleIncompleteRequest(position);
            return;
        }

        // At this point we have the full request (and it's not streaming), so
        // rewind the buffer for reading and execute the request.
        inputStream.getBuffer().rewind();

        if(logger.isTraceEnabled())
            logger.trace("Starting execution for " + socketChannel.socket());

        DataInputStream dataInputStream = new DataInputStream(inputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        streamRequestHandler = requestHandler.handleRequest(dataInputStream, dataOutputStream);

        if(logger.isDebugEnabled()) {
            logger.debug("AsyncRequestHandler:read finished request from "
                         + socketChannel.socket().getRemoteSocketAddress() + " handlerRef: "
                         + System.identityHashCode(dataInputStream) + " at time: "
                         + System.currentTimeMillis() + " elapsed time: "
                         + (System.nanoTime() - startNs) + " ns");
        }

        if(streamRequestHandler != null) {
            // In the case of a StreamRequestHandler, we handle that separately
            // (attempting to process multiple "segments").
            handleStreamRequest(selectionKey);
            return;
        }

        // At this point we've completed a full stand-alone request. So clear
        // our input buffer and prepare for outputting back to the client.
        if(logger.isTraceEnabled())
            logger.trace("Finished execution for " + socketChannel.socket());

        prepForWrite(selectionKey);
    }

    @Override
    protected void write(SelectionKey selectionKey) throws IOException {
        if(outputStream.getBuffer().hasRemaining()) {
            // If we have data, write what we can now...
            try {
                int count = socketChannel.write(outputStream.getBuffer());

                if(logger.isTraceEnabled())
                    logger.trace("Wrote " + count + " bytes, remaining: "
                                 + outputStream.getBuffer().remaining() + " for "
                                 + socketChannel.socket());
            } catch(IOException e) {
                if(streamRequestHandler != null) {
                    streamRequestHandler.close(new DataOutputStream(outputStream));
                    streamRequestHandler = null;
                }

                throw e;
            }

        } else {
            if(logger.isTraceEnabled())
                logger.trace("Wrote no bytes for " + socketChannel.socket());
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

        if(streamRequestHandler != null
           && streamRequestHandler.getDirection() == StreamRequestDirection.WRITING) {
            // In the case of streaming writes, it's possible we can process
            // another segment of the stream. We process streaming writes this
            // way because there won't be any other notification for us to do
            // work as we won't be notified via reads.
            if(logger.isTraceEnabled())
                logger.trace("Request is streaming for " + socketChannel.socket());

            handleStreamRequest(selectionKey);
        } else {
            // If we're not streaming writes, signal the Selector that we're
            // ready to read the next request.
            selectionKey.interestOps(SelectionKey.OP_READ);
        }
    }

    private void handleStreamRequest(SelectionKey selectionKey) throws IOException {
        // You are not expected to understand this.
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        // We need to keep track of the last known starting index *before* we
        // attempt to service the next segment. This is needed in case of
        // partial reads so that we can revert back to this point.
        int preRequestPosition = inputStream.getBuffer().position();

        StreamRequestHandlerState state = handleStreamRequestInternal(selectionKey,
                                                                      dataInputStream,
                                                                      dataOutputStream);

        if(state == StreamRequestHandlerState.READING) {
            // We've read our request and handled one segment, but we aren't
            // ready to write anything just yet as we're streaming reads from
            // the client. So let's keep executing segments as much as we can
            // until we're no longer reading anything.
            do {
                preRequestPosition = inputStream.getBuffer().position();
                state = handleStreamRequestInternal(selectionKey, dataInputStream, dataOutputStream);
            } while(state == StreamRequestHandlerState.READING);
        } else if(state == StreamRequestHandlerState.WRITING) {
            // We've read our request and written one segment, but we're still
            // ready to stream writes to the client. So let's keep executing
            // segments as much as we can until we're there's nothing more to do
            // or until we blow past our buffer.
            do {
                state = handleStreamRequestInternal(selectionKey, dataInputStream, dataOutputStream);
            } while(state == StreamRequestHandlerState.WRITING && !outputStream.wasExpanded());

            if(state != StreamRequestHandlerState.COMPLETE) {
                // We've read our request and are ready to start streaming
                // writes to the client.
                prepForWrite(selectionKey);
            }
        }

        if(state == null) {
            // We got an error...
            return;
        }

        if(state == StreamRequestHandlerState.INCOMPLETE_READ) {
            // We need the data that's in there so far and aren't ready to write
            // anything out yet, so don't clear the input buffer or signal that
            // we're ready to write. But we do want to compact the buffer as we
            // don't want it to trigger an increase in the buffer if we don't
            // need to do so.

            // We need to do the following steps...
            //
            // a) ...figure out where we are in the buffer...
            int currentPosition = inputStream.getBuffer().position();

            // b) ...position ourselves at the start of the incomplete
            // "segment"...
            inputStream.getBuffer().position(preRequestPosition);

            // c) ...then copy the data starting from preRequestPosition's data
            // is at index 0...
            inputStream.getBuffer().compact();

            // d) ...and reset the position to be ready for the rest of the
            // reads and the limit to allow more data.
            handleIncompleteRequest(currentPosition - preRequestPosition);
        } else if(state == StreamRequestHandlerState.COMPLETE) {
            streamRequestHandler.close(dataOutputStream);
            streamRequestHandler = null;

            // Treat this as a normal request. Assume that all completed
            // requests want to write something back to the client.
            prepForWrite(selectionKey);
        }
    }

    private StreamRequestHandlerState handleStreamRequestInternal(SelectionKey selectionKey,
                                                                  DataInputStream dataInputStream,
                                                                  DataOutputStream dataOutputStream)
            throws IOException {
        StreamRequestHandlerState state = null;

        try {
            if(logger.isTraceEnabled())
                traceInputBufferState("Before streaming request handler");

            // this is the lowest level in the NioSocketServer stack at which we
            // still have a reference to the client IP address and port
            long startNs = -1;

            if(logger.isDebugEnabled())
                startNs = System.nanoTime();

            state = streamRequestHandler.handleRequest(dataInputStream, dataOutputStream);

            if(logger.isDebugEnabled()) {
                logger.debug("Handled request from "
                             + socketChannel.socket().getRemoteSocketAddress() + " handlerRef: "
                             + System.identityHashCode(dataInputStream) + " at time: "
                             + System.currentTimeMillis() + " elapsed time: "
                             + (System.nanoTime() - startNs) + " ns");
            }

            if(logger.isTraceEnabled())
                traceInputBufferState("After streaming request handler");
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);

            VoldemortException error = e instanceof VoldemortException ? (VoldemortException) e
                                                                      : new VoldemortException(e);
            streamRequestHandler.handleError(dataOutputStream, error);
            streamRequestHandler.close(dataOutputStream);
            streamRequestHandler = null;

            prepForWrite(selectionKey);

            close();
        }

        return state;
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

            if(logger.isInfoEnabled())
                logger.info("Protocol negotiated for " + socketChannel.socket() + ": "
                            + requestFormatType.getDisplayName());

            // The protocol negotiation is the first request, so respond by
            // sticking the bytes in the output buffer, signaling the Selector,
            // and returning false to denote no further processing is needed.
            outputStream.getBuffer().put(ByteUtils.getBytes("ok", "UTF-8"));
            prepForWrite(selectionKey);

            return false;
        } catch(IllegalArgumentException e) {
            // okay we got some nonsense. For backwards compatibility,
            // assume this is an old client who does not know how to negotiate
            RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT_V0;
            requestHandler = requestHandlerFactory.getRequestHandler(requestFormatType);

            if(logger.isInfoEnabled())
                logger.info("No protocol proposal given for " + socketChannel.socket()
                            + ", assuming " + requestFormatType.getDisplayName());

            return true;
        }
    }

    @Override
    public void close() {
        if(!isClosed.compareAndSet(false, true))
            return;

        nioStats.removeConnection();
        closeInternal();
    }
}
