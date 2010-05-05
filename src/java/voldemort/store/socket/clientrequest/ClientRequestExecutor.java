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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;

import voldemort.utils.SelectorManagerWorker;

/**
 * ClientRequestExecutor represents a persistent link between a client and
 * server and is used by the {@link ClientRequestExecutorPool} to execute
 * {@link ClientRequest requests} for the client.
 * 
 * Instances are maintained in a pool by {@link ClientRequestExecutorPool} using
 * a checkout/checkin pattern. When an instance is checked out, the calling code
 * has exclusive access to that instance. Then the
 * {@link #addClientRequest(ClientRequest) request can be executed}.
 * 
 * @see SelectorManagerWorker
 * @see ClientRequestExecutorPool
 */

public class ClientRequestExecutor extends SelectorManagerWorker {

    private ClientRequest<?> clientRequest;

    public ClientRequestExecutor(Selector selector,
                                 SocketChannel socketChannel,
                                 int socketBufferSize) {
        super(selector, socketChannel, socketBufferSize);
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public boolean isValid() {
        Socket s = socketChannel.socket();
        return !s.isClosed() && s.isBound() && s.isConnected();
    }

    public synchronized void addClientRequest(ClientRequest<?> clientRequest) {
        if(logger.isTraceEnabled())
            logger.trace("Associating client with "
                         + socketChannel.socket().getRemoteSocketAddress());

        this.clientRequest = clientRequest;
        outputStream.getBuffer().clear();

        boolean wasSuccessful = clientRequest.formatRequest(new DataOutputStream(outputStream));

        if(logger.isTraceEnabled())
            traceInputBufferState("About to clear read buffer");

        if(inputStream.getBuffer().capacity() >= resizeThreshold)
            inputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
        else
            inputStream.getBuffer().clear();

        if(logger.isTraceEnabled())
            traceInputBufferState("Cleared read buffer");

        outputStream.getBuffer().flip();

        if(wasSuccessful) {
            SelectionKey selectionKey = socketChannel.keyFor(selector);

            if(selectionKey != null) {
                selectionKey.interestOps(SelectionKey.OP_WRITE);

                // This wakeup is required because it's invoked by the calling
                // code in a different thread than the SelectorManager.
                selector.wakeup();
            } else {
                if(logger.isDebugEnabled())
                    logger.debug("Client associated with "
                                 + socketChannel.socket().getRemoteSocketAddress()
                                 + " was not registered with Selector, assuming initial protocol negotiation");
            }
        } else {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Client associated with "
                            + socketChannel.socket().getRemoteSocketAddress()
                            + " did not successfully buffer output for request");

            completeClientRequest();
        }
    }

    @Override
    public void close() {
        super.close();
        completeClientRequest();
    }

    @Override
    protected void read(SelectionKey selectionKey) throws IOException {
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
        completeClientRequest();
    }

    @Override
    protected void write(SelectionKey selectionKey) throws IOException {
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

    private synchronized void completeClientRequest() {
        if(clientRequest == null) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("No client associated with "
                            + socketChannel.socket().getRemoteSocketAddress());

            return;
        }

        clientRequest.complete();

        // Don't forget to null out our client request...
        clientRequest = null;

        if(logger.isTraceEnabled())
            logger.trace("Marked client associated with "
                         + socketChannel.socket().getRemoteSocketAddress() + " as complete");
    }

}
