/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import voldemort.common.nio.CommBufferSizeStats;
import voldemort.common.nio.SelectorManagerWorker;
import voldemort.utils.Time;

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

    private long expiration;
    private boolean isExpired;

    public ClientRequestExecutor(Selector selector,
                                 SocketChannel socketChannel,
                                 int socketBufferSize) {
        // Not tracking or exposing the comm buffer statistics for now
        super(selector, socketChannel, socketBufferSize, new CommBufferSizeStats());
        isExpired = false;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public boolean isValid() {
        if(isClosed())
            return false;

        Socket s = socketChannel.socket();
        return !s.isClosed() && s.isBound() && s.isConnected();
    }

    public synchronized boolean checkTimeout() {
        if(expiration <= 0)
            return true;

        if(System.nanoTime() <= expiration)
            return true;

        if(logger.isEnabledFor(Level.WARN))
            logger.warn("Client request associated with " + socketChannel.socket() + " timed out");

        isExpired = true;
        close();

        return false;
    }

    public synchronized void addClientRequest(ClientRequest<?> clientRequest) {
        addClientRequest(clientRequest, -1);
    }

    public synchronized void addClientRequest(ClientRequest<?> clientRequest, long timeoutMs) {
        addClientRequest(clientRequest, timeoutMs, 0);
    }

    public synchronized void addClientRequest(ClientRequest<?> clientRequest,
                                              long timeoutMs,
                                              long elapsedNs) {
        if(logger.isTraceEnabled())
            logger.trace("Associating client with " + socketChannel.socket());

        this.clientRequest = clientRequest;

        if(timeoutMs == -1) {
            this.expiration = -1;
        } else {
            if(elapsedNs > (Time.NS_PER_MS * timeoutMs)) {
                this.expiration = System.nanoTime();
            } else {
                this.expiration = System.nanoTime() + (Time.NS_PER_MS * timeoutMs) - elapsedNs;
            }

            if(this.expiration < System.nanoTime())
                throw new IllegalArgumentException("timeout " + timeoutMs + " not valid");
        }

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
                    logger.debug("Client associated with " + socketChannel.socket()
                                 + " was not registered with Selector " + selector
                                 + ", assuming initial protocol negotiation");
            }
        } else {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Client associated with " + socketChannel.socket()
                            + " did not successfully buffer output for request");

            completeClientRequest();
        }
    }

    @Override
    public void close() {
        // Due to certain code paths, close may be called in a recursive
        // fashion. Rather than trying to handle all of the cases, simply keep
        // track of whether we've been called before and only perform the logic
        // once.
        if(!isClosed.compareAndSet(false, true))
            return;

        completeClientRequest();
        closeInternal();
    }

    @Override
    protected void read(SelectionKey selectionKey) throws IOException {
        if(!checkTimeout())
            return;

        int count = 0;

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
            logger.trace("Starting read for " + socketChannel.socket());

        clientRequest.parseResponse(new DataInputStream(inputStream));

        // At this point we've completed a full stand-alone request. So clear
        // our input buffer and prepare for outputting back to the client.
        if(logger.isTraceEnabled())
            logger.trace("Finished read for " + socketChannel.socket());

        selectionKey.interestOps(0);
        completeClientRequest();
    }

    @Override
    protected void write(SelectionKey selectionKey) throws IOException {
        if(!checkTimeout())
            return;

        if(outputStream.getBuffer().hasRemaining()) {
            // If we have data, write what we can now...
            int count = socketChannel.write(outputStream.getBuffer());

            if(logger.isTraceEnabled())
                logger.trace("Wrote " + count + " bytes, remaining: "
                             + outputStream.getBuffer().remaining() + " for "
                             + socketChannel.socket());
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

        // If we're not streaming writes, signal the Selector that we're
        // ready to read the next request.
        selectionKey.interestOps(SelectionKey.OP_READ);
    }

    /**
     * Null out our client request *before* calling complete because of the case
     * where complete will cause a ClientRequestExecutor check-in (in
     * SocketStore.NonblockingStoreCallbackClientRequest) and we'll end up
     * recursing back here again when close is called in which case we'll try to
     * check in the instance again which causes problems for the pool
     * maintenance.
     */
    private synchronized ClientRequest<?> atomicNullOutClientRequest() {
        ClientRequest<?> local = clientRequest;
        clientRequest = null;
        expiration = 0;

        return local;
    }

    /**
     * Null out current clientRequest before calling complete. timeOut and
     * complete must *not* be within a synchronized block since both eventually
     * check in the client request executor. Such a check in can trigger
     * additional synchronized methods deeper in the stack.
     */
    private void completeClientRequest() {
        ClientRequest<?> local = atomicNullOutClientRequest();
        if(local == null) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("No client associated with " + socketChannel.socket());

            return;
        }

        if(isExpired)
            local.timeOut();
        else
            local.complete();

        if(logger.isTraceEnabled())
            logger.trace("Marked client associated with " + socketChannel.socket() + " as complete");
    }

}
