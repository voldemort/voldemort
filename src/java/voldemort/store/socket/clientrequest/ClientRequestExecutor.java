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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;

import voldemort.VoldemortApplicationException;
import voldemort.common.nio.ByteBufferBackedInputStream;
import voldemort.common.nio.ByteBufferBackedOutputStream;
import voldemort.common.nio.ByteBufferContainer;
import voldemort.common.nio.CommBufferSizeStats;
import voldemort.common.nio.SelectorManagerWorker;
import voldemort.store.socket.SocketDestination;
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

public class ClientRequestExecutor extends SelectorManagerWorker implements Closeable {

    private ClientRequest<?> clientRequest;

    private long expiration;
    private long startTime;
    private long timeoutMs;
    private boolean isExpired;
    protected ByteBufferContainer bufferContainer;
    private final SocketDestination socketDesination;
    private final long idleConnectionTimeoutNs;

    public ClientRequestExecutor(Selector selector,
                                 SocketChannel socketChannel,
                                 int socketBufferSize,
                                 long idleConnectionTimeoutNs,
                                 SocketDestination socketDesination) {
        // Not tracking or exposing the comm buffer statistics for now
        super(selector, socketChannel, socketBufferSize);
        isExpired = false;
        this.idleConnectionTimeoutNs = idleConnectionTimeoutNs;

        initializeStreams(socketBufferSize, new CommBufferSizeStats());
        if(this.inputStream == null || this.outputStream == null) {
            throw new VoldemortApplicationException("InputStream or OutputStream is null after initialization");
        }
        this.socketDesination = socketDesination;

    }

    @Override
    protected String getDebugInfo() {
        return "Destination: " + String.valueOf(socketDesination) + " , Socket: "
               + String.valueOf(socketChannel.socket());
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    private boolean isIdleConnectionTimeoutExceeded() {
        if (idleConnectionTimeoutNs > 0) {
            long elapsedTime = System.nanoTime() - startTime;
            if (elapsedTime > idleConnectionTimeoutNs) {
                logger.warn("Idle connection " + socketChannel.socket() + " exceeded for destination "
                        + socketDesination + ". Start time(ns) " + startTime + " timeout (ns) "
                        + idleConnectionTimeoutNs + " elapsed time(ns) " + elapsedTime);
                return true;
            }
        }
        return false;
    }

    public boolean isValid() {
        if(isClosed())
            return false;

        Socket s = socketChannel.socket();
        boolean isValidSocket = !s.isClosed() && s.isBound() && s.isConnected();
        if(!isValidSocket) {
            return false;
        }

        if (isIdleConnectionTimeoutExceeded()) {
            return false;
        }
        return true;

    }

    public synchronized boolean checkTimeout() {
        if (expiration <= 0) {
            if (!isIdleConnectionTimeoutExceeded()) {
                return true;
            }
        } else {
            long nowNs = System.nanoTime();
            if (nowNs <= expiration)
                return true;

            if (logger.isEnabledFor(Level.WARN)) {
                long allowedTime = expiration - startTime;
                long elapsedTime = nowNs - startTime;
                logger.warn("Client request associated with " + socketChannel.socket() + " Destination "
                        + socketDesination + " timed out. Start time(ns) " + startTime + " allowed time(ns) "
                        + allowedTime + " elapsed time(ns) " + elapsedTime);
            }
        }


        isExpired = true;
        close();

        return false;
    }

    private void computeExpirationTime(long timeoutMs, long elapsedNs) {
        this.timeoutMs = timeoutMs;
        startTime = System.nanoTime();

        if(elapsedNs > (Time.NS_PER_MS * timeoutMs)) {
            this.expiration = startTime;
        } else {
            this.expiration = startTime + (Time.NS_PER_MS * timeoutMs) - elapsedNs;
        }

        if(this.expiration < startTime) {
            String errorMessage = String.format("Invalid timeout specified. startTime (%d) ns expiration (%d) ns timeout (%d) ms elapsed (%d) ns",
                                                startTime,
                                                expiration,
                                                timeoutMs,
                                                elapsedNs);
            throw new IllegalArgumentException(errorMessage);
        }

    }

    public void setConnectRequest(ClientRequest<?> clientRequest, long timeoutMs) {
        this.clientRequest = clientRequest;
        computeExpirationTime(timeoutMs, 0);
    }

    public synchronized void addClientRequest(ClientRequest<?> clientRequest,
                                              long timeoutMs,
                                              long elapsedNs) {
        if(logger.isTraceEnabled()) {
            logger.trace("Associating client with " + socketChannel.socket());
        }

        this.clientRequest = clientRequest;
        computeExpirationTime(timeoutMs, elapsedNs);
        outputStream.getBuffer().clear();

        boolean wasSuccessful = clientRequest.formatRequest(outputStream);
        outputStream.getBuffer().flip();

        if(wasSuccessful) {
            SelectionKey selectionKey = socketChannel.keyFor(selector);

            if(selectionKey != null) {
                selectionKey.interestOps(SelectionKey.OP_WRITE);

                // This wakeup is required because it's invoked by the calling
                // code in a different thread than the SelectorManager.
                selector.wakeup();
            } else {
                /*
                 * Servers could close the Socket during bounce or other
                 * situations. In those cases the cached client connections are
                 * cleared up as well. But there is a race condition between the
                 * requests getting cached connections and connections getting
                 * cleared up. In those cases a request could get a connection
                 * but before sending request, it could have been invalidated
                 * and removed from the selector. This place handles the case by
                 * sending an IO Error to the request.
                 * 
                 * This case is no different than the request sent to the server
                 * and the server closing the socket.
                 */
                String message = "Client associated with " + socketChannel.socket()
                            + " was not registered with Selector " + selector
                                 + ", it could have been closed due to server restarts ";
                logger.warn(message);
                IOException ex = new IOException(message);
                reportException(ex);
                completeClientRequest();
            }
        } else {
            logger.warn("Client associated with " + socketChannel.socket()
                        + " did not successfully buffer output for request");
            completeClientRequest();
        }
    }

    @Override
    protected void initializeStreams(int socketBufferSize, CommBufferSizeStats commBufferStats) {

        bufferContainer = new ByteBufferContainer(socketBufferSize,
                                                  resizeThreshold,
                                                  commBufferStats.getCommReadBufferSizeTracker());
        this.inputStream = new ByteBufferBackedInputStream(bufferContainer);
        this.outputStream = new ByteBufferBackedOutputStream(bufferContainer);
    }

    private void resetStreams() {
        bufferContainer.reset();
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

        // uses a local variable to point to request to prevent racing condition
        // when atomicNullOutClientRequest() is called by another thread
        ClientRequest<?> request = clientRequest;

        if(request != null) {

            if(!request.isCompleteResponse(inputStream.getBuffer())) {
                // Ouch - we're missing some data for a full request, so handle
                // that and return.
                handleIncompleteRequest(position);
                return;
            }

            // At this point we have the full request (and it's not streaming),
            // so rewind the buffer for reading and execute the request.
            inputStream.getBuffer().rewind();

            if(logger.isTraceEnabled())
                logger.trace("Starting read for " + socketChannel.socket());

            request.parseResponse(new DataInputStream(inputStream));

            // At this point we've completed a full stand-alone request. So
            // clear our input buffer and prepare for outputting back to the
            // client.
            if(logger.isTraceEnabled())
                logger.trace("Finished read for " + socketChannel.socket());

            resetStreams();
            /*
             * Leave the socket with read interest. When node is restarted it
             * closes all the sockets. If there is no interest then FIN packet
             * from the node will be ignored though the connection is dead and
             * must be removed from the cache. Since this connection exists any
             * future operation will receive this dead connection and will fail
             * with EOF on write. When entire cluster is bounced, all the client
             * connections cached will be dead and for low throughput clients it
             * will cause the operation to fail.
             * 
             * If the read interest is still there, the selector notifies read.
             * The read returns -1 and it will be converted to EOFException. So
             * socket will be closed and clientRequestExecutor will be
             * invalidated. The caller will create new connection.
             * 
             * Commented code --> selectionKey.interestOps(0);
             */
        }
        ClientRequest<?> originalRequest = completeClientRequest();

        if(originalRequest == null && logger.isEnabledFor(Level.WARN))
            logger.warn("No client associated with " + socketChannel.socket());
    }

    @Override
    protected void reportException(IOException e) {
        ClientRequest<?> local = clientRequest;
        if(local != null) {
            local.reportException(e);
        }
    }

    @Override
    protected void connect(SelectionKey selectionKey) throws IOException {
        if(!checkTimeout()) {
            return;
        }

        if(socketChannel.finishConnect() == false) {
            return;
        }

        if(logger.isDebugEnabled()) {
            // check buffer sizes you often don't get out what you put in!
            if(socketChannel.socket().getReceiveBufferSize() != this.socketBufferSize) {
                logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                             + " bytes but actual size is "
                             + socketChannel.socket().getReceiveBufferSize() + " bytes.");
            }

            if(socketChannel.socket().getSendBufferSize() != this.socketBufferSize) {
                logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                             + " bytes but actual size is "
                             + socketChannel.socket().getSendBufferSize() + " bytes.");
            }
        }

        addClientRequest(clientRequest, timeoutMs, 0);
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

        resetStreams();

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
    private ClientRequest<?> completeClientRequest() {
        ClientRequest<?> local = atomicNullOutClientRequest();
        if(local == null) {
            return null;
        }

        if(isExpired) {
            local.timeOut();
        }
        else {
            local.complete();
        }

        if(logger.isTraceEnabled())
            logger.trace("Marked client associated with " + socketChannel.socket() + " as complete");

        return local;
    }

}
