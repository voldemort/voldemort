/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.client.protocol.admin;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;
import voldemort.utils.pool.ResourceFactory;

/**
 * A Factory for creating sockets
 * 
 * 
 */
public class SocketResourceFactory implements ResourceFactory<SocketDestination, SocketAndStreams> {

    public static final Logger logger = Logger.getLogger(SocketResourceFactory.class);

    private final int soTimeoutMs;
    private final int socketBufferSize;
    private final AtomicInteger created;
    private final AtomicInteger destroyed;
    private final boolean socketKeepAlive;
    private final Map<SocketDestination, Long> lastClosedTimestamps;

    public SocketResourceFactory(int soTimeoutMs, int socketBufferSize) {
        this(soTimeoutMs, socketBufferSize, false);
    }

    public SocketResourceFactory(int soTimeoutMs, int socketBufferSize, boolean socketKeepAlive) {
        this.soTimeoutMs = soTimeoutMs;
        this.created = new AtomicInteger(0);
        this.destroyed = new AtomicInteger(0);
        this.socketBufferSize = socketBufferSize;
        this.socketKeepAlive = socketKeepAlive;
        this.lastClosedTimestamps = new ConcurrentHashMap<SocketDestination, Long>();
    }

    /**
     * Close the socket
     */
    public void destroy(SocketDestination dest, SocketAndStreams sands) throws Exception {
        sands.getSocket().close();
        int numDestroyed = destroyed.incrementAndGet();
        if(logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    /**
     * Create a socket for the given host/port
     */
    public SocketAndStreams create(SocketDestination dest) throws Exception {
        Socket socket = new Socket();
        socket.setReceiveBufferSize(this.socketBufferSize);
        socket.setSendBufferSize(this.socketBufferSize);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(soTimeoutMs);
        socket.setKeepAlive(this.socketKeepAlive);
        socket.connect(new InetSocketAddress(dest.getHost(), dest.getPort()), soTimeoutMs);

        recordSocketCreation(dest, socket);

        SocketAndStreams sands = new SocketAndStreams(socket, dest.getRequestFormatType());
        negotiateProtocol(sands, dest.getRequestFormatType());

        return sands;
    }

    private void negotiateProtocol(SocketAndStreams socket, RequestFormatType type)
            throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        byte[] proposal = ByteUtils.getBytes(type.getCode(), "UTF-8");
        outputStream.write(proposal);
        outputStream.flush();
        DataInputStream inputStream = socket.getInputStream();
        byte[] responseBytes = new byte[2];
        inputStream.readFully(responseBytes);
        String response = ByteUtils.getString(responseBytes, "UTF-8");
        if(response.equals("ok"))
            return;
        else if(response.equals("no"))
            throw new VoldemortException(type.getDisplayName()
                                         + " is not an acceptable protcol for the server.");
        else
            throw new VoldemortException("Unknown server response: " + response);
    }

    /* Log relevant socket creation details */
    private void recordSocketCreation(SocketDestination dest, Socket socket) throws SocketException {
        int numCreated = created.incrementAndGet();
        logger.debug("Created socket " + numCreated + " for " + dest.getHost() + ":"
                     + dest.getPort() + " using protocol " + dest.getRequestFormatType().getCode());

        // check buffer sizes--you often don't get out what you put in!
        int sendBufferSize = socket.getSendBufferSize();
        int receiveBufferSize = socket.getReceiveBufferSize();
        if(receiveBufferSize != this.socketBufferSize)
            logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + receiveBufferSize + " bytes.");
        if(sendBufferSize != this.socketBufferSize)
            logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + sendBufferSize + " bytes.");
    }

    public boolean validate(SocketDestination dest, SocketAndStreams sands) {
        /**
         * Keep track of the last time that we closed the sockets for a specific
         * SocketDestination. That way we know which sockets were created
         * *before* the SocketDestination was closed. For any sockets in the
         * pool at time of closure of the SocketDestination, these are shut down
         * immediately. For in-flight sockets that aren't in the pool at time of
         * closure of the SocketDestination, these are caught when they're
         * checked in via validate noting the relation of the timestamps.
         * 
         * See bug #222.
         */
        long lastClosedTimestamp = getLastClosedTimestamp(dest);

        if(sands.getCreateTimestamp() <= lastClosedTimestamp) {
            if(logger.isDebugEnabled())
                logger.debug("Socket connection " + sands + " was created on "
                             + new Date(sands.getCreateTimestamp() / Time.NS_PER_MS)
                             + " before socket pool was closed and re-created (on "
                             + new Date(lastClosedTimestamp / Time.NS_PER_MS) + ")");
            return false;
        }

        Socket s = sands.getSocket();
        boolean isValid = !s.isClosed() && s.isBound() && s.isConnected();
        if(!isValid && logger.isDebugEnabled())
            logger.debug("Socket connection " + sands + " is no longer valid, closing.");
        return isValid;
    }

    public int getTimeout() {
        return this.soTimeoutMs;
    }

    public int getNumberCreated() {
        return this.created.get();
    }

    public int getNumberDestroyed() {
        return this.destroyed.get();
    }

    public void close() {}

    /**
     * Returns the nanosecond-based timestamp of when this socket destination
     * was last closed. SocketDestination objects can be closed when their node
     * is marked as unavailable if the node goes down (temporarily or
     * otherwise). This timestamp is used to determine when sockets related to
     * the SocketDestination should be closed.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via setLastClosedTimestamp each
     * time the node is marked as unavailable.
     * 
     * @return Nanosecond-based timestamp of last close
     */

    private long getLastClosedTimestamp(SocketDestination socketDestination) {
        Long lastClosedTimestamp = lastClosedTimestamps.get(socketDestination);
        return lastClosedTimestamp != null ? lastClosedTimestamp.longValue() : 0;
    }

    /**
     * Assigns the last closed timestamp based on the current time in
     * nanoseconds.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via this method each time the
     * node is marked as unavailable.
     */

    public void setLastClosedTimestamp(SocketDestination socketDestination) {
        lastClosedTimestamps.put(socketDestination, System.nanoTime());
    }

}
