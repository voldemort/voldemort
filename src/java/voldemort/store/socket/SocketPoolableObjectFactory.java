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

package voldemort.store.socket;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.utils.ByteUtils;

/**
 * A Factory for creating sockets
 * 
 * @author jay
 * 
 */
public class SocketPoolableObjectFactory implements KeyedPoolableObjectFactory {

    public static final Logger logger = Logger.getLogger(SocketPoolableObjectFactory.class);

    private final int soTimeoutMs;
    private final int socketBufferSize;
    private final AtomicInteger created;
    private final AtomicInteger destroyed;

    public SocketPoolableObjectFactory(int soTimeoutMs, int socketBufferSize) {
        this.soTimeoutMs = soTimeoutMs;
        this.created = new AtomicInteger(0);
        this.destroyed = new AtomicInteger(0);
        this.socketBufferSize = socketBufferSize;
    }

    public void activateObject(Object key, Object value) throws Exception {
    // nothing to see here
    }

    public void passivateObject(Object key, Object value) throws Exception {
    // nothing to see here
    }

    /**
     * Close the socket
     */
    public void destroyObject(Object key, Object value) throws Exception {
        SocketDestination dest = (SocketDestination) key;
        SocketAndStreams sands = (SocketAndStreams) value;
        sands.getSocket().close();
        int numDestroyed = destroyed.incrementAndGet();
        if(logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    /**
     * Create a socket for the given host/port
     */
    public Object makeObject(Object key) throws Exception {
        SocketDestination dest = (SocketDestination) key;
        Socket socket = new Socket();
        socket.setReceiveBufferSize(this.socketBufferSize);
        socket.setSendBufferSize(this.socketBufferSize);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(soTimeoutMs);
        socket.connect(new InetSocketAddress(dest.getHost(), dest.getPort()));

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

    public boolean validateObject(Object key, Object value) {
        SocketAndStreams sands = (SocketAndStreams) value;
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

}
