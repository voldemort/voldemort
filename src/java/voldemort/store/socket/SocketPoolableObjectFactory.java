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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.log4j.Logger;

/**
 * A Factory for creating sockets
 * 
 * @author jay
 * 
 */
public class SocketPoolableObjectFactory implements KeyedPoolableObjectFactory {

    public static final Logger logger = Logger.getLogger(SocketPoolableObjectFactory.class);

    private final int timeoutMs;
    private final int socketBufferSize;
    public final AtomicInteger created;
    public final AtomicInteger destroyed;

    public SocketPoolableObjectFactory(int timeoutMs, int socketBufferSize) {
        this.timeoutMs = timeoutMs;
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

    public void destroyObject(Object key, Object value) throws Exception {
        SocketDestination dest = (SocketDestination) key;
        SocketAndStreams sands = (SocketAndStreams) value;
        sands.getSocket().close();
        int numDestroyed = destroyed.incrementAndGet();
        if(logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    public Object makeObject(Object key) throws Exception {
        SocketDestination dest = (SocketDestination) key;
        Socket socket = new Socket();
        socket.setReceiveBufferSize(this.socketBufferSize);
        socket.setSendBufferSize(this.socketBufferSize);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(timeoutMs);
        socket.connect(new InetSocketAddress(dest.getHost(), dest.getPort()));

        // check buffer sizes
        if(socket.getReceiveBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + socket.getReceiveBufferSize() + " bytes.");
        if(socket.getSendBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + socket.getSendBufferSize() + " bytes.");

        int numCreated = created.incrementAndGet();
        if(logger.isDebugEnabled())
            logger.debug("Created socket " + numCreated + " for " + dest.getHost() + ":"
                         + dest.getPort());

        return new SocketAndStreams(socket);
    }

    public boolean validateObject(Object key, Object value) {
        SocketAndStreams sands = (SocketAndStreams) value;
        Socket s = sands.getSocket();
        boolean isValid = !s.isClosed() && s.isBound() && s.isConnected();
        if(!isValid)
            logger.debug("Socket connection " + sands + " is no longer valid, closing.");
        return isValid;
    }

    public int getTimeout() {
        return this.timeoutMs;
    }

    public int getNumberCreated() {
        return this.created.get();
    }

    public int getNumberDestroyed() {
        return this.destroyed.get();
    }

}
