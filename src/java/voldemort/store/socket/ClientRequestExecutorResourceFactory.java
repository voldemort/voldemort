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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.store.socket.clientrequest.BlockingClientRequest;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ProtocolNegotiatorClientRequest;
import voldemort.utils.Time;
import voldemort.utils.pool.ResourceFactory;

/**
 * A Factory for creating ClientRequestExecutor instances.
 */

public class ClientRequestExecutorResourceFactory implements
        ResourceFactory<SocketDestination, ClientRequestExecutor> {

    private final ClientSelectorManager selectorManager;
    private final int soTimeoutMs;
    private final int socketBufferSize;
    private final AtomicInteger created;
    private final AtomicInteger destroyed;
    private final boolean socketKeepAlive;
    private final Logger logger = Logger.getLogger(getClass());

    public ClientRequestExecutorResourceFactory(ClientSelectorManager selectorManager,
                                                int soTimeoutMs,
                                                int socketBufferSize) {
        this(selectorManager, soTimeoutMs, socketBufferSize, false);
    }

    public ClientRequestExecutorResourceFactory(ClientSelectorManager selectorManager,
                                                int soTimeoutMs,
                                                int socketBufferSize,
                                                boolean socketKeepAlive) {
        this.selectorManager = selectorManager;
        this.soTimeoutMs = soTimeoutMs;
        this.created = new AtomicInteger(0);
        this.destroyed = new AtomicInteger(0);
        this.socketBufferSize = socketBufferSize;
        this.socketKeepAlive = socketKeepAlive;
    }

    /**
     * Close the ClientRequestExecutor.
     */

    public void destroy(SocketDestination dest, ClientRequestExecutor clientRequestExecutor)
            throws Exception {
        clientRequestExecutor.close();
        int numDestroyed = destroyed.incrementAndGet();

        if(logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    /**
     * Create a ClientRequestExecutor for the given host/port
     */

    public ClientRequestExecutor create(SocketDestination dest) throws Exception {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.socket().setReceiveBufferSize(this.socketBufferSize);
        socketChannel.socket().setSendBufferSize(this.socketBufferSize);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setSoTimeout(soTimeoutMs);
        socketChannel.socket().setKeepAlive(this.socketKeepAlive);
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(dest.getHost(), dest.getPort()));

        // Since we're non-blocking and it takes a non-zero amount of time to
        // connect, invoke finishConnect and loop.
        while(!socketChannel.finishConnect()) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Still connecting to " + dest);
        }

        int numCreated = created.incrementAndGet();
        logger.debug("Created socket " + numCreated + " for " + dest.getHost() + ":"
                     + dest.getPort() + " using protocol " + dest.getRequestFormatType().getCode());

        // check buffer sizes--you often don't get out what you put in!
        if(socketChannel.socket().getReceiveBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                         + " bytes but actual size is "
                         + socketChannel.socket().getReceiveBufferSize() + " bytes.");

        if(socketChannel.socket().getSendBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                         + " bytes but actual size is "
                         + socketChannel.socket().getSendBufferSize() + " bytes.");

        return negotiateProtocol(dest, socketChannel);
    }

    private ClientRequestExecutor negotiateProtocol(SocketDestination dest,
                                                    SocketChannel socketChannel)
            throws IOException, InterruptedException {
        ClientRequestExecutor clientRequestExecutor = new ClientRequestExecutor(socketChannel,
                                                                                this.socketBufferSize,
                                                                                dest.getRequestFormatType());
        BlockingClientRequest<String> clientRequest = new BlockingClientRequest<String>(new ProtocolNegotiatorClientRequest(dest.getRequestFormatType()));
        clientRequestExecutor.setClientRequest(clientRequest);
        clientRequest.formatRequest(new DataOutputStream(clientRequestExecutor.getOutputStream()));
        selectorManager.submitRequest(clientRequestExecutor);
        clientRequest.await();

        // This will throw an error if the result of the protocol negotiation
        // failed, otherwise it returns an uninteresting token we can safely
        // ignore.
        clientRequest.getResult();

        return clientRequestExecutor;
    }

    public boolean validate(SocketDestination dest, ClientRequestExecutor clientRequestExecutor) {
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

        if(clientRequestExecutor.getCreateTimestamp() <= dest.getLastClosedTimestamp()) {
            if(logger.isDebugEnabled())
                logger.debug("Socket connection "
                             + clientRequestExecutor
                             + " was created on "
                             + new Date(clientRequestExecutor.getCreateTimestamp() / Time.NS_PER_MS)
                             + " before socket pool was closed and re-created (on "
                             + new Date(dest.getLastClosedTimestamp() / Time.NS_PER_MS) + ")");
            return false;
        }

        boolean isValid = clientRequestExecutor.isValid();

        if(!isValid && logger.isDebugEnabled())
            logger.debug("Client request executor connection " + clientRequestExecutor
                         + " is no longer valid, closing.");

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
