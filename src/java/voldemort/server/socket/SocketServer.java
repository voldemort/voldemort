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

package voldemort.server.socket;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.server.StatusManager;
import voldemort.server.protocol.RequestHandlerFactory;

/**
 * A simple socket-based server for serving voldemort requests
 * 
 * 
 */
@JmxManaged
public class SocketServer extends Thread {

    private final Logger logger;

    /* Used to indicate success in queue */
    private static final Object SUCCESS = new Object();
    private final BlockingQueue<Object> startedStatusQueue = new LinkedBlockingQueue<Object>();

    private final ThreadPoolExecutor threadPool;
    private final int port;
    private final ThreadGroup threadGroup;
    private final int socketBufferSize;
    private final RequestHandlerFactory handlerFactory;
    private final int maxThreads;
    private final StatusManager statusManager;
    private final AtomicLong sessionIdSequence;
    private final ConcurrentMap<Long, SocketServerSession> activeSessions;
    private final String serverName;

    private ServerSocket serverSocket = null;

    public SocketServer(int port,
                        int defaultThreads,
                        int maxThreads,
                        int socketBufferSize,
                        RequestHandlerFactory handlerFactory,
                        String serverName) {
        this.port = port;
        this.socketBufferSize = socketBufferSize;
        this.threadGroup = new ThreadGroup("voldemort-socket-server");
        this.handlerFactory = handlerFactory;
        this.maxThreads = maxThreads;
        this.threadPool = new ThreadPoolExecutor(defaultThreads,
                                                 maxThreads,
                                                 0,
                                                 TimeUnit.MILLISECONDS,
                                                 new SynchronousQueue<Runnable>(),
                                                 threadFactory,
                                                 rejectedExecutionHandler);
        this.statusManager = new StatusManager(this.threadPool);
        this.sessionIdSequence = new AtomicLong(0);
        this.activeSessions = new ConcurrentHashMap<Long, SocketServerSession>();
        this.serverName = serverName;
        this.logger = Logger.getLogger(SocketServer.class.getName() + "[" + serverName + "]");
    }

    private final ThreadFactory threadFactory = new ThreadFactory() {

        private AtomicLong threadIdSequence = new AtomicLong(0);

        public Thread newThread(Runnable r) {
            String name = "voldemort-server-" + threadIdSequence.getAndIncrement();
            Thread t = new Thread(threadGroup, r, name);
            t.setDaemon(true);
            return t;
        }
    };

    private final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {

        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            SocketServerSession session = (SocketServerSession) r;
            if(interrupted()) {
                logger.info("Denying connection from "
                            + session.getSocket().getRemoteSocketAddress()
                            + ", server is shutting down.");
            } else {
                logger.error("Too many open connections, " + executor.getActiveCount() + " of "
                             + executor.getLargestPoolSize()
                             + " threads in use, denying connection from "
                             + session.getSocket().getRemoteSocketAddress());
            }
            try {
                session.getSocket().close();
            } catch(IOException e) {
                logger.error("Could not close socket.", e);
            }
        }
    };

    @Override
    public void run() {
        logger.info("Starting voldemort socket server (" + serverName + ") on port " + port);
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(port));
            serverSocket.setReceiveBufferSize(this.socketBufferSize);
            startedStatusQueue.put(SUCCESS);
            while(!isInterrupted() && !serverSocket.isClosed()) {
                final Socket socket = serverSocket.accept();
                configureSocket(socket);
                long sessionId = this.sessionIdSequence.getAndIncrement();
                this.threadPool.execute(new SocketServerSession(activeSessions,
                                                                socket,
                                                                handlerFactory,
                                                                sessionId));
            }
        } catch(BindException e) {
            logger.error("Could not bind to port " + port + ".");
            startedStatusQueue.offer(e);
            throw new VoldemortException(e);
        } catch(SocketException e) {
            startedStatusQueue.offer(e);
            // If we have been manually shutdown, ignore
            if(!isInterrupted())
                logger.error("Error in server: ", e);
        } catch(IOException e) {
            startedStatusQueue.offer(e);
            throw new VoldemortException(e);
        } catch(Throwable t) {
            logger.error(t);
            startedStatusQueue.offer(t);
            if(t instanceof Error)
                throw (Error) t;
            else if(t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new VoldemortException(t);
        } finally {
            if(serverSocket != null) {
                try {
                    serverSocket.close();
                } catch(IOException e) {
                    logger.warn("Error while shutting down server.", e);
                }
            }

        }
    }

    private void configureSocket(Socket socket) throws SocketException {
        socket.setTcpNoDelay(true);
        socket.setSendBufferSize(this.socketBufferSize);
        if(socket.getReceiveBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket receive buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + socket.getReceiveBufferSize() + " bytes.");
        if(socket.getSendBufferSize() != this.socketBufferSize)
            logger.debug("Requested socket send buffer size was " + this.socketBufferSize
                         + " bytes but actual size is " + socket.getSendBufferSize() + " bytes.");
    }

    public void shutdown() {
        logger.info("Shutting down voldemort socket server (" + serverName + ").");

        // first shut down the acceptor to stop new connections
        interrupt();
        try {
            if(!serverSocket.isClosed())
                serverSocket.close();
        } catch(IOException e) {
            logger.error("Error while closing socket server: " + e.getMessage());
        }

        // now kill all the active sessions
        threadPool.shutdownNow();
        killActiveSessions();

        try {
            boolean completed = threadPool.awaitTermination(5, TimeUnit.SECONDS);
            if(!completed)
                logger.warn("Timed out waiting for threadpool to close.");
        } catch(InterruptedException e) {
            logger.warn("Interrupted while waiting for socket server shutdown to complete: ", e);
        }
    }

    @JmxOperation(description = "Kill all the active sessions.")
    public void killActiveSessions() {
        // loop through and close the socket of all the active sessions
        logger.info("Killing all active sessions.");
        for(Map.Entry<Long, SocketServerSession> entry: activeSessions.entrySet()) {
            try {
                logger.debug("Closing session " + entry.getKey());
                entry.getValue().close();
            } catch(IOException e) {
                logger.warn("Error while closing session socket: ", e);
            }
        }
    }

    @JmxGetter(name = "port", description = "The port on which the server accepts connections.")
    public int getPort() {
        return this.port;
    }

    @JmxGetter(name = "maxThreads", description = "The maximum number of threads that can be started on the server.")
    public int getMaxThreads() {
        return this.maxThreads;
    }

    @JmxGetter(name = "currentThreads", description = "The current number of utilized threads on the server.")
    public int getCurrentThreads() {
        return this.threadPool.getActiveCount();
    }

    @JmxGetter(name = "remainingThreadCapacity", description = "The number of additional threads that can be allocated before reaching the maximum.")
    public int getRemainingThreads() {
        return getMaxThreads() - getCurrentThreads();
    }

    /**
     * Blocks until the server has started successfully or an exception is
     * thrown.
     * 
     * @throws VoldemortException if a problem occurs during start-up wrapping
     *         the original exception.
     */
    public void awaitStartupCompletion() {
        try {
            Object obj = startedStatusQueue.take();
            if(obj instanceof Throwable)
                throw new VoldemortException((Throwable) obj);
        } catch(InterruptedException e) {
            // this is okay, if we are interrupted we can stop waiting
        }
    }

    public StatusManager getStatusManager() {
        return statusManager;
    }
}
