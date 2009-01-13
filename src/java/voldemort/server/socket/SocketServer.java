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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.Store;

/**
 * A simple socket-based server for serving voldemort requests
 * 
 * @author jay
 * 
 */
public class SocketServer extends Thread {

    private static final Logger logger = Logger.getLogger(SocketServer.class.getName());

    private final ExecutorService threadPool;
    private final Random random = new Random();
    private final int port;
    private final ConcurrentMap<String, ? extends Store<byte[], byte[]>> storeMap;
    private final ThreadGroup threadGroup;
    private final CountDownLatch isStarted = new CountDownLatch(1);
    private ServerSocket serverSocket = null;

    private final ThreadFactory threadFactory = new ThreadFactory() {

        public Thread newThread(Runnable r) {
            String name = getThreadName("handler");
            Thread t = new Thread(threadGroup, r, name);
            t.setDaemon(true);
            return t;
        }
    };

    private final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {

        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            SocketServerSession session = (SocketServerSession) r;
            logger.error("Too many open connections, " + executor.getActiveCount() + " of "
                         + executor.getLargestPoolSize()
                         + " threads in use, denying connection from "
                         + session.getSocket().getRemoteSocketAddress() + ":"
                         + session.getSocket().getPort());
            try {
                session.getSocket().close();
            } catch(IOException e) {
                logger.error("Could not close socket.", e);
            }
        }
    };

    public SocketServer(ConcurrentMap<String, ? extends Store<byte[], byte[]>> storeMap,
                        int port,
                        int defaultThreads,
                        int maxThreads) {
        this.port = port;
        this.threadGroup = new ThreadGroup("VoldemortRawSocketHandler");
        this.storeMap = storeMap;
        this.threadPool = new ThreadPoolExecutor(defaultThreads,
                                                 maxThreads,
                                                 1,
                                                 TimeUnit.SECONDS,
                                                 new SynchronousQueue<Runnable>(),
                                                 threadFactory,
                                                 rejectedExecutionHandler);
    }

    @Override
    public void run() {
        logger.info("Starting voldemort socket server on port " + port + ".");
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(port));
            isStarted.countDown();
            while(!isInterrupted() && !serverSocket.isClosed()) {
                final Socket socket = serverSocket.accept();
                socket.setReceiveBufferSize(10000);
                socket.setSendBufferSize(10000);
                socket.setTcpNoDelay(true);
                this.threadPool.execute(new SocketServerSession(socket));
            }
        } catch(BindException e) {
            logger.error("Could not bind to port " + port + ".");
            throw new VoldemortException(e);
        } catch(SocketException e) {
            // If we have been manually shutdown, ignore
            if(!isInterrupted())
                logger.error("Error in server: ", e);
        } catch(IOException e) {
            throw new VoldemortException(e);
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

    public void shutdown() {
        logger.info("Shutting down voldemort socket server on port " + port + ".");
        threadGroup.interrupt();
        interrupt();
        threadPool.shutdownNow();
        try {
            threadPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.warn("Interrupted while waiting for tasks to complete: ", e);
        }
        try {
            if(!serverSocket.isClosed())
                serverSocket.close();
        } catch(IOException e) {
            logger.warn("Exception while closing server socket: ", e);
        }
    }

    public int getPort() {
        return this.port;
    }

    public void awaitStartupCompletion() {
        try {
            isStarted.await();
        } catch(InterruptedException e) {
            // this is okay, if we are interrupted we can stop waiting
        }
    }

    private String getThreadName(String baseName) {
        return baseName + random.nextInt(1000000);
    }

    private class SocketServerSession implements Runnable {

        private final Socket socket;

        public SocketServerSession(Socket socket) {
            this.socket = socket;
        }

        public Socket getSocket() {
            return socket;
        }

        public void run() {
            try {
                logger.info("Client " + socket.getRemoteSocketAddress() + " connected.");
                StreamStoreRequestHandler handler = new StreamStoreRequestHandler(storeMap,
                                                                                  new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                                                                              1000)),
                                                                                  new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                                                                                1000)));
                while(!Thread.currentThread().isInterrupted()) {
                    handler.handleRequest();
                }
            } catch(EOFException e) {
                logger.info("Client " + socket.getRemoteSocketAddress() + " disconnected.");
            } catch(IOException e) {
                logger.error(e);
            } finally {
                try {
                    socket.close();
                } catch(Exception e) {
                    logger.error("Error while closing socket", e);
                }
            }
        }
    }

}
