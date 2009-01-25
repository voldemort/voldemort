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

package voldemort.client;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
import voldemort.store.Store;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.Utils;

/**
 * A StoreClientFactory abstracts away the connection pooling, threading, and
 * bootstrapping mechanism. It can be used to create any number of
 * {@link voldemort.client.StoreClient StoreClient} instances for different
 * stores.
 * 
 * @author jay
 * 
 */
public class SocketStoreClientFactory extends AbstractStoreClientFactory {

    public static final String URL_SCHEME = "tcp";
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 5000;
    public static final int DEFAULT_NUM_THREADS = 5;
    public static final int DEFAULT_MAX_QUEUED_REQUESTS = 1000;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_NODE = 10;
    public static final int DEFAULT_MAX_CONNECTIONS = 50;

    private SocketPool socketPool;

    public SocketStoreClientFactory(String bootstrapUrl) {
        this(DEFAULT_NUM_THREADS,
             DEFAULT_NUM_THREADS,
             DEFAULT_MAX_QUEUED_REQUESTS,
             DEFAULT_MAX_CONNECTIONS_PER_NODE,
             DEFAULT_MAX_CONNECTIONS,
             bootstrapUrl);
    }

    public SocketStoreClientFactory(int coreThreads,
                                    int maxThreads,
                                    int maxQueuedRequests,
                                    int maxConnectionsPerNode,
                                    int maxTotalConnections,
                                    String... bootstrapUrls) {
        this(coreThreads,
             maxThreads,
             maxQueuedRequests,
             maxConnectionsPerNode,
             maxTotalConnections,
             DEFAULT_SOCKET_TIMEOUT_MS,
             AbstractStoreClientFactory.DEFAULT_ROUTING_TIMEOUT_MS,
             bootstrapUrls);
    }

    public SocketStoreClientFactory(int coreThreads,
                                    int maxThreads,
                                    int maxQueuedRequests,
                                    int maxConnectionsPerNode,
                                    int maxTotalConnections,
                                    int socketTimeoutMs,
                                    int routingTimeoutMs,
                                    String... bootstrapUrls) {
        this(new ThreadPoolExecutor(coreThreads,
                                    maxThreads,
                                    10000L,
                                    TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(maxQueuedRequests),
                                    new DaemonThreadFactory("voldemort-client-thread-"),
                                    new ThreadPoolExecutor.CallerRunsPolicy()),
             maxConnectionsPerNode,
             maxTotalConnections,
             socketTimeoutMs,
             routingTimeoutMs,
             AbstractStoreClientFactory.DEFAULT_NODE_BANNAGE_MS,
             new DefaultSerializerFactory(),
             bootstrapUrls);
    }

    public SocketStoreClientFactory(ExecutorService service,
                                    int maxConnectionsPerNode,
                                    int maxTotalConnections,
                                    int socketTimeoutMs,
                                    int routingTimeoutMs,
                                    int defaultNodeBannageMs,
                                    String... bootstrapUrls) {
        this(service,
             maxConnectionsPerNode,
             maxTotalConnections,
             socketTimeoutMs,
             routingTimeoutMs,
             defaultNodeBannageMs,
             new DefaultSerializerFactory(),
             bootstrapUrls);
    }

    public SocketStoreClientFactory(ExecutorService service,
                                    int maxConnectionsPerNode,
                                    int maxTotalConnections,
                                    int socketTimeoutMs,
                                    int routingTimeoutMs,
                                    int defaultNodeBannageMs,
                                    SerializerFactory serializerFactory,
                                    String... boostrapUrls) {
        super(service, serializerFactory, routingTimeoutMs, defaultNodeBannageMs, boostrapUrls);
        this.socketPool = new SocketPool(maxConnectionsPerNode,
                                         maxTotalConnections,
                                         socketTimeoutMs);
    }

    @Override
    protected Store<byte[], byte[]> getStore(String storeName, String host, int port) {
        return new SocketStore(Utils.notNull(storeName), Utils.notNull(host), port, socketPool);
    }

    @Override
    protected int getPort(Node node) {
        return node.getSocketPort();
    }

    @Override
    protected void validateUrl(URI url) {
        if(!URL_SCHEME.equals(url.getScheme()))
            throw new IllegalArgumentException("Illegal scheme in bootstrap URL for SocketStoreClientFactory:"
                                               + " expected '"
                                               + URL_SCHEME
                                               + "' but found '"
                                               + url.getScheme() + "'.");
    }

}
