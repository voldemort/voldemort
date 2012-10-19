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

package voldemort.client;

import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.ClientStoreVerifier;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorListener;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * A StoreClientFactory abstracts away the connection pooling, threading, and
 * bootstrapping mechanism. It can be used to create any number of
 * {@link voldemort.client.StoreClient StoreClient} instances for different
 * stores.
 * 
 * 
 */
public class SocketStoreClientFactory extends AbstractStoreClientFactory {

    public static final String URL_SCHEME = "tcp";

    private final SocketStoreFactory storeFactory;
    private FailureDetectorListener failureDetectorListener;
    private final RequestRoutingType requestRoutingType;

    public SocketStoreClientFactory(ClientConfig config) {
        super(config);
        this.requestRoutingType = RequestRoutingType.getRequestRoutingType(RoutingTier.SERVER.equals(config.getRoutingTier()),
                                                                           false);
        this.storeFactory = new ClientRequestExecutorPool(config.getSelectors(),
                                                          config.getMaxConnectionsPerNode(),
                                                          config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                          config.getSocketTimeout(TimeUnit.MILLISECONDS),
                                                          config.getSocketBufferSize(),
                                                          config.getSocketKeepAlive(),
                                                          config.isJmxEnabled(),
                                                          jmxId);
    }

    @Override
    public <K, V> StoreClient<K, V> getStoreClient(final String storeName,
                                                   final InconsistencyResolver<Versioned<V>> resolver) {
        if(getConfig().isLazyEnabled())
            return new LazyStoreClient<K, V>(new Callable<StoreClient<K, V>>() {

                public StoreClient<K, V> call() throws Exception {
                    return getParentStoreClient(storeName, resolver);
                }
            });

        return getParentStoreClient(storeName, resolver);
    }

    private <K, V> StoreClient<K, V> getParentStoreClient(String storeName,
                                                          InconsistencyResolver<Versioned<V>> resolver) {
        return super.getStoreClient(storeName, resolver);
    }

    @Override
    protected List<Versioned<String>> getRemoteMetadata(String key, URI url) {
        try {
            return super.getRemoteMetadata(key, url);
        } catch(VoldemortException e) {
            // Fix SNA-4227: When an error occurs during bootstrap, close the
            // socket
            SocketDestination destination = new SocketDestination(url.getHost(),
                                                                  url.getPort(),
                                                                  getRequestFormatType());
            storeFactory.close(destination);
            throw new VoldemortException(e);
        }
    }

    @Override
    protected Store<ByteArray, byte[], byte[]> getStore(String storeName,
                                                        String host,
                                                        int port,
                                                        RequestFormatType type) {
        return storeFactory.create(storeName, host, port, type, requestRoutingType);
    }

    @Override
    protected FailureDetector initFailureDetector(final ClientConfig config, Cluster cluster) {
        failureDetectorListener = new FailureDetectorListener() {

            public void nodeAvailable(Node node) {

            }

            public void nodeUnavailable(Node node) {
                if(logger.isInfoEnabled())
                    logger.info(node + " has been marked as unavailable, destroying socket pool");

                // Kill the socket pool for this node...
                SocketDestination destination = new SocketDestination(node.getHost(),
                                                                      node.getSocketPort(),
                                                                      config.getRequestFormatType());
                storeFactory.close(destination);
            }

        };

        ClientStoreVerifier storeVerifier = new ClientStoreVerifier() {

            @Override
            protected Store<ByteArray, byte[], byte[]> getStoreInternal(Node node) {
                logger.debug("Returning a new store verifier for node: " + node);
                return SocketStoreClientFactory.this.getStore(MetadataStore.METADATA_STORE_NAME,
                                                              node.getHost(),
                                                              node.getSocketPort(),
                                                              config.getRequestFormatType());
            }

        };

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig(config).setCluster(cluster)
                                                                                       .setStoreVerifier(storeVerifier);

        return create(failureDetectorConfig, false, failureDetectorListener);
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

    @Override
    public void close() {
        this.storeFactory.close();
        if(failureDetector != null)
            this.failureDetector.removeFailureDetectorListener(failureDetectorListener);

        super.close();
    }

    public <K, V, T> Store<K, V, T> getSystemStore(String storeName,
                                                   String clusterXml,
                                                   FailureDetector fd) {
        return getRawStore(storeName,
                           null,
                           SystemStoreConstants.SYSTEM_STORE_SCHEMA,
                           clusterXml,
                           fd);
    }
}
