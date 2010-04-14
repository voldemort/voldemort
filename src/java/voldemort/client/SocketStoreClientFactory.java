/*
 * Copyright 2008-2010 LinkedIn, Inc
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
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import voldemort.client.protocol.RequestFormatType;
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
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;

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

        this.storeFactory = new ClientRequestExecutorPool(config.getMaxConnectionsPerNode(),
                                                          config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                          config.getSocketTimeout(TimeUnit.MILLISECONDS),
                                                          config.getSocketBufferSize(),
                                                          config.getSocketKeepAlive());
        if(config.isJmxEnabled())
            JmxUtils.registerMbean(storeFactory, JmxUtils.createObjectName(storeFactory.getClass()));
    }

    @Override
    protected Store<ByteArray, byte[]> getStore(String storeName,
                                                String host,
                                                int port,
                                                RequestFormatType type) {
        return storeFactory.create(storeName, host, port, type, requestRoutingType);
    }

    @Override
    protected FailureDetector initFailureDetector(final ClientConfig config,
                                                  final Collection<Node> nodes) {
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
            protected Store<ByteArray, byte[]> getStoreInternal(Node node) {
                return SocketStoreClientFactory.this.getStore(MetadataStore.METADATA_STORE_NAME,
                                                              node.getHost(),
                                                              node.getSocketPort(),
                                                              config.getRequestFormatType());
            }

        };

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig(config).setNodes(nodes)
                                                                                       .setStoreVerifier(storeVerifier);

        return create(failureDetectorConfig, true, failureDetectorListener);
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
        this.getThreadPool().shutdown();

        super.close();
    }

}
