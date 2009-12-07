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

import java.io.StringReader;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.ClientStoreResolver;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorListener;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
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

    private final SocketPool socketPool;
    private final RoutingTier routingTier;
    private FailureDetectorListener failureDetectorListener;

    public SocketStoreClientFactory(ClientConfig config) {
        super(config);
        this.routingTier = config.getRoutingTier();
        this.socketPool = new SocketPool(config.getMaxConnectionsPerNode(),
                                         config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                         config.getSocketTimeout(TimeUnit.MILLISECONDS),
                                         config.getSocketBufferSize());
        registerJmx(JmxUtils.createObjectName(SocketPool.class), socketPool);

        String clusterXml = bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);
        Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml));

        failureDetector = initFailureDetector(config, cluster.getNodes());
    }

    @Override
    protected Store<ByteArray, byte[]> getStore(String storeName,
                                                String host,
                                                int port,
                                                RequestFormatType type) {
        return new SocketStore(Utils.notNull(storeName),
                               new SocketDestination(Utils.notNull(host), port, type),
                               socketPool,
                               RoutingTier.SERVER.equals(routingTier));
    }

    protected FailureDetector initFailureDetector(final ClientConfig config,
                                                  final Collection<Node> nodes) {
        failureDetectorListener = new FailureDetectorListener() {

            public void nodeAvailable(Node node) {

            }

            public void nodeUnavailable(Node node) {
                if(logger.isInfoEnabled())
                    logger.info("Node " + node
                                + " has been marked as unavailable, destroying socket pool");

                // Kill the socket pool for this node...
                SocketDestination destination = new SocketDestination(node.getHost(),
                                                                      node.getSocketPort(),
                                                                      config.getRequestFormatType());
                socketPool.close(destination);
            }

        };

        ClientStoreResolver storeResolver = new ClientStoreResolver() {

            @Override
            protected Store<ByteArray, byte[]> getStoreInternal(Node node) {
                return SocketStoreClientFactory.this.getStore(MetadataStore.METADATA_STORE_NAME,
                                                              node.getHost(),
                                                              node.getSocketPort(),
                                                              config.getRequestFormatType());
            }

        };

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(config.getFailureDetector())
                                                                                 .setJmxEnabled(config.isJmxEnabled())
                                                                                 .setNodeBannagePeriod(config.getNodeBannagePeriod(TimeUnit.MILLISECONDS))
                                                                                 .setNodes(nodes)
                                                                                 .setStoreResolver(storeResolver);

        return FailureDetectorUtils.create(failureDetectorConfig, failureDetectorListener);
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
        this.socketPool.close();
        this.failureDetector.removeFailureDetectorListener(failureDetectorListener);
        this.getThreadPool().shutdown();

        super.close();
    }

}
