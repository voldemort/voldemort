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
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpVersion;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.ClientStoreVerifier;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.VoldemortIOUtils;

/**
 * A {@link voldemort.client.StoreClientFactory StoreClientFactory} that creates
 * a remote client that connects and bootstraps itself via HTTP.
 * 
 * 
 */
public class HttpStoreClientFactory extends AbstractStoreClientFactory {

    public static final String URL_SCHEME = "http";

    private static final String VOLDEMORT_USER_AGENT = "vldmrt/0.01";

    private final DefaultHttpClient httpClient;
    private final RequestFormatFactory requestFormatFactory;
    private final boolean reroute;

    public HttpStoreClientFactory(ClientConfig config) {
        super(config);
        ThreadSafeClientConnManager mgr = new ThreadSafeClientConnManager(SchemeRegistryFactory.createDefault(),
                                                                          config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                                          TimeUnit.MILLISECONDS);
        mgr.setMaxTotal(config.getMaxTotalConnections());
        mgr.setDefaultMaxPerRoute(config.getMaxConnectionsPerNode());

        this.httpClient = new DefaultHttpClient(mgr);
        HttpParams clientParams = this.httpClient.getParams();

        HttpProtocolParams.setUserAgent(clientParams, VOLDEMORT_USER_AGENT);
        HttpProtocolParams.setVersion(clientParams, HttpVersion.HTTP_1_1);

        HttpConnectionParams.setConnectionTimeout(clientParams,
                                                  config.getConnectionTimeout(TimeUnit.MILLISECONDS));
        HttpConnectionParams.setSoTimeout(clientParams,
                                          config.getSocketTimeout(TimeUnit.MILLISECONDS));
        HttpConnectionParams.setStaleCheckingEnabled(clientParams, false);

        this.httpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
        HttpClientParams.setCookiePolicy(clientParams, CookiePolicy.IGNORE_COOKIES);

        this.reroute = config.getRoutingTier().equals(RoutingTier.SERVER);
        this.requestFormatFactory = new RequestFormatFactory();
    }

    @Override
    protected Store<ByteArray, byte[], byte[]> getStore(String name,
                                                        String host,
                                                        int port,
                                                        RequestFormatType type) {
        return new HttpStore(name,
                             host,
                             port,
                             httpClient,
                             requestFormatFactory.getRequestFormat(type),
                             reroute);
    }

    @Override
    protected FailureDetector initFailureDetector(final ClientConfig config, Cluster cluster) {
        ClientStoreVerifier storeVerifier = new ClientStoreVerifier() {

            @Override
            protected Store<ByteArray, byte[], byte[]> getStoreInternal(Node node) {
                return HttpStoreClientFactory.this.getStore(MetadataStore.METADATA_STORE_NAME,
                                                            node.getHost(),
                                                            node.getHttpPort(),
                                                            config.getRequestFormatType());
            }

        };

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig(config).setCluster(cluster)
                                                                                       .setStoreVerifier(storeVerifier);

        return create(failureDetectorConfig, config.isJmxEnabled());
    }

    @Override
    protected int getPort(Node node) {
        return node.getHttpPort();
    }

    @Override
    protected void validateUrl(URI url) {
        if(!URL_SCHEME.equals(url.getScheme()))
            throw new IllegalArgumentException("Illegal scheme in bootstrap URL for HttpStoreClientFactory:"
                                               + " expected '"
                                               + URL_SCHEME
                                               + "' but found '"
                                               + url.getScheme() + "'.");
    }

    @Override
    public void close() {
        super.close();
        // should timeout connections on its own
        VoldemortIOUtils.closeQuietly(this.httpClient);
    }

}
