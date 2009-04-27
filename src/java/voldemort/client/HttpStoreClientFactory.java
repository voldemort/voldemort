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
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;

import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.utils.ByteArray;

/**
 * A {@link voldemort.client.StoreClientFactory StoreClientFactory} that creates
 * a remote client that connects and bootstraps itself via HTTP.
 * 
 * @author jay
 * 
 */
public class HttpStoreClientFactory extends AbstractStoreClientFactory {

    public static final String URL_SCHEME = "http";

    private static final String VOLDEMORT_USER_AGENT = "vldmrt/0.01";

    private final HttpClient httpClient;
    private final MultiThreadedHttpConnectionManager connectionManager;
    private final RequestFormatFactory requestFormatFactory;
    private final boolean reroute;

    public HttpStoreClientFactory(ClientConfig config) {
        super(config);
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
        this.connectionManager = new MultiThreadedHttpConnectionManager();
        this.httpClient = new HttpClient(connectionManager);
        this.httpClient.setHostConfiguration(hostConfig);
        HttpClientParams clientParams = this.httpClient.getParams();
        clientParams.setConnectionManagerTimeout(config.getConnectionTimeout(TimeUnit.MILLISECONDS));
        clientParams.setSoTimeout(config.getSocketTimeout(TimeUnit.MILLISECONDS));
        clientParams.setParameter(HttpMethodParams.RETRY_HANDLER,
                                  new DefaultHttpMethodRetryHandler(0, false));
        clientParams.setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        clientParams.setParameter("http.useragent", VOLDEMORT_USER_AGENT);
        HttpConnectionManagerParams managerParams = this.httpClient.getHttpConnectionManager()
                                                                   .getParams();
        managerParams.setConnectionTimeout(config.getConnectionTimeout(TimeUnit.MILLISECONDS));
        managerParams.setMaxTotalConnections(config.getMaxTotalConnections());
        managerParams.setStaleCheckingEnabled(false);
        managerParams.setMaxConnectionsPerHost(httpClient.getHostConfiguration(),
                                               config.getMaxConnectionsPerNode());
        this.reroute = config.getRoutingTier().equals(RoutingTier.SERVER);
        this.requestFormatFactory = new RequestFormatFactory();
    }

    @Override
    protected Store<ByteArray, byte[]> getStore(String name,
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

    public void close() {
    // should timeout connections on its own
    }

}
