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

package voldemort.performance;

import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

import voldemort.utils.VoldemortIOUtils;

public class HttpClientBench {

    private static final int DEFAULT_CONNECTION_MANAGER_TIMEOUT = 100000;
    private static final int DEFAULT_MAX_CONNECTIONS = 10;
    private static final int DEFAULT_MAX_HOST_CONNECTIONS = 10;
    private static final String VOLDEMORT_USER_AGENT = "vldmrt/0.01";

    public static void main(String[] args) throws Exception {
        if(args.length < 3) {
            System.err.println("USAGE: java HttpClientBench url numThreads numRequests");
            System.exit(1);
        }

        final String url = args[0];
        final int numThreads = Integer.parseInt(args[1]);
        final int numRequests = Integer.parseInt(args[2]);

        System.out.println("Benchmarking against " + url);

        final HttpClient client = createClient();
        PerformanceTest perfTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) {
                HttpResponse response = null;
                try {
                    HttpGet get = new HttpGet(url);
                    response = client.execute(get);
                    response.getEntity().consumeContent();
                } catch(Exception e) {
                    e.printStackTrace();
                } finally {
                    VoldemortIOUtils.closeQuietly(response);
                }
            }
        };
        perfTest.run(numRequests, numThreads);
        perfTest.printStats();
        VoldemortIOUtils.closeQuietly(client);
        System.exit(1);
    }

    private static HttpClient createClient() {
        ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager(SchemeRegistryFactory.createDefault(),
                                                                                        DEFAULT_CONNECTION_MANAGER_TIMEOUT,
                                                                                        TimeUnit.MILLISECONDS);

        DefaultHttpClient httpClient = new DefaultHttpClient(connectionManager);

        HttpParams clientParams = httpClient.getParams();

        clientParams.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, false);

        clientParams.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 60000);
        clientParams.setParameter(CoreProtocolPNames.USER_AGENT, VOLDEMORT_USER_AGENT);
        // HostConfiguration hostConfig = new HostConfiguration();
        // hostConfig.setHost("localhost");

        clientParams.setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);

        HttpConnectionParams.setConnectionTimeout(clientParams, DEFAULT_CONNECTION_MANAGER_TIMEOUT);
        HttpConnectionParams.setSoTimeout(clientParams, 500);
        httpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
        HttpClientParams.setCookiePolicy(clientParams, CookiePolicy.IGNORE_COOKIES);

        connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS);
        connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HOST_CONNECTIONS);
        HttpConnectionParams.setStaleCheckingEnabled(clientParams, false);

        return httpClient;
    }

}
