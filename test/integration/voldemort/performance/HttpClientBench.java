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

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;

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

            public void doOperation(int index) {
                GetMethod get = new GetMethod(url);
                try {
                    client.executeMethod(get);
                    byte[] bytes = get.getResponseBody();
                } catch(Exception e) {
                    e.printStackTrace();
                } finally {
                    get.releaseConnection();
                }
            }
        };
        perfTest.run(numRequests, numThreads);
        perfTest.printStats();

        System.exit(1);
    }

    private static HttpClient createClient() {
        HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
        HttpClient httpClient = new HttpClient(connectionManager);
        HttpClientParams clientParams = httpClient.getParams();
        clientParams.setConnectionManagerTimeout(DEFAULT_CONNECTION_MANAGER_TIMEOUT);
        clientParams.setSoTimeout(500);
        clientParams.setParameter(HttpMethodParams.RETRY_HANDLER,
                                  new DefaultHttpMethodRetryHandler(0, false));
        clientParams.setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        clientParams.setBooleanParameter("http.tcp.nodelay", false);
        clientParams.setIntParameter("http.socket.receivebuffer", 60000);
        clientParams.setParameter("http.useragent", VOLDEMORT_USER_AGENT);
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.setHost("localhost");
        hostConfig.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
        httpClient.setHostConfiguration(hostConfig);
        HttpConnectionManagerParams managerParams = httpClient.getHttpConnectionManager()
                                                              .getParams();
        managerParams.setConnectionTimeout(DEFAULT_CONNECTION_MANAGER_TIMEOUT);
        managerParams.setMaxTotalConnections(DEFAULT_MAX_CONNECTIONS);
        managerParams.setMaxConnectionsPerHost(httpClient.getHostConfiguration(),
                                               DEFAULT_MAX_HOST_CONNECTIONS);
        managerParams.setStaleCheckingEnabled(false);

        return httpClient;
    }

}
