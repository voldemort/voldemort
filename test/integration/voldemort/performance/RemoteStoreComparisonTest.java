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
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.http.HttpService;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class RemoteStoreComparisonTest {

    public static void main(String[] args) throws Exception {
        if(args.length != 2)
            Utils.croak("USAGE: java " + RemoteStoreComparisonTest.class.getName()
                        + " numRequests numThreads [useNio]");

        int numRequests = Integer.parseInt(args[0]);
        int numThreads = Integer.parseInt(args[1]);
        boolean useNio = args.length > 2 ? args[2].equals("true") : false;

        /** * In memory test ** */
        final Store<byte[], byte[], byte[]> memStore = new InMemoryStorageEngine<byte[], byte[], byte[]>("test");
        PerformanceTest memWriteTest = new PerformanceTest() {

            @Override
            public void doOperation(int i) {
                byte[] key = String.valueOf(i).getBytes();
                memStore.put(key, new Versioned<byte[]>(key), null);
            }
        };
        System.out.println("###########################################");
        System.out.println("Performing memory write test.");
        memWriteTest.run(numRequests, numThreads);
        memWriteTest.printStats();
        System.out.println();

        PerformanceTest memReadTest = new PerformanceTest() {

            @Override
            public void doOperation(int i) {
                try {
                    memStore.get(String.valueOf(i).getBytes(), null);
                } catch(Exception e) {
                    System.out.println("Failure on i = " + i);
                    e.printStackTrace();
                }
            }
        };
        System.out.println("Performing memory read test.");
        memReadTest.run(numRequests, numThreads);
        memReadTest.printStats();
        System.out.println();
        System.out.println();

        /** * Do Socket tests ** */
        String storeName = "test";
        StoreRepository repository = new StoreRepository();
        repository.addLocalStore(new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName));
        SocketPool socketPool = new SocketPool(10, 1000, 1000, 32 * 1024);
        final SocketStore socketStore = new SocketStore(storeName,
                                                        new SocketDestination("localhost",
                                                                              6666,
                                                                              RequestFormatType.VOLDEMORT_V1),
                                                        socketPool,
                                                        false);
        RequestHandlerFactory factory = ServerTestUtils.getSocketRequestHandlerFactory(repository);
        AbstractSocketService socketService = ServerTestUtils.getSocketService(useNio,
                                                                               factory,
                                                                               6666,
                                                                               50,
                                                                               50,
                                                                               1000);
        socketService.start();

        PerformanceTest socketWriteTest = new PerformanceTest() {

            @Override
            public void doOperation(int i) {
                byte[] bytes = String.valueOf(i).getBytes();
                ByteArray key = new ByteArray(bytes);
                socketStore.put(key, new Versioned<byte[]>(bytes), null);
            }
        };
        System.out.println("###########################################");
        System.out.println("Performing socket write test.");
        socketWriteTest.run(numRequests, numThreads);
        socketWriteTest.printStats();
        System.out.println();

        PerformanceTest socketReadTest = new PerformanceTest() {

            @Override
            public void doOperation(int i) {
                try {
                    socketStore.get(TestUtils.toByteArray(String.valueOf(i)), null);
                } catch(Exception e) {
                    System.out.println("Failure on i = " + i);
                    e.printStackTrace();
                }
            }
        };
        System.out.println("Performing socket read test.");
        socketReadTest.run(numRequests, 1);
        socketReadTest.printStats();
        System.out.println();
        System.out.println();

        socketStore.close();
        socketPool.close();
        socketService.stop();

        /** * Do HTTP tests ** */
        repository.addLocalStore(new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName));
        HttpService httpService = new HttpService(null,
                                                  null,
                                                  repository,
                                                  RequestFormatType.VOLDEMORT_V0,
                                                  numThreads,
                                                  8080);
        httpService.start();
        HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
        HttpClientParams clientParams = httpClient.getParams();
        clientParams.setParameter(HttpMethodParams.RETRY_HANDLER,
                                  new DefaultHttpMethodRetryHandler(0, false));
        clientParams.setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        clientParams.setParameter("http.useragent", "test-agent");
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
        httpClient.setHostConfiguration(hostConfig);
        HttpConnectionManagerParams managerParams = httpClient.getHttpConnectionManager()
                                                              .getParams();
        managerParams.setConnectionTimeout(10000);
        managerParams.setMaxTotalConnections(numThreads);
        managerParams.setStaleCheckingEnabled(false);
        managerParams.setMaxConnectionsPerHost(httpClient.getHostConfiguration(), numThreads);
        final HttpStore httpStore = new HttpStore("test",
                                                  "localhost",
                                                  8080,
                                                  httpClient,
                                                  new RequestFormatFactory().getRequestFormat(RequestFormatType.VOLDEMORT_V0),
                                                  false);
        Thread.sleep(400);

        PerformanceTest httpWriteTest = new PerformanceTest() {

            @Override
            public void doOperation(int i) {
                byte[] key = String.valueOf(i).getBytes();
                httpStore.put(new ByteArray(key), new Versioned<byte[]>(key), null);
            }
        };
        System.out.println("###########################################");
        System.out.println("Performing HTTP write test.");
        httpWriteTest.run(numRequests, numThreads);
        httpWriteTest.printStats();
        System.out.println();

        PerformanceTest httpReadTest = new PerformanceTest() {

            @Override
            public void doOperation(int i) {
                httpStore.get(new ByteArray(String.valueOf(i).getBytes()), null);
            }
        };
        System.out.println("Performing HTTP read test.");
        httpReadTest.run(numRequests, numThreads);
        httpReadTest.printStats();

        httpService.stop();
    }
}
