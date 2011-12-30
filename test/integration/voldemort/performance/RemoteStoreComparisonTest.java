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

import org.apache.http.HttpVersion;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.http.HttpService;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
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
        SocketStoreFactory storeFactory = new ClientRequestExecutorPool(10, 1000, 1000, 32 * 1024);
        final Store<ByteArray, byte[], byte[]> socketStore = storeFactory.create(storeName,
                                                                                 "localhost",
                                                                                 6666,
                                                                                 RequestFormatType.VOLDEMORT_V1,
                                                                                 RequestRoutingType.NORMAL);
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
        storeFactory.close();
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

        ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager();
        DefaultHttpClient httpClient = new DefaultHttpClient(connectionManager);

        HttpParams clientParams = httpClient.getParams();
        httpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
        HttpClientParams.setCookiePolicy(clientParams, CookiePolicy.IGNORE_COOKIES);
        clientParams.setParameter(CoreProtocolPNames.USER_AGENT, "test-agent");
        clientParams.setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
        HttpConnectionParams.setConnectionTimeout(clientParams, 10000);

        connectionManager.setMaxTotal(numThreads);
        connectionManager.setDefaultMaxPerRoute(numThreads);
        HttpConnectionParams.setStaleCheckingEnabled(clientParams, false);

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
