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

package voldemort.performance;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.TimeoutConfig;
import voldemort.utils.CmdUtils;

/**
 * Stress tests the client. Intended to diagnose issues such as connection leaks
 */
public class ClientConnectionStressTest {

    private static final String CONNECTION_TIMEOUT = "connection-timeout";
    private static final String ROUTING_TIMEOUT = "routing-timeout";
    private static final String SOCKET_TIMEOUT = "socket-timeout";
    private static final String MAX_CONNECTIONS = "max-connections";
    private static final String MAX_CONNECTIONS_TOTAL = "max-connections-total";
    private static final String MAX_THREADS = "max-threads";
    private static final String SELECTORS = "selectors";
    private static final String SOCKET_BUFFER_SIZE = "socket-buffer-size";
    private static final String REQS = "reqs";
    private static final String CONNECTIONS = "connections";

    private final String storeName;
    private final int connsParallel;
    private final int connsTotal;
    private final int reqsPerConn;
    private final ExecutorService executor;
    private final StoreClientFactory factory;

    public ClientConnectionStressTest(ClientConfig config,
                                      String storeName,
                                      int connsParallel,
                                      int connsTotal,
                                      int reqsPerConn) {
        this.storeName = storeName;
        this.connsParallel = connsParallel;
        this.connsTotal = connsTotal;
        this.reqsPerConn = reqsPerConn;
        this.executor = Executors.newFixedThreadPool(connsParallel);
        this.factory = new SocketStoreClientFactory(config);
    }

    public void execute() throws Exception {

        for(int i = 0; i < connsTotal; i++) {
            final CountDownLatch latch = new CountDownLatch(connsParallel);

            for(int j = 0; j < connsParallel; j++) {
                System.out.println("Connection " + (i + j));
                executor.submit(new Runnable() {

                    public void run() {
                        try {
                            StoreClient<String, String> client = factory.getStoreClient(storeName);

                            for(int k = 0; k < reqsPerConn; k++)
                                client.get(Integer.toString(k));
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            latch.await();
        }
        executor.shutdown();
    }

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts(CONNECTION_TIMEOUT, "Connection timeout (ms)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(ROUTING_TIMEOUT, "Routing timeout (ms)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(SOCKET_TIMEOUT, "Socket timeout (ms)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(MAX_CONNECTIONS, "Max connections per node")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(MAX_CONNECTIONS_TOTAL, "Max total connections")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(MAX_THREADS, "Max threads").withRequiredArg().ofType(Integer.class);
        parser.accepts(SELECTORS, "Number of NIO selectors")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(SOCKET_BUFFER_SIZE, "Socket buffer size")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(REQS, "Requests per session").withRequiredArg().ofType(Integer.class);
        parser.accepts(CONNECTIONS, "Total connections to make")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("help");

        OptionSet options = parser.parse(args);
        List<String> rest = options.nonOptionArguments();
        if(rest.size() < 2 || options.has("help")) {
            parser.printHelpOn(System.err);
            System.err.println("Usage: ClientConnectionStressTest <options> url store-name");
            System.exit(0);
        }

        String url = rest.get(0);
        String storeName = rest.get(1);
        Integer connsTotal = CmdUtils.valueOf(options, CONNECTIONS, 100);
        Integer reqs = CmdUtils.valueOf(options, REQS, 1000);

        ClientConfig config = new ClientConfig();
        if(options.has(CONNECTION_TIMEOUT))
            config.setConnectionTimeout((Integer) options.valueOf(CONNECTION_TIMEOUT),
                                        TimeUnit.MILLISECONDS);
        if(options.has(ROUTING_TIMEOUT))
            config.setTimeoutConfig(new TimeoutConfig(TimeUnit.MILLISECONDS.toMillis((Integer) options.valueOf(ROUTING_TIMEOUT)),
                                                      false,
                                                      false));

        if(options.has(SOCKET_TIMEOUT))
            config.setSocketTimeout((Integer) options.valueOf(SOCKET_TIMEOUT),
                                    TimeUnit.MILLISECONDS);
        if(options.has(MAX_CONNECTIONS))
            config.setMaxConnectionsPerNode((Integer) options.valueOf(MAX_CONNECTIONS));
        if(options.has(MAX_THREADS))
            config.setMaxThreads((Integer) options.valueOf(MAX_THREADS));
        if(options.has(SELECTORS))
            config.setSelectors((Integer) options.valueOf(SELECTORS));
        if(options.has(SOCKET_BUFFER_SIZE))
            config.setSocketBufferSize((Integer) options.valueOf(SOCKET_BUFFER_SIZE));
        config.setBootstrapUrls(url);

        ClientConnectionStressTest test = new ClientConnectionStressTest(config,
                                                                         storeName,
                                                                         config.getMaxThreads(),
                                                                         connsTotal,
                                                                         reqs);
        test.execute();
    }
}
