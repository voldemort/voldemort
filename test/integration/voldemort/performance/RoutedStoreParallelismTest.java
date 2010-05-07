/*
 * Copyright 2010 LinkedIn, Inc
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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.MutableStoreVerifier;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.routed.RoutedStore;
import voldemort.store.routed.RoutedStoreFactory;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.xml.StoreDefinitionsMapper;

public class RoutedStoreParallelismTest {

    private final static String THREAD_POOL_ROUTED_STORE = "threaded";
    private final static String PIPELINE_ROUTED_STORE = "pipeline";
    private final static int DEFAULT_NUM_KEYS = 50;
    private final static int DEFAULT_MAX_CONNECTIONS = new ClientConfig().getMaxConnectionsPerNode();
    private final static int DEFAULT_MAX_THREADS = new ClientConfig().getMaxThreads();
    private final static int DEFAULT_NUM_NODES = 2;
    private final static int DEFAULT_NUM_SLOW_NODES = 1;
    private final static int DEFAULT_DELAY = 500;
    private final static int DEFAULT_NUM_CLIENTS = 20;
    private final static String DEFAULT_ROUTED_STORE_TYPE = PIPELINE_ROUTED_STORE;

    public static void main(String[] args) throws Throwable {
        OptionParser parser = new OptionParser();
        parser.accepts("num-keys",
                       "The number of keys to submit for retrieval  Default = " + DEFAULT_NUM_KEYS)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("max-connections",
                       "The maximum number of connections (sockets) per node; same value as client configuration parameter \""
                               + ClientConfig.MAX_CONNECTIONS_PER_NODE_PROPERTY
                               + "\"  Default = "
                               + DEFAULT_MAX_CONNECTIONS)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("max-threads",
                       "The maximum number of threads used by the threaded RoutedStore implementation; same value as client configuration parameter \""
                               + ClientConfig.MAX_THREADS_PROPERTY
                               + "\"  Default = "
                               + DEFAULT_MAX_THREADS)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("num-nodes", "The number of nodes  Default = " + DEFAULT_NUM_NODES)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("num-slow-nodes",
                       "The number of nodes that exhibit delay Default = " + DEFAULT_NUM_SLOW_NODES)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("delay",
                       "The millisecond delay shown by slow nodes Default = " + DEFAULT_DELAY)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("num-clients",
                       "The number of threads to make requests concurrently  Default = "
                               + DEFAULT_NUM_CLIENTS).withRequiredArg().ofType(Integer.class);
        parser.accepts("routed-store-type",
                       "Type of routed store, either \"" + THREAD_POOL_ROUTED_STORE + "\" or \""
                               + PIPELINE_ROUTED_STORE + "\"  Default = "
                               + DEFAULT_ROUTED_STORE_TYPE).withRequiredArg();
        parser.accepts("help", "This help");

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            printUsage(System.out, parser);
        }

        final int numKeys = CmdUtils.valueOf(options, "num-keys", DEFAULT_NUM_KEYS);
        int maxConnectionsPerNode = CmdUtils.valueOf(options,
                                                     "max-connections",
                                                     DEFAULT_MAX_CONNECTIONS);
        int maxThreads = CmdUtils.valueOf(options, "max-threads", DEFAULT_MAX_THREADS);
        int numNodes = CmdUtils.valueOf(options, "num-nodes", DEFAULT_NUM_NODES);
        int numSlowNodes = CmdUtils.valueOf(options, "num-slow-nodes", DEFAULT_NUM_SLOW_NODES);
        int delay = CmdUtils.valueOf(options, "delay", DEFAULT_DELAY);
        int numClients = CmdUtils.valueOf(options, "num-clients", DEFAULT_NUM_CLIENTS);
        String routedStoreType = CmdUtils.valueOf(options,
                                                  "routed-store-type",
                                                  DEFAULT_ROUTED_STORE_TYPE);

        System.err.println("num-keys : " + numKeys);
        System.err.println("max-connections : " + maxConnectionsPerNode);
        System.err.println("max-threads : " + maxThreads);
        System.err.println("num-nodes : " + numNodes);
        System.err.println("num-slow-nodes : " + numSlowNodes);
        System.err.println("delay : " + delay);
        System.err.println("num-clients : " + numClients);
        System.err.println("routed-store-type : " + routedStoreType);

        ClientConfig clientConfig = new ClientConfig().setMaxConnectionsPerNode(maxConnectionsPerNode)
                                                      .setMaxThreads(maxThreads);

        Map<Integer, VoldemortServer> serverMap = new HashMap<Integer, VoldemortServer>();

        int[][] partitionMap = new int[numNodes][1];

        for(int i = 0; i < numNodes; i++) {
            partitionMap[i][0] = i;
        }

        Cluster cluster = ServerTestUtils.getLocalCluster(numNodes, partitionMap);
        String storeDefinitionFile = "test/common/voldemort/config/single-store.xml";
        StoreDefinition storeDefinition = new StoreDefinitionsMapper().readStoreList(new File(storeDefinitionFile))
                                                                      .get(0);

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(clientConfig.getSelectors(),
                                                                              clientConfig.getMaxConnectionsPerNode(),
                                                                              clientConfig.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                                              clientConfig.getSocketTimeout(TimeUnit.MILLISECONDS),
                                                                              clientConfig.getSocketBufferSize(),
                                                                              clientConfig.getSocketKeepAlive());

        for(int i = 0; i < cluster.getNumberOfNodes(); i++) {
            VoldemortConfig config = ServerTestUtils.createServerConfig(true,
                                                                        i,
                                                                        TestUtils.createTempDir()
                                                                                 .getAbsolutePath(),
                                                                        null,
                                                                        storeDefinitionFile,
                                                                        new Properties());

            VoldemortServer server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                          config,
                                                                          cluster);
            serverMap.put(i, server);

            Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>("test-sleepy");

            if(i < numSlowNodes)
                store = new SleepyStore<ByteArray, byte[]>(delay, store);

            StoreRepository storeRepository = server.getStoreRepository();
            storeRepository.addLocalStore(store);
        }

        Map<Integer, Store<ByteArray, byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[]>>();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> socketStore = ServerTestUtils.getSocketStore(socketStoreFactory,
                                                                                  "test-sleepy",
                                                                                  node.getSocketPort(),
                                                                                  clientConfig.getRequestFormatType());
            stores.put(node.getId(), socketStore);
        }

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(BannagePeriodFailureDetector.class.getName())
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setStoreVerifier(MutableStoreVerifier.create(stores));
        FailureDetector failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);

        ExecutorService routedStoreThreadPool = Executors.newFixedThreadPool(clientConfig.getMaxThreads());
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(routedStoreType.trim()
                                                                                      .equalsIgnoreCase(PIPELINE_ROUTED_STORE),
                                                                       routedStoreThreadPool,
                                                                       clientConfig.getRoutingTimeout(TimeUnit.MILLISECONDS));

        final RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                                  storeDefinition,
                                                                  stores,
                                                                  true,
                                                                  failureDetector);

        ExecutorService runner = Executors.newFixedThreadPool(numClients);
        long start = System.nanoTime();

        try {
            for(int i = 0; i < numClients; i++) {
                runner.submit(new Runnable() {

                    public void run() {

                        for(int i = 0; i < numKeys; i++) {
                            ByteArray key = new ByteArray(("test-key-" + i).getBytes());
                            try {
                                routedStore.get(key);
                            } catch(VoldemortException e) {
                                // 
                            }
                        }
                    }

                });
            }

            runner.shutdown();
            runner.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            long time = (System.nanoTime() - start) / Time.NS_PER_MS;

            System.err.println("Time: " + time + " ms.");
        } finally {
            runner.shutdown();
        }

        if(failureDetector != null)
            failureDetector.destroy();

        for(VoldemortServer server: serverMap.values())
            server.stop();

        if(routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();

        System.exit(0);
    }

    private static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/bin/run-class.sh "
                    + RoutedStoreParallelismTest.class.getName() + " [options]\n");
        parser.printHelpOn(out);
        System.exit(1);
    }

}
