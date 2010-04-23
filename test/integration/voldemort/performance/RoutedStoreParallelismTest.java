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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import voldemort.utils.Time;
import voldemort.xml.StoreDefinitionsMapper;

public class RoutedStoreParallelismTest {

    public static void main(String[] args) throws Throwable {
        int argsIndex = 0;
        int numKeys = Integer.parseInt(args[argsIndex++]);
        boolean usePipeline = args[argsIndex++].equals("pipeline");

        final List<ByteArray> keys = new ArrayList<ByteArray>();

        for(int i = 0; i < numKeys; i++) {
            ByteArray key = new ByteArray(("test-key-" + i).getBytes());
            keys.add(key);
        }

        ClientConfig clientConfig = new ClientConfig();
        Map<Integer, VoldemortServer> serverMap = new HashMap<Integer, VoldemortServer>();
        Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0 }, { 1 } });
        String storeDefinitionFile = "test/common/voldemort/config/single-store.xml";
        StoreDefinition storeDefinition = new StoreDefinitionsMapper().readStoreList(new File(storeDefinitionFile))
                                                                      .get(0);

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(clientConfig.getMaxConnectionsPerNode(),
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
            store = new SleepyStore<ByteArray, byte[]>(500 * i, store);

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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(usePipeline,
                                                                       routedStoreThreadPool,
                                                                       clientConfig.getRoutingTimeout(TimeUnit.MILLISECONDS));

        final RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                                  storeDefinition,
                                                                  stores,
                                                                  true,
                                                                  failureDetector);

        int numClients = clientConfig.getMaxThreads() * 4;
        System.err.println("Type of routed store: " + routedStore.getClass() + " running with "
                           + numClients + " clients and " + clientConfig.getMaxThreads()
                           + " routed store threads");

        ExecutorService runner = Executors.newFixedThreadPool(numClients);
        long start = System.nanoTime();

        try {
            for(int i = 0; i < numClients; i++) {
                runner.submit(new Runnable() {

                    public void run() {
                        for(ByteArray key: keys) {
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

            System.err.println(time);
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

}
