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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.rebalance.DefaultReabalnceClient;
import voldemort.client.rebalance.RebalanceClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortServer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

/**
 * Things to test purely functional
 * <ul>
 * <li> simple steal/donate partitions with checks <br>
 * new nodes have the keys <br>
 * old nodes throw invalid metadata exception if queried.</li>
 * <li> redirect get while stealing/donating </li>
 * <li> rollback strategy .. kill server in midflight </li>
 * <li> cannot start 2 rebalancing on same node</li>
 * </ul>
 * 
 * <imp> All socket timeouts are kept high as the streaming read/write bandwidth
 * is kept small on purpose
 * 
 * @author bbansal
 * 
 */
public class RebalancingClientTest extends TestCase {

    private static String storeName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    VoldemortServer server0;
    VoldemortServer server1;
    RebalanceClient rebalanceClient;

    @Override
    public void setUp() throws IOException {
        // start 2 node cluster with free ports
        int[] ports = ServerTestUtils.findFreePorts(3);
        Node node0 = new Node(0,
                              "localhost",
                              ports[0],
                              ports[1],
                              ports[2],
                              Arrays.asList(new Integer[] { 0, 1 }));

        ports = ServerTestUtils.findFreePorts(3);
        Node node1 = new Node(1,
                              "localhost",
                              ports[0],
                              ports[1],
                              ports[2],
                              Arrays.asList(new Integer[] { 2, 3 }));

        Cluster cluster = new Cluster("rebalancing-client-test", Arrays.asList(new Node[] { node0,
                node1 }));

        server0 = new VoldemortServer(ServerTestUtils.createServerConfig(0,
                                                                         TestUtils.createTempDir()
                                                                                  .getAbsolutePath(),
                                                                         null,
                                                                         storesXmlfile),
                                      cluster);
        server0.start();

        server1 = new VoldemortServer(ServerTestUtils.createServerConfig(1,
                                                                         TestUtils.createTempDir()
                                                                                  .getAbsolutePath(),
                                                                         null,
                                                                         storesXmlfile),
                                      cluster);
        server1.start();

        // create RebalanceClient with high timeouts
        rebalanceClient = new DefaultReabalnceClient(0,
                                                     server0.getVoldemortMetadata(),
                                                     new SocketPool(1, 2, 20000, 10000, 32 * 1024));
    }

    @Override
    public void tearDown() {
        try {
            server0.stop();
            server1.stop();
        } catch(Exception e) {
            // ignore exceptions here
        }
    }

    /**
     * Generate Random Keys and populate them in server0 or server 1 depending
     * on the partition. The keys are inserted with replication 1.
     * 
     * @param total_keys
     * @param storeName
     * @return
     */
    private Map<ByteArray, byte[]> populateTestKeys(int total_keys, String storeName) {
        Map<ByteArray, byte[]> keyValMap = new HashMap<ByteArray, byte[]>();
        // get store Engine for both servers
        Store<ByteArray, byte[]> store0 = server0.getStoreRepository().getStorageEngine(storeName);
        Store<ByteArray, byte[]> store1 = server1.getStoreRepository().getStorageEngine(storeName);
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server0.getVoldemortMetadata()
                                                                               .getCurrentCluster()
                                                                               .getNodes(),
                                                                        1);

        // enter data into server 1 & 2
        for(int i = 0; i <= total_keys; i++) {
            int rand_key = (int) (Math.random() * Integer.MAX_VALUE);
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + rand_key, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + rand_key, "UTF-8");

            if(!keyValMap.containsKey(key)) {
                keyValMap.put(key, value);

                int keyPartition = routingStrategy.getPartitionList(key.get()).get(0);
                switch(keyPartition) {
                    case 0: /* Intentional Fall through */
                    case 1:
                        store0.put(new ByteArray(key.get()), new Versioned<byte[]>(value));
                        break;
                    case 2:/* Intentional Fall through */
                    case 3:
                        store1.put(new ByteArray(key.get()), new Versioned<byte[]>(value));
                        break;
                }
            }
        }
        return keyValMap;
    }

    /**
     * Test basic steal process and verify all keys for stolen partitions are
     * present at stealerNode and the donatingNode throws
     * InvalidMetadataException for all stolen keys
     * 
     * @throws IOException
     */
    public void testStealPartitions() {

        // insert some data into each servers
        Map<ByteArray, byte[]> keyValMap = populateTestKeys(1000, storeName);

        // check all keys for partition 0,1,2 are present in server0
        checkAllKeysArePresent(server0, Arrays.asList(0, 1), keyValMap);

        // check all keys for partition 3 are present in server1
        checkAllKeysArePresent(server1, Arrays.asList(2, 3), keyValMap);

        // Lets steal partition 2 from server 1

        // step 0. create target cluster Node0 {0, 1, 2} Node 1{3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        Cluster targetCluster = new Cluster("rebalancing-client-test-steal",
                                            Arrays.asList(new Node[] {
                                                    new Node(node0.getId(),
                                                             node0.getHost(),
                                                             node0.getHttpPort(),
                                                             node0.getSocketPort(),
                                                             node0.getAdminPort(),
                                                             Arrays.asList(0, 1, 2)),
                                                    new Node(node1.getId(),
                                                             node1.getHost(),
                                                             node1.getHttpPort(),
                                                             node1.getSocketPort(),
                                                             node1.getAdminPort(),
                                                             Arrays.asList(3)) }));

        // do steal partitions
        rebalanceClient.stealPartitions(0, storeName, server0.getVoldemortMetadata()
                                                             .getCurrentCluster(), targetCluster);

        // check all keys for partition 0,1,2 are present in server0
        checkAllKeysArePresent(server0, Arrays.asList(0, 1, 2), keyValMap);

        // check all keys for partition 3 are present in server1
        checkAllKeysArePresent(server1, Arrays.asList(3), keyValMap);

        // check server0 throw InvalidMetadataException for partition {3}
        checkNoKeyReturns(server0, Arrays.asList(3), keyValMap);

        // check server1 throw InvalidMetadataException for partition {0, 1, 2}
        checkNoKeyReturns(server1, Arrays.asList(0, 1, 2), keyValMap);

    }

    /**
     * Test basic donate process and verify all keys for stolen partitions are
     * present at stealerNode and the donatingNode throws
     * InvalidMetadataException for all stolen keys
     */
    public void testDonatePartitions() {

        // insert some data into each servers
        Map<ByteArray, byte[]> keyValMap = populateTestKeys(1000, storeName);

        // check all keys for partition 0,1,2 are present in server0
        checkAllKeysArePresent(server0, Arrays.asList(0, 1), keyValMap);

        // check all keys for partition 3 are present in server1
        checkAllKeysArePresent(server1, Arrays.asList(2, 3), keyValMap);

        // step 0. create target cluster Node0 {0} Node 1{1,2 3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        Cluster targetCluster = new Cluster("rebalancing-client-test-donate",
                                            Arrays.asList(new Node[] {
                                                    new Node(node0.getId(),
                                                             node0.getHost(),
                                                             node0.getHttpPort(),
                                                             node0.getSocketPort(),
                                                             node0.getAdminPort(),
                                                             Arrays.asList(0)),
                                                    new Node(node1.getId(),
                                                             node1.getHost(),
                                                             node1.getHttpPort(),
                                                             node1.getSocketPort(),
                                                             node1.getAdminPort(),
                                                             Arrays.asList(1, 2, 3)) }));

        // do donate partitions
        rebalanceClient.donatePartitions(0, storeName, server0.getVoldemortMetadata()
                                                              .getCurrentCluster(), targetCluster);

        // check all keys for partition 0 are present in server0
        checkAllKeysArePresent(server0, Arrays.asList(0), keyValMap);

        // check all keys for partition 1, 2 , 3 are present in server1
        checkAllKeysArePresent(server1, Arrays.asList(1, 2, 3), keyValMap);

        // check server0 throw InvalidMetadataException for partition {1, 2, 3}
        checkNoKeyReturns(server0, Arrays.asList(1, 2, 3), keyValMap);

        // check server1 throw InvalidMetadataException for partition {0}
        checkNoKeyReturns(server1, Arrays.asList(0), keyValMap);
    }

    /**
     * Test Redirect Get while stealing
     * 
     * @throws IOException
     */
    public void testRedirectGetRoutingWhileStealing() {

        // insert some data into each servers
        final Map<ByteArray, byte[]> keyValMap = populateTestKeys(10000, storeName);
        // step 0. create target cluster Node0 {0, 1, 2} Node 1{3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        final Cluster targetCluster = new Cluster("rebalancing-client-test-steal",
                                                  Arrays.asList(new Node[] {
                                                          new Node(node0.getId(),
                                                                   node0.getHost(),
                                                                   node0.getHttpPort(),
                                                                   node0.getSocketPort(),
                                                                   node0.getAdminPort(),
                                                                   Arrays.asList(0, 1, 2)),
                                                          new Node(node1.getId(),
                                                                   node1.getHost(),
                                                                   node1.getHttpPort(),
                                                                   node1.getSocketPort(),
                                                                   node1.getAdminPort(),
                                                                   Arrays.asList(3)) }));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        // do steal Partitions and redirect get request in parallel.
        executor.execute(new Runnable() {

            public void run() {
                rebalanceClient.stealPartitions(0,
                                                storeName,
                                                server0.getVoldemortMetadata().getCurrentCluster(),
                                                targetCluster);
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                checkAllKeysArePresent(server0, Arrays.asList(2), keyValMap);
            }

        });
    }

    /**
     * Test Redirect Get while donating
     * 
     * @throws IOException
     */
    public void testRedirectGetRoutingWhileDonating() {
        // insert some data into each servers
        final Map<ByteArray, byte[]> keyValMap = populateTestKeys(1000, storeName);

        // step 0. create target cluster Node0 {0} Node 1{1,2 3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        final Cluster targetCluster = new Cluster("rebalancing-client-test-donate",
                                                  Arrays.asList(new Node[] {
                                                          new Node(node0.getId(),
                                                                   node0.getHost(),
                                                                   node0.getHttpPort(),
                                                                   node0.getSocketPort(),
                                                                   node0.getAdminPort(),
                                                                   Arrays.asList(0)),
                                                          new Node(node1.getId(),
                                                                   node1.getHost(),
                                                                   node1.getHttpPort(),
                                                                   node1.getSocketPort(),
                                                                   node1.getAdminPort(),
                                                                   Arrays.asList(1, 2, 3)) }));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        // do donate Partitions and redirect get request in parallel.
        executor.execute(new Runnable() {

            public void run() {
                rebalanceClient.donatePartitions(0,
                                                 storeName,
                                                 server0.getVoldemortMetadata().getCurrentCluster(),
                                                 targetCluster);
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                checkAllKeysArePresent(server1, Arrays.asList(1), keyValMap);
            }
        });
    }

    /**
     * Test Rollback while stealing
     * 
     * @throws InterruptedException
     * 
     * @throws IOException
     */
    public void testRollbackWhileStealing() throws InterruptedException {

        // insert some data into each servers
        final Map<ByteArray, byte[]> keyValMap = populateTestKeys(10000, storeName);
        // step 0. create target cluster Node0 {0, 1, 2} Node 1{3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        final Cluster targetCluster = new Cluster("rebalancing-client-test-steal",
                                                  Arrays.asList(new Node[] {
                                                          new Node(node0.getId(),
                                                                   node0.getHost(),
                                                                   node0.getHttpPort(),
                                                                   node0.getSocketPort(),
                                                                   node0.getAdminPort(),
                                                                   Arrays.asList(0, 1, 2)),
                                                          new Node(node1.getId(),
                                                                   node1.getHost(),
                                                                   node1.getHttpPort(),
                                                                   node1.getSocketPort(),
                                                                   node1.getAdminPort(),
                                                                   Arrays.asList(3)) }));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        // do steal Partitions and redirect get request in parallel.
        executor.execute(new Runnable() {

            public void run() {
                try {
                    rebalanceClient.stealPartitions(0,
                                                    storeName,
                                                    server0.getVoldemortMetadata()
                                                           .getCurrentCluster(),
                                                    targetCluster);
                } catch(Exception e) {
                    // ignore this
                }
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                server1.stop();
            }
        });

        executor.awaitTermination(60, TimeUnit.SECONDS);

        // check all keys for partition 0, 1 are present in server0
        checkAllKeysArePresent(server0, Arrays.asList(0, 1), keyValMap);

        // check server0 throw InvalidMetadataException for partition{2,3}
        checkNoKeyReturns(server0, Arrays.asList(2, 3), keyValMap);
    }

    /**
     * Test Rollback while donating
     * 
     * @throws InterruptedException
     * 
     * @throws IOException
     */
    public void testRollBackWhileDonating() throws InterruptedException {
        // insert some data into each servers
        final Map<ByteArray, byte[]> keyValMap = populateTestKeys(1000, storeName);

        // step 0. create target cluster Node0 {0} Node 1{1,2 3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        final Cluster targetCluster = new Cluster("rebalancing-client-test-donate",
                                                  Arrays.asList(new Node[] {
                                                          new Node(node0.getId(),
                                                                   node0.getHost(),
                                                                   node0.getHttpPort(),
                                                                   node0.getSocketPort(),
                                                                   node0.getAdminPort(),
                                                                   Arrays.asList(0)),
                                                          new Node(node1.getId(),
                                                                   node1.getHost(),
                                                                   node1.getHttpPort(),
                                                                   node1.getSocketPort(),
                                                                   node1.getAdminPort(),
                                                                   Arrays.asList(1, 2, 3)) }));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        // do donate Partitions and redirect get request in parallel.
        executor.execute(new Runnable() {

            public void run() {
                rebalanceClient.donatePartitions(0,
                                                 storeName,
                                                 server0.getVoldemortMetadata().getCurrentCluster(),
                                                 targetCluster);
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                server1.stop();
            }
        });

        executor.awaitTermination(60, TimeUnit.SECONDS);

        // check all keys for partition 0, 1 are present in server0
        checkAllKeysArePresent(server0, Arrays.asList(0, 1), keyValMap);

        // check server0 throw InvalidMetadataException for partition{2,3}
        checkNoKeyReturns(server0, Arrays.asList(2, 3), keyValMap);
    }

    /**
     * Test try to start multiple rebalancing request at same node
     * 
     * @throws InterruptedException
     * 
     * @throws IOException
     */
    public void testMultipleRebalanceRequest() throws InterruptedException {

        // insert some data into each servers
        final Map<ByteArray, byte[]> keyValMap = populateTestKeys(10000, storeName);
        // step 0. create target cluster Node0 {0, 1, 2} Node 1{3}
        Node node0 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(0);
        Node node1 = server0.getVoldemortMetadata().getCurrentCluster().getNodeById(1);
        final Cluster targetCluster1 = new Cluster("rebalancing-client-test-steal",
                                                   Arrays.asList(new Node[] {
                                                           new Node(node0.getId(),
                                                                    node0.getHost(),
                                                                    node0.getHttpPort(),
                                                                    node0.getSocketPort(),
                                                                    node0.getAdminPort(),
                                                                    Arrays.asList(0, 1, 2)),
                                                           new Node(node1.getId(),
                                                                    node1.getHost(),
                                                                    node1.getHttpPort(),
                                                                    node1.getSocketPort(),
                                                                    node1.getAdminPort(),
                                                                    Arrays.asList(3)) }));

        final Cluster targetCluster2 = new Cluster("rebalancing-client-test-donate",
                                                   Arrays.asList(new Node[] {
                                                           new Node(node0.getId(),
                                                                    node0.getHost(),
                                                                    node0.getHttpPort(),
                                                                    node0.getSocketPort(),
                                                                    node0.getAdminPort(),
                                                                    Arrays.asList(0)),
                                                           new Node(node1.getId(),
                                                                    node1.getHost(),
                                                                    node1.getHttpPort(),
                                                                    node1.getSocketPort(),
                                                                    node1.getAdminPort(),
                                                                    Arrays.asList(1, 2, 3)) }));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        // do steal Partitions and redirect get request in parallel.
        executor.execute(new Runnable() {

            public void run() {
                rebalanceClient.stealPartitions(0,
                                                storeName,
                                                server0.getVoldemortMetadata().getCurrentCluster(),
                                                targetCluster1);
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                try {
                    rebalanceClient.stealPartitions(0,
                                                    storeName,
                                                    server0.getVoldemortMetadata()
                                                           .getCurrentCluster(),
                                                    targetCluster2);
                    fail("Should not be able to start multiple rebalancing on one node.");
                } catch(VoldemortException e) {
                    // ignore this
                }
            }
        });

        executor.awaitTermination(60, TimeUnit.SECONDS);

        // check stealing finished successfully
        checkAllKeysArePresent(server0, Arrays.asList(0, 1, 2), keyValMap);
        checkAllKeysArePresent(server1, Arrays.asList(3), keyValMap);
        checkNoKeyReturns(server0, Arrays.asList(3), keyValMap);
        checkNoKeyReturns(server1, Arrays.asList(0, 1, 2), keyValMap);
    }

    private void checkAllKeysArePresent(VoldemortServer server,
                                        List<Integer> checkPartitionsList,
                                        Map<ByteArray, byte[]> keyValMap) {
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server.getVoldemortMetadata()
                                                                              .getCurrentCluster()
                                                                              .getNodes(),
                                                                        1);
        SocketStore socketStore = ServerTestUtils.getSocketStore(storeName,
                                                                 server.getIdentityNode()
                                                                       .getSocketPort(),
                                                                 10000);
        for(Entry<ByteArray, byte[]> entry: keyValMap.entrySet()) {
            int keyPartition = routingStrategy.getPartitionList(entry.getKey().get()).get(0);
            if(checkPartitionsList.contains(keyPartition)) {
                assertEquals("Exactly one pair should be present on server:"
                             + server.getIdentityNode().getId() + " for key in partition:"
                             + keyPartition, 1, socketStore.get(entry.getKey()).size());
            }
        }
    }

    private void checkNoKeyReturns(VoldemortServer server,
                                   List<Integer> checkPartitionsList,
                                   Map<ByteArray, byte[]> keyValMap) {
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server.getVoldemortMetadata()
                                                                              .getCurrentCluster()
                                                                              .getNodes(),
                                                                        1);
        SocketStore socketStore = ServerTestUtils.getSocketStore(storeName,
                                                                 server.getIdentityNode()
                                                                       .getSocketPort(),
                                                                 10000);
        for(Entry<ByteArray, byte[]> entry: keyValMap.entrySet()) {
            int keyPartition = routingStrategy.getPartitionList(entry.getKey().get()).get(0);
            if(checkPartitionsList.contains(keyPartition)) {
                try {
                    socketStore.get(entry.getKey());
                    fail("Key not belonging to this partition should throw InvalidMetadataException:");
                } catch(InvalidMetadataException e) {
                    // this is expected
                }
            }
        }
    }
}
