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

package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.json.JsonReader;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.readonly.JsonStoreBuilder;
import voldemort.store.readonly.ReadOnlyStorageEngineTestInstance;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

public abstract class AbstractRebalanceTest {

    protected static int NUM_KEYS = 100;
    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRO = "test-ro";
    protected static String storeDefFile = "test/common/voldemort/config/two-stores.xml";
    private List<StoreDefinition> storeDefs;
    protected SocketStoreFactory socketStoreFactory;
    HashMap<String, String> testEntries;

    @Before
    public void setUp() throws IOException {
        testEntries = ServerTestUtils.createRandomKeyValueString(NUM_KEYS);
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
        storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storeDefFile));
    }

    @After
    public void tearDown() {
        testEntries.clear();
        socketStoreFactory.close();
    }

    protected abstract Cluster startServers(Cluster cluster,
                                            String StoreDefXmlFile,
                                            List<Integer> nodeToStart,
                                            Map<String, String> configProps) throws Exception;

    protected abstract void stopServer(List<Integer> nodesToStop) throws Exception;

    protected Cluster updateCluster(Cluster template) {
        return template;
    }

    protected Store<ByteArray, byte[], byte[]> getSocketStore(String storeName,
                                                              String host,
                                                              int port) {
        return getSocketStore(storeName, host, port, false);
    }

    protected Store<ByteArray, byte[], byte[]> getSocketStore(String storeName,
                                                              String host,
                                                              int port,
                                                              boolean isRouted) {
        return ServerTestUtils.getSocketStore(socketStoreFactory,
                                              storeName,
                                              host,
                                              port,
                                              RequestFormatType.PROTOCOL_BUFFERS,
                                              isRouted);
    }

    @Test
    public void testSingleRebalance() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      new RebalanceClientConfig());
        try {
            populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testDeleteAfterRebalancing() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig rebalanceConfig = new RebalanceClientConfig();
        rebalanceConfig.setDeleteAfterRebalancingEnabled(true);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      rebalanceConfig);

        try {
            populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1));

            // check that all keys are partitions 2,3 are Indeeed deleted.
            // assign all partitions to node 0 by force ..
            rebalanceClient.getAdminClient()
                           .updateRemoteCluster(0,
                                                updatedCluster,
                                                ((VectorClock) RebalanceUtils.getLatestCluster(null,
                                                                                               rebalanceClient.getAdminClient())
                                                                             .getVersion()).incremented(0,
                                                                                                        System.currentTimeMillis()));
            checkGetEntries(updatedCluster.getNodeById(0),
                            updatedCluster,
                            Arrays.asList(2, 3),
                            null,
                            false);

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testDeleteAfterRebalancingDisabled() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      new RebalanceClientConfig());
        try {
            populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1));

            // check that all keys are partitions 2,3 are still present -
            // applicable only for Read-write
            // assign all partitions to node 0 by force ..
            rebalanceClient.getAdminClient()
                           .updateRemoteCluster(0,
                                                updatedCluster,
                                                ((VectorClock) RebalanceUtils.getLatestCluster(null,
                                                                                               rebalanceClient.getAdminClient())
                                                                             .getVersion()).incremented(0,
                                                                                                        System.currentTimeMillis()));
            checkGetEntries(updatedCluster.getNodeById(0),
                            updatedCluster,
                            null,
                            Arrays.asList(2, 3),
                            true);

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testMultipleRebalance() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 4, 5, 6 },
                { 2, 3 }, { 7, 8 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1, 2);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      new RebalanceClientConfig());
        try {
            populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1, 2));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testMultipleParallelRebalance() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 4, 5, 6 },
                { 2, 3 }, { 7, 8 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1, 2);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(2);
        config.setMaxParallelDonors(2);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1, 2));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testMultipleDonors() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2 },
                { 1, 3, 5 }, { 4, 6 }, {} });
        Cluster targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0 }, { 1, 3 },
                { 4, 6 }, { 2, 5 } });

        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(2);
        config.setMaxParallelDonors(2);

        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(updatedCluster, Arrays.asList(0, 1, 2), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(3));
        } finally {
            // stop servers
            stopServer(serverList);
        }

    }

    @Test
    public void testMultipleDonorsMultipleStealers() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 4, 6 },
                { 1, 3, 5 }, {}, {} });
        Cluster targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 }, { 1 },
                { 6, 3 }, { 2, 5 } });

        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(2);
        config.setMaxParallelDonors(2);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(updatedCluster, Arrays.asList(0, 1), rebalanceClient.getAdminClient());
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(3));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testProxyGetDuringRebalancing() throws Exception {
        final Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3 }, {} });

        final Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { {},
                { 0, 1, 2, 3 } });

        // start servers 0 , 1 only
        final List<Integer> serverList = Arrays.asList(0, 1);
        final Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

        RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
        rebalanceClientConfig.setMaxParallelDonors(2);
        rebalanceClientConfig.setMaxParallelRebalancing(2);

        final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                            0),
                                                                            rebalanceClientConfig);

        // populate data now.
        populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());

        final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCluster,
                                                                                                                                  0))
                                                                                                .setEnableLazy(false)
                                                                                                .setSocketTimeout(120,
                                                                                                                  TimeUnit.SECONDS));

        final StoreClient<String, String> storeClient = new DefaultStoreClient<String, String>(testStoreNameRW,
                                                                                               null,
                                                                                               factory,
                                                                                               3);
        final boolean[] masterNodeResponded = { false, false };

        // start get operation.
        executors.execute(new Runnable() {

            public void run() {
                try {
                    List<String> keys = new ArrayList<String>(testEntries.keySet());

                    int nRequests = 0;
                    while(!rebalancingToken.get()) {
                        // should always able to get values.
                        int index = (int) (Math.random() * keys.size());

                        // should get a valid value
                        try {
                            nRequests++;
                            Versioned<String> value = storeClient.get(keys.get(index));
                            assertNotSame("StoreClient get() should not return null.", null, value);
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         value);
                            int masterNode = storeClient.getResponsibleNodes(keys.get(index))
                                                        .get(0)
                                                        .getId();
                            masterNodeResponded[masterNode] = true;

                        } catch(Exception e) {
                            System.out.println(e);
                            e.printStackTrace();
                            exceptions.add(e);
                        }
                    }

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    factory.close();
                }
            }

        });

        executors.execute(new Runnable() {

            public void run() {
                try {

                    Thread.sleep(500);

                    rebalanceAndCheck(updatedCluster,
                                      updateCluster(targetCluster),
                                      rebalanceClient,
                                      Arrays.asList(1));

                    Thread.sleep(500);

                    rebalancingToken.set(true);

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers
                    try {
                        stopServer(serverList);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        executors.shutdown();
        executors.awaitTermination(300, TimeUnit.SECONDS);

        assertEquals("Client should see values returned master at both (0,1):("
                             + masterNodeResponded[0] + "," + masterNodeResponded[1] + ")",
                     true,
                     masterNodeResponded[0] && masterNodeResponded[1]);

        // check No Exception
        if(exceptions.size() > 0) {
            for(Exception e: exceptions) {
                e.printStackTrace();
            }
            fail("Should not see any exceptions.");
        }
    }

    @Test
    public void testProxyGetWithMultipleDonors() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 2, 4, 6 },
                { 1, 3, 5 }, {}, {} });
        final Cluster targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 4 },
                { 1 }, { 6, 3 }, { 2, 5 } });

        // start servers 0 , 1 only
        final List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        final Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

        RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
        rebalanceClientConfig.setMaxParallelDonors(2);
        rebalanceClientConfig.setMaxParallelRebalancing(2);

        final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                            0),
                                                                            rebalanceClientConfig);

        // populate data now.
        populateData(updatedCluster, Arrays.asList(0, 1), rebalanceClient.getAdminClient());

        final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCluster,
                                                                                                                                  0))
                                                                                                .setEnableLazy(false)
                                                                                                .setSocketTimeout(120,
                                                                                                                  TimeUnit.SECONDS));

        final StoreClient<String, String> storeClient = new DefaultStoreClient<String, String>(testStoreNameRW,
                                                                                               null,
                                                                                               factory,
                                                                                               3);
        final Boolean[] masterNodeResponded = { false, false, false, false };

        // start get operation.
        executors.execute(new Runnable() {

            public void run() {
                try {
                    List<String> keys = new ArrayList<String>(testEntries.keySet());

                    int nRequests = 0;
                    while(!rebalancingToken.get()) {
                        // should always able to get values.
                        int index = (int) (Math.random() * keys.size());

                        // should get a valid value
                        try {
                            nRequests++;
                            Versioned<String> value = storeClient.get(keys.get(index));
                            assertNotSame("StoreClient get() should not return null.", null, value);
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         value);
                            int masterNode = storeClient.getResponsibleNodes(keys.get(index))
                                                        .get(0)
                                                        .getId();
                            masterNodeResponded[masterNode] = true;

                        } catch(Exception e) {
                            System.out.println(e);
                            e.printStackTrace();
                            exceptions.add(e);
                        }
                    }

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    factory.close();
                }
            }

        });

        executors.execute(new Runnable() {

            public void run() {
                try {

                    Thread.sleep(500);

                    rebalanceAndCheck(updatedCluster,
                                      updateCluster(targetCluster),
                                      rebalanceClient,
                                      Arrays.asList(2, 3));

                    Thread.sleep(500);

                    rebalancingToken.set(true);

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers
                    try {
                        stopServer(serverList);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        executors.shutdown();
        executors.awaitTermination(300, TimeUnit.SECONDS);

        assertEquals("Client should see values returned master at both (0,1,2,3):("
                             + Joiner.on(",").join(masterNodeResponded) + ")",
                     true,
                     masterNodeResponded[0] && masterNodeResponded[1] && masterNodeResponded[2]
                             && masterNodeResponded[3]);

        // check No Exception
        if(exceptions.size() > 0) {
            for(Exception e: exceptions) {
                e.printStackTrace();
            }
            fail("Should not see any exceptions.");
        }
    }

    @Test
    public void testServerSideRouting() throws Exception {
        Cluster localCluster = ServerTestUtils.getLocalCluster(2,
                                                               new int[][] { { 0, 1, 2, 3 }, {} });

        Cluster localTargetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { {},
                { 0, 1, 2, 3 } });

        // start servers 0 , 1 only
        final List<Integer> serverList = Arrays.asList(0, 1);
        final Cluster updatedCluster = startServers(localCluster, storeDefFile, serverList, null);
        final Cluster targetCluster = updateCluster(localTargetCluster);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

        // populate data now.
        RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
        rebalanceClientConfig.setMaxParallelDonors(2);
        rebalanceClientConfig.setMaxParallelRebalancing(2);

        final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                            0),
                                                                            rebalanceClientConfig);

        populateData(updatedCluster, Arrays.asList(0), rebalanceClient.getAdminClient());

        Node node = updatedCluster.getNodeById(0);
        final Store<ByteArray, byte[], byte[]> serverSideRoutingStore = getSocketStore(testStoreNameRW,
                                                                                       node.getHost(),
                                                                                       node.getSocketPort(),
                                                                                       true);

        final CountDownLatch latch = new CountDownLatch(1);

        // start get operation.
        executors.execute(new Runnable() {

            public void run() {
                try {
                    List<String> keys = new ArrayList<String>(testEntries.keySet());

                    int nRequests = 0;
                    while(!rebalancingToken.get()) {
                        // should always able to get values.
                        int index = (int) (Math.random() * keys.size());

                        // should get a valid value
                        try {
                            nRequests++;
                            List<Versioned<byte[]>> values = serverSideRoutingStore.get(new ByteArray(ByteUtils.getBytes(keys.get(index),
                                                                                                                         "UTF-8")),
                                                                                        null);

                            assertEquals("serverSideRoutingStore should return value.",
                                         1,
                                         values.size());
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         new Versioned<String>(ByteUtils.getString(values.get(0)
                                                                                         .getValue(),
                                                                                   "UTF-8"),
                                                               values.get(0).getVersion()));
                        } catch(UnreachableStoreException e) {
                            // ignore
                        } catch(Exception e) {
                            exceptions.add(e);
                        }
                    }

                    latch.countDown();
                } catch(Exception e) {
                    exceptions.add(e);
                }
            }

        });

        executors.execute(new Runnable() {

            public void run() {
                try {

                    Thread.sleep(500);

                    rebalanceAndCheck(updatedCluster,
                                      targetCluster,
                                      rebalanceClient,
                                      Arrays.asList(1));

                    Thread.sleep(500);

                    rebalancingToken.set(true);

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers as soon as the client thread has exited its
                    // loop.
                    try {
                        latch.await(300, TimeUnit.SECONDS);
                        stopServer(serverList);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        executors.shutdown();
        executors.awaitTermination(300, TimeUnit.SECONDS);

        // check No Exception
        if(exceptions.size() > 0) {
            for(Exception e: exceptions) {
                e.printStackTrace();
            }
            fail("Should not see any exceptions !!");
        }
    }

    protected void populateData(Cluster cluster, List<Integer> nodeList, AdminClient adminClient)
            throws Exception {

        // Populate Read write stores

        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        for(int nodeId: nodeList) {
            Node node = cluster.getNodeById(nodeId);
            storeMap.put(nodeId, getSocketStore(testStoreNameRW,
                                                node.getHost(),
                                                node.getSocketPort()));

        }

        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);
        for(Entry<String, String> entry: testEntries.entrySet()) {
            int masterNode = routing.routeRequest(ByteUtils.getBytes(entry.getKey(), "UTF-8"))
                                    .get(0)
                                    .getId();
            if(nodeList.contains(masterNode)) {
                try {
                    ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
                    storeMap.get(masterNode)
                            .put(keyBytes,
                                 new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(), "UTF-8")),
                                 null);
                } catch(ObsoleteVersionException e) {
                    System.out.println("Why are we seeing this at all here ?? ");
                    e.printStackTrace();
                }
            }
        }

        // close all socket stores
        for(Store<ByteArray, byte[], byte[]> store: storeMap.values()) {
            store.close();
        }

        // Populate Read only stores

        File baseDir = TestUtils.createTempDir();
        JsonReader reader = ReadOnlyStorageEngineTestInstance.makeTestDataReader(testEntries,
                                                                                 baseDir);

        StoreDefinition def = null;
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().compareTo(testStoreNameRO) == 0) {
                def = storeDef;
                break;
            }
        }

        Utils.notNull(def);
        RoutingStrategy router = new RoutingStrategyFactory().updateRoutingStrategy(def, cluster);

        File outputDir = TestUtils.createTempDir(baseDir);
        JsonStoreBuilder storeBuilder = new JsonStoreBuilder(reader,
                                                             cluster,
                                                             def,
                                                             router,
                                                             outputDir,
                                                             null,
                                                             testEntries.size() / 5,
                                                             1,
                                                             2,
                                                             10000,
                                                             false);
        storeBuilder.build(ReadOnlyStorageFormat.READONLY_V1);

        AdminStoreSwapper swapper = new AdminStoreSwapper(cluster,
                                                          Executors.newFixedThreadPool(nodeList.size()),
                                                          adminClient,
                                                          100000);
        swapper.swapStoreData(testStoreNameRO, outputDir.getAbsolutePath(), 1L);

    }

    protected String getBootstrapUrl(Cluster cluster, int nodeId) {
        Node node = cluster.getNodeById(nodeId);
        return "tcp://" + node.getHost() + ":" + node.getSocketPort();
    }

    protected List<Integer> getUnavailablePartitions(Cluster targetCluster,
                                                     List<Integer> availablePartitions) {
        List<Integer> unavailablePartitions = new ArrayList<Integer>();

        for(Node node: targetCluster.getNodes()) {
            unavailablePartitions.addAll(node.getPartitionIds());
        }

        unavailablePartitions.removeAll(availablePartitions);
        return unavailablePartitions;
    }

    private void rebalanceAndCheck(Cluster currentCluster,
                                   Cluster targetCluster,
                                   RebalanceController rebalanceClient,
                                   List<Integer> nodeCheckList) {
        rebalanceClient.rebalance(targetCluster);

        for(int nodeId: nodeCheckList) {
            List<Integer> availablePartitions = targetCluster.getNodeById(nodeId).getPartitionIds();
            List<Integer> unavailablePartitions = getUnavailablePartitions(targetCluster,
                                                                           availablePartitions);

            checkGetEntries(currentCluster.getNodeById(nodeId),
                            targetCluster,
                            unavailablePartitions,
                            availablePartitions,
                            false);
        }

    }

    protected void checkGetEntries(Node node,
                                   Cluster cluster,
                                   List<Integer> unavailablePartitions,
                                   List<Integer> availablePartitions,
                                   boolean onlyReadWrite) {
        int matchedEntries = 0;
        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);

        Store<ByteArray, byte[], byte[]> storeRW = getSocketStore(testStoreNameRW,
                                                                  node.getHost(),
                                                                  node.getSocketPort());
        Store<ByteArray, byte[], byte[]> storeRO = getSocketStore(testStoreNameRO,
                                                                  node.getHost(),
                                                                  node.getSocketPort());

        for(Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));

            List<Integer> partitions = routing.getPartitionList(keyBytes.get());

            if(null != unavailablePartitions && unavailablePartitions.containsAll(partitions)) {
                try {
                    List<Versioned<byte[]>> value = storeRW.get(keyBytes, null);
                    assertEquals("unavailable partitons should return zero size list.",
                                 0,
                                 value.size());

                } catch(InvalidMetadataException e) {
                    // ignore.
                }
                if(!onlyReadWrite) {
                    try {
                        List<Versioned<byte[]>> value = storeRO.get(keyBytes, null);
                        assertEquals("unavailable partitons should return zero size list.",
                                     0,
                                     value.size());
                    } catch(InvalidMetadataException e) {
                        // ignore.
                    }
                }

            } else if(null != availablePartitions && availablePartitions.containsAll(partitions)) {
                List<Versioned<byte[]>> values = storeRW.get(keyBytes, null);

                // expecting exactly one version
                assertEquals("Expecting exactly one version", 1, values.size());
                Versioned<byte[]> value = values.get(0);
                // check version matches (expecting base version for all)
                assertEquals("Value version should match", new VectorClock(), value.getVersion());
                // check value matches.
                assertEquals("Value bytes should match",
                             entry.getValue(),
                             ByteUtils.getString(value.getValue(), "UTF-8"));

                if(!onlyReadWrite) {
                    values = storeRO.get(keyBytes, null);

                    // expecting exactly one version
                    assertEquals("Expecting exactly one version", 1, values.size());
                    value = values.get(0);
                    // check version matches (expecting base version for all)
                    assertEquals("Value version should match",
                                 new VectorClock(),
                                 value.getVersion());
                    // check value matches.
                    assertEquals("Value bytes should match",
                                 entry.getValue(),
                                 ByteUtils.getString(value.getValue(), "UTF-8"));
                }
                matchedEntries++;
            } else {
                // dont care about these
            }
        }

        if(null != availablePartitions && availablePartitions.size() > 0)
            assertNotSame("CheckGetEntries should match some entries.", 0, matchedEntries);
    }
}