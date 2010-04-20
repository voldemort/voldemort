package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class AbstractRebalanceTest {

    protected static int NUM_KEYS = 100;
    protected static String testStoreName = "test";
    protected static String storeDefFile = "test/common/voldemort/config/single-store.xml";

    HashMap<String, String> testEntries;

    @Before
    public void setUp() {
        testEntries = ServerTestUtils.createRandomKeyValueString(NUM_KEYS);
    }

    @After
    public void tearDown() {
        testEntries.clear();
    }

    protected abstract Cluster startServers(Cluster cluster,
                                            String StoreDefXmlFile,
                                            List<Integer> nodeToStart,
                                            Map<String, String> configProps) throws Exception;

    protected abstract void stopServer(List<Integer> nodesToStop) throws Exception;

    protected Cluster updateCluster(Cluster template) {
        return template;
    }

    protected SocketStore getSocketStore(String storeName, String host, int port) {
        return getSocketStore(storeName, host, port, false);
    }

    protected SocketStore getSocketStore(String storeName, String host, int port, boolean isRouted) {
        return ServerTestUtils.getSocketStore(storeName,
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
            populateData(updatedCluster, Arrays.asList(0));
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
            populateData(updatedCluster, Arrays.asList(0));
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
                            null);

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
            populateData(updatedCluster, Arrays.asList(0));
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
                            null,
                            Arrays.asList(2, 3));

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
            populateData(updatedCluster, Arrays.asList(0));
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
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(updatedCluster, Arrays.asList(0));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1, 2));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test
    public void testMultipleDonors() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                       { 0, 2 }, { 1, 3, 5 }, { 4, 6 }, {} });
        Cluster targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                       { 0  }, { 1, 3 }, { 4, 6 }, { 2, 5 } });

        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(2);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(updatedCluster, Arrays.asList(0,1,2));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(3));
        } finally {
            // stop servers
            stopServer(serverList);
        }

    }

    @Test
    public void testMultipleDonorsMultipleStealers() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                       { 0, 2, 4, 6 }, { 1, 3, 5 }, {}, {} });
        Cluster targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                       { 0, 4  }, { 1 }, { 6, 3 }, { 2, 5 } });

        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(2);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(updatedCluster, Arrays.asList(0,1));
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

        // populate data now.
        populateData(updatedCluster, Arrays.asList(0));

        final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCluster,
                                                                                                                                  0))
                                                                                                .setSocketTimeout(120,
                                                                                                                  TimeUnit.SECONDS));

        final StoreClient<String, String> storeClient = new DefaultStoreClient<String, String>(testStoreName,
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

                    RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                                  0),
                                                                                  new RebalanceClientConfig());
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
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                       { 0, 2, 4, 6 }, { 1, 3, 5 }, {}, {} });
        final Cluster targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                       { 0, 4  }, { 1 }, { 6, 3 }, { 2, 5 } });


        // start servers 0 , 1 only
        final List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        final Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

        // populate data now.
        populateData(updatedCluster, Arrays.asList(0, 1));

        final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCluster,
                                                                                                                                  0))
                                                                                                .setSocketTimeout(120,
                                                                                                                  TimeUnit.SECONDS));

        final StoreClient<String, String> storeClient = new DefaultStoreClient<String, String>(testStoreName,
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

                    RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                                  0),
                                                                                  new RebalanceClientConfig());
                    rebalanceAndCheck(updatedCluster,
                                      updateCluster(targetCluster),
                                      rebalanceClient,
                                      Arrays.asList(2,3));

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

        assertEquals("Client should see values returned master at both (0,1,2,3):(" +
                             Joiner.on(",").join(masterNodeResponded) + ")",
                     true,
                     masterNodeResponded[0] && masterNodeResponded[1] && masterNodeResponded[2] && masterNodeResponded[3] );

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
        populateData(updatedCluster, Arrays.asList(0));

        Node node = updatedCluster.getNodeById(0);
        final Store<ByteArray, byte[]> serverSideRoutingStore = getSocketStore(testStoreName,
                                                                               node.getHost(),
                                                                               node.getSocketPort(),
                                                                               true);

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
                                                                                                                         "UTF-8")));

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

                } catch(Exception e) {
                    exceptions.add(e);
                }
            }

        });

        executors.execute(new Runnable() {

            public void run() {
                try {

                    Thread.sleep(500);

                    RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCluster,
                                                                                                  0),
                                                                                  new RebalanceClientConfig());
                    rebalanceAndCheck(updatedCluster,
                                      targetCluster,
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

        // check No Exception
        if(exceptions.size() > 0) {
            for(Exception e: exceptions) {
                e.printStackTrace();
            }
            fail("Should not see any exceptions !!");
        }
    }

    protected void populateData(Cluster cluster, List<Integer> nodeList) {

        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[]>>();
        for(int nodeId: nodeList) {
            Node node = cluster.getNodeById(nodeId);
            storeMap.put(nodeId,
                         getSocketStore(testStoreName, node.getHost(), node.getSocketPort()));

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
                                 new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(), "UTF-8")));
                } catch(ObsoleteVersionException e) {
                    System.out.println("Why are we seeing this at all here ?? ");
                    e.printStackTrace();
                }
            }
        }

        // close all socket stores
        for(Store<ByteArray, byte[]> store: storeMap.values()) {
            store.close();
        }
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
                            availablePartitions);
        }

    }

    protected void checkGetEntries(Node node,
                                   Cluster cluster,
                                   List<Integer> unavailablePartitions,
                                   List<Integer> availablePartitions) {
        int matchedEntries = 0;
        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);

        SocketStore store = getSocketStore(testStoreName, node.getHost(), node.getSocketPort());

        for(Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));

            List<Integer> partitions = routing.getPartitionList(keyBytes.get());

            if(null != unavailablePartitions && unavailablePartitions.containsAll(partitions)) {
                try {
                    List<Versioned<byte[]>> value = store.get(keyBytes);
                    assertEquals("unavailable partitons should return zero size list.",
                                 0,
                                 value.size());
                } catch(InvalidMetadataException e) {
                    // ignore.
                }
            } else if(null != availablePartitions && availablePartitions.containsAll(partitions)) {
                List<Versioned<byte[]>> values = store.get(keyBytes);

                // expecting exactly one version
                assertEquals("Expecting exactly one version", 1, values.size());
                Versioned<byte[]> value = values.get(0);
                // check version matches (expecting base version for all)
                assertEquals("Value version should match", new VectorClock(), value.getVersion());
                // check value matches.
                assertEquals("Value bytes should match",
                             entry.getValue(),
                             ByteUtils.getString(value.getValue(), "UTF-8"));
                matchedEntries++;
            } else {
                // dont care about these
            }
        }

        if(null != availablePartitions && availablePartitions.size() > 0)
            assertNotSame("CheckGetEntries should match some entries.", 0, matchedEntries);
    }
}