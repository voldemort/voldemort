package voldemort.client.rebalance;

import java.io.IOException;
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

import junit.framework.TestCase;
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

import com.google.common.collect.ImmutableMap;

public abstract class AbstractRebalanceTest extends TestCase {

    private static final int NUM_KEYS = 10000;
    private static String testStoreName = "test";
    private static String storeDefFile = "test/common/voldemort/config/single-store.xml";

    HashMap<String, String> testEntries;

    @Override
    public void setUp() {
        testEntries = ServerTestUtils.createRandomKeyValueString(NUM_KEYS);
    }

    @Override
    public void tearDown() {
        testEntries.clear();
    }

    protected abstract Cluster startServers(Cluster cluster,
                                            String StoreDefXmlFile,
                                            List<Integer> nodeToStart,
                                            Map<String, String> configProps) throws IOException;

    protected abstract void stopServer(List<Integer> nodesToStop) throws IOException;

    // test a single rebalancing.
    public void testSingleRebalance() throws IOException {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                              new RebalanceClientConfig());
        try {
            populateData(currentCluster, Arrays.asList(0));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    public void testDeleteAfterRebalancing() throws IOException {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster,
                                              storeDefFile,
                                              serverList,
                                              ImmutableMap.of("enable.rebalancing.delete", "true"));

        RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                              new RebalanceClientConfig());
        try {
            populateData(currentCluster, Arrays.asList(0));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1));

            // check that all keys are partitions 2,3 are Indeeed deleted.
            // assign all partitions to node 0 by force ..
            rebalanceClient.getAdminClient()
                           .updateRemoteCluster(0,
                                                currentCluster,
                                                ((VectorClock) RebalanceUtils.getLatestCluster(null,
                                                                                               rebalanceClient.getAdminClient())
                                                                             .getVersion()).incremented(0,
                                                                                                        System.currentTimeMillis()));
            checkGetEntries(currentCluster.getNodeById(0),
                            currentCluster,
                            Arrays.asList(2, 3),
                            null);

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    public void testDeleteAfterRebalancingDisabled() throws IOException {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                              new RebalanceClientConfig());
        try {
            populateData(currentCluster, Arrays.asList(0));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1));

            // check that all keys are partitions 2,3 are Indeeed deleted.
            // assign all partitions to node 0 by force ..
            rebalanceClient.getAdminClient()
                           .updateRemoteCluster(0,
                                                currentCluster,
                                                ((VectorClock) RebalanceUtils.getLatestCluster(null,
                                                                                               rebalanceClient.getAdminClient())
                                                                             .getVersion()).incremented(0,
                                                                                                        System.currentTimeMillis()));
            checkGetEntries(currentCluster.getNodeById(0),
                            currentCluster,
                            null,
                            Arrays.asList(2, 3));

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    public void testMultipleRebalance() throws IOException {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 4, 5, 6 },
                { 2, 3 }, { 7, 8 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1, 2);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                              new RebalanceClientConfig());
        try {
            populateData(currentCluster, Arrays.asList(0));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1, 2));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    public void testMultipleParallelRebalance() throws IOException {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 4, 5, 6 },
                { 2, 3 }, { 7, 8 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1, 2);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList, null);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setMaxParallelRebalancing(2);
        RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                              config);
        try {
            populateData(currentCluster, Arrays.asList(0));
            rebalanceAndCheck(updatedCluster, targetCluster, rebalanceClient, Arrays.asList(1, 2));
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    // test a single rebalancing.
    public void testProxyGetDuringRebalancing() throws IOException, InterruptedException {
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
        populateData(currentCluster, Arrays.asList(0));

        final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(currentCluster,
                                                                                                                                  0)));

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

                        } catch(UnreachableStoreException e) {
                            // ignore
                        } catch(Exception e) {
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

                    Thread.sleep(100);

                    RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster,
                                                                                          0),
                                                                          new RebalanceClientConfig());
                    rebalanceAndCheck(updatedCluster,
                                      targetCluster,
                                      rebalanceClient,
                                      Arrays.asList(1));

                    // sleep for 1 mins before stopping servers
                    Thread.sleep(60 * 1000);

                    rebalancingToken.set(true);

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers
                    try {
                        stopServer(serverList);
                    } catch(IOException e) {
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
            fail("Should not see any exceptions !!");
        }
    }

    // test a single rebalancing.
    public void testServerSideRouting() throws IOException, InterruptedException {
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
        populateData(currentCluster, Arrays.asList(0));

        Node node = currentCluster.getNodeById(0);
        final Store<ByteArray, byte[]> serverSideRoutingStore = ServerTestUtils.getSocketStore(testStoreName,
                                                                                               node.getHost(),
                                                                                               node.getSocketPort(),
                                                                                               RequestFormatType.PROTOCOL_BUFFERS,
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

                    Thread.sleep(100);

                    RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster,
                                                                                          0),
                                                                          new RebalanceClientConfig());
                    rebalanceAndCheck(updatedCluster,
                                      targetCluster,
                                      rebalanceClient,
                                      Arrays.asList(1));

                    // sleep for 1 mins before stopping servers
                    Thread.sleep(60 * 1000);

                    rebalancingToken.set(true);

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers
                    try {
                        stopServer(serverList);
                    } catch(IOException e) {
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

    private void populateData(Cluster cluster, List<Integer> nodeList) {

        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[]>>();
        for(int nodeId: nodeList) {
            Node node = cluster.getNodeById(nodeId);
            storeMap.put(nodeId, ServerTestUtils.getSocketStore(testStoreName,
                                                                node.getHost(),
                                                                node.getSocketPort(),
                                                                RequestFormatType.PROTOCOL_BUFFERS));
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
        for(Store store: storeMap.values()) {
            store.close();
        }
    }

    private String getBootstrapUrl(Cluster cluster, int nodeId) {
        Node node = cluster.getNodeById(nodeId);
        return "tcp://" + node.getHost() + ":" + node.getSocketPort();
    }

    private List<Integer> getUnavailablePartitions(Cluster targetCluster,
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
                                   RebalanceClient rebalanceClient,
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

    private void checkGetEntries(Node node,
                                 Cluster cluster,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        int matchedEntries = 0;
        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);

        SocketStore store = ServerTestUtils.getSocketStore(testStoreName,
                                                           node.getHost(),
                                                           node.getSocketPort(),
                                                           RequestFormatType.PROTOCOL_BUFFERS);

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