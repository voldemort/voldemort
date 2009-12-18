package voldemort.client.rebalance;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class AbstractRebalanceTest extends TestCase {

    private static final int NUM_KEYS = 10000;
    private static String testStoreName = "test-replication-memory";
    private static String storeDefFile = "test/common/voldemort/config/stores.xml";

    HashMap<ByteArray, byte[]> testEntries;

    @Override
    public void setUp() {
        testEntries = ServerTestUtils.createRandomKeyValuePairs(NUM_KEYS);
    }

    @Override
    public void tearDown() {
        testEntries.clear();
    }

    protected abstract Cluster startServers(Cluster cluster,
                                            String StoreDefXmlFile,
                                            List<Integer> nodeToStart) throws IOException;

    protected abstract void stopServer(List<Integer> nodesToStop) throws IOException;

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
        for(Entry<ByteArray, byte[]> entry: testEntries.entrySet()) {
            int masterNode = routing.routeRequest(entry.getKey().get()).get(0).getId();
            if(nodeList.contains(masterNode)) {
                storeMap.get(masterNode).put(entry.getKey(), new Versioned(entry.getValue()));
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

    // test a single rebalancing.
    public void testSingleRebalance() throws IOException {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList);

        try {
            // generate data at node 0 only.
            populateData(updatedCluster, Arrays.asList(0));

            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                                  new RebalanceClientConfig());

            rebalanceClient.rebalance(targetCluster, Arrays.asList(testStoreName));

            checkGetEntries(updatedCluster.getNodeById(1),
                            targetCluster,
                            Arrays.asList(0, 1, 4, 5, 6, 7, 8),
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
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList);

        try {
            // generate data at node 0 only.
            populateData(updatedCluster, Arrays.asList(0));

            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                                  new RebalanceClientConfig());

            rebalanceClient.rebalance(targetCluster, Arrays.asList(testStoreName));

            // check Node 1
            checkGetEntries(updatedCluster.getNodeById(1),
                            targetCluster,
                            Arrays.asList(0, 1, 4, 5, 6, 7, 8),
                            Arrays.asList(2, 3));

            // check Node 2
            checkGetEntries(updatedCluster.getNodeById(2),
                            targetCluster,
                            Arrays.asList(0, 1, 2, 3, 4, 5, 6),
                            Arrays.asList(7, 8));

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
        Cluster updatedCluster = startServers(currentCluster, storeDefFile, serverList);

        try {
            // generate data at node 0 only.
            populateData(updatedCluster, Arrays.asList(0));

            // set maxParallelRebalancing to 2
            RebalanceClientConfig config = new RebalanceClientConfig();
            config.setMaxParallelRebalancing(2);
            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                                  config);

            rebalanceClient.rebalance(targetCluster, Arrays.asList(testStoreName));

            // check Node 1
            checkGetEntries(updatedCluster.getNodeById(1),
                            targetCluster,
                            Arrays.asList(0, 1, 4, 5, 6, 7, 8),
                            Arrays.asList(2, 3));

            // check Node 2
            checkGetEntries(updatedCluster.getNodeById(2),
                            targetCluster,
                            Arrays.asList(0, 1, 2, 3, 4, 5, 6),
                            Arrays.asList(7, 8));

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    // test a single rebalancing.
    public void testProxyGetDuringRebalancing() throws IOException {
        ExecutorService executors = Executors.newFixedThreadPool(2);
    }

    private void checkGetEntries(Node node,
                                 Cluster cluster,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);

        SocketStore store = ServerTestUtils.getSocketStore(testStoreName,
                                                           node.getHost(),
                                                           node.getSocketPort(),
                                                           RequestFormatType.PROTOCOL_BUFFERS);

        for(Entry<ByteArray, byte[]> entry: testEntries.entrySet()) {
            List<Integer> partitions = routing.getPartitionList(entry.getKey().get());

            if(unavailablePartitions.containsAll(partitions)) {
                try {
                    List<Versioned<byte[]>> value = store.get(entry.getKey());
                    fail();
                } catch(InvalidMetadataException e) {
                    // ignore.
                }
            } else if(availablePartitions.containsAll(partitions)) {
                List<Versioned<byte[]>> values = store.get(entry.getKey());

                // expecting exactly one version
                assertEquals("Expecting exactly one version", 1, values.size());
                Versioned<byte[]> value = values.get(0);
                // check version matches (expecting base version for all)
                assertEquals("Value version should match", new VectorClock(), value.getVersion());
                // check value matches.
                assertEquals("Value bytes should match", 0, ByteUtils.compare(entry.getValue(),
                                                                              value.getValue()));
            }
        }
    }
}