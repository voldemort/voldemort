package voldemort.client.rebalance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public abstract class AbstractRebalanceTest extends TestCase {

    private static final int NUM_KEYS = 100000;
    private static String testStoreName = "test-replication-memory";
    private static String storeDefFile = "test/common/voldemort/config/stores.xml";

    private Cluster currentCluster;
    private Cluster targetCluster;
    List<StoreDefinition> storeDefList;
    HashMap<ByteArray, byte[]> testEntries;

    @Override
    public void setUp() {
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });

        testEntries = ServerTestUtils.createRandomKeyValuePairs(NUM_KEYS);

        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
        } catch(FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    @Override
    public void tearDown() {

    }

    protected abstract Cluster startServers(Cluster cluster,
                                            List<StoreDefinition> storeDefs,
                                            List<Integer> nodeToStart) throws IOException;

    protected abstract void stopServer(List<Integer> nodesToStop) throws IOException;

    protected String getStoresXmlFile() {
        return storeDefFile;
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
        // start servers 0 , 1 only
        Cluster updatedCluster = startServers(currentCluster, storeDefList, Arrays.asList(0, 1));

        try {
            // generate data at node 0 only.
            populateData(updatedCluster, Arrays.asList(0));

            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(currentCluster, 0),
                                                                  1,
                                                                  new AdminClientConfig());

            rebalanceClient.rebalance(targetCluster, Arrays.asList(testStoreName));

            checkGetEntries(updatedCluster.getNodeById(1),
                            Arrays.asList(0, 1, 4, 5, 6, 7, 8),
                            Arrays.asList(2, 3));
        } finally {
            // stop servers
            stopServer(Arrays.asList(0, 1));
        }
    }

    private void checkGetEntries(Node node,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = new ConsistentRoutingStrategy(targetCluster.getNodes(), 1);

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