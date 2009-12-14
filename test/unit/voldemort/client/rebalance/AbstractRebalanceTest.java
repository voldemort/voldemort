package voldemort.client.rebalance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class AbstractRebalanceTest extends TestCase {

    private static String testStoreName = "test-replication-memory";
    private Cluster currentCluster;
    private Cluster targetCluster;

    @Override
    public void setUp() {
        currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {}, {} });

        targetCluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 1 }, { 2, 3 },
                { 4, 5 }, { 6, 7 } });
    }

    protected abstract AdminClient getAdminClient();

    protected abstract Map<Integer, Node> startServers(Cluster cluster, List<Integer> nodeToStart);

    protected abstract void stopServer(List<Integer> nodesToStop);

    public void testSingleRebalance() {}

    private void checkGetEntries(HashMap<ByteArray, byte[]> entryMap,
                                 int nodeId,
                                 String storeName,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = new ConsistentRoutingStrategy(targetCluster.getNodes(), 1);
        Store<ByteArray, byte[]> store = null;

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routing.getPartitionList(entry.getKey().get());
            List<Versioned<byte[]>> values = store.get(entry.getKey());

            if(unavailablePartitions.containsAll(partitions)) {
                assertEquals("Store.get() for non-available partitions:" + partitions
                             + " should return empty list.", 0, values.size());
            } else if(availablePartitions.containsAll(partitions)) {
                // expecting exactly one version
                assertEquals("Store.get() for non-available partitions:" + partitions
                             + " should return non-empty list.", 1, values.size());
                // check version matches (expecting base version for all)
                assertEquals("Value version should match", new VectorClock(), values.get(0)
                                                                                    .getVersion());
                // check value matches.
                assertEquals("Value bytes should match", 0, ByteUtils.compare(entry.getValue(),
                                                                              values.get(0)
                                                                                    .getValue()));
            }
        }
    }
}