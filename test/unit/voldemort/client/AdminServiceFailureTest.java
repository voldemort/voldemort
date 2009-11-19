package voldemort.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * Test to check streaming behavior under failure.
 * 
 * @author bbansal
 * 
 */
public class AdminServiceFailureTest extends TestCase {

    private static String testStoreName = "test-replication-memory";
    private static int TEST_KEYS = 10000;
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private AdminClient adminClient;
    private VoldemortServer server;
    private Cluster cluster;

    private enum Operations {
        FETCH_ENTRIES,
        FETCH_KEYS,
        DELETE_PARTITIONS,
        UPDATE_ENTRIES
    };

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2);
        VoldemortConfig config = ServerTestUtils.createServerConfig(0,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile);
        server = new VoldemortServer(config, cluster);
        server.start();

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Override
    public void tearDown() throws IOException {
        try {
            adminClient.stop();
            ServerTestUtils.stopVoldemortServer(server);
        } catch(Exception e) {
            // ignore
        }
    }

    protected AdminClient getAdminClient() {
        return adminClient;
    }

    protected Store<ByteArray, byte[]> getStore(int nodeId, String storeName) {
        if(nodeId != 0)
            throw new RuntimeException("can only get store for nodeId 0 for now.");
        return server.getStoreRepository().getStorageEngine(storeName);
    }

    public void testWithStartFailure() {
        // put some entries in store
        server.stop();
        try {
            fail();
        } catch(UnreachableStoreException e) {
            // ignore
        }
    }

    private void doOperation(Operations e,
                             int nodeId,
                             String storeName,
                             List<Integer> partitionList,
                             Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator) {
        switch(e) {
            case DELETE_PARTITIONS:
                putAlltoStore(nodeId, storeName);
                getAdminClient().deletePartitions(nodeId, storeName, partitionList, null);
                break;
            case FETCH_ENTRIES:
                putAlltoStore(nodeId, storeName);
                getAdminClient().fetchPartitionEntries(nodeId, storeName, partitionList, null);
            case FETCH_KEYS:
                putAlltoStore(nodeId, storeName);
                getAdminClient().fetchPartitionKeys(nodeId, storeName, partitionList, null);
            case UPDATE_ENTRIES:
                getAdminClient().updateEntries(nodeId, storeName, entryIterator, null);
                break;
            default:
                throw new RuntimeException("Unknown operation");
        }
    }

    private void putAlltoStore(int nodeId, String storeName) {
        for(Entry<ByteArray, byte[]> entry: ServerTestUtils.createRandomKeyValuePairs(TEST_KEYS)
                                                           .entrySet()) {
            getStore(nodeId, storeName).put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
        }
    }
}
