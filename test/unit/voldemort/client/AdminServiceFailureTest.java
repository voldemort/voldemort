package voldemort.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
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

    protected Store<ByteArray, byte[]> getStore(String storeName) {
        return server.getStoreRepository().getStorageEngine(storeName);
    }

    public void testWithStartFailure() {
        // put some entries in store
        for(Entry<ByteArray, byte[]> entry: ServerTestUtils.createRandomKeyValuePairs(TEST_KEYS)
                                                           .entrySet()) {
            getStore(testStoreName).put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
        }

        server.stop();
        try {
            // make fetch stream call.
            Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = getAdminClient().fetchPartitionEntries(0,
                                                                                                                testStoreName,
                                                                                                                Arrays.asList(new Integer[] { 0 }),
                                                                                                                null);
            fail();
        } catch(UnreachableStoreException e) {
            // ignore
        }
    }
}
