package voldemort.versioning;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;

/*
 * Test to ensure that the TimeBasedInconsistencyResolver returns a max Vector
 * clock with the max timestamp.
 */
public class ChainedInconsistencyResolverTest {

    private static final String KEY = "XYZ";
    private Versioned<String> v1, v2;
    private Versioned<String> conflict1, conflict2, conflict3, conflict4, conflict5, conflict6;

    private StoreClient<String, String> defaultStoreClient;
    private Store<ByteArray, byte[], byte[]> socketStore;
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private static final String STORES_XML = "test/common/voldemort/config/single-store.xml";

    @Before
    public void setUp() throws IOException {
        boolean useNio = false;
        int numServers = 2;
        VoldemortServer[] servers = new VoldemortServer[numServers];
        Cluster cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                                servers,
                                                                null,
                                                                socketStoreFactory,
                                                                useNio,
                                                                null,
                                                                STORES_XML,
                                                                new Properties());

        // Initialize versioned puts for basic test
        v1 = getVersioned(0, 1, 1, 1, 1, 1);
        v2 = getVersioned(0, 0, 1, 1, 1, 1);

        // Initialize versioned puts for > 1 conflicts
        conflict1 = getVersioned(0, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        conflict2 = getVersioned(0, 0, 1, 1, 1, 1, 1, 1, 1, 1);
        conflict3 = getVersioned(0, 0, 0, 1, 1, 1, 1, 1, 1, 1);
        conflict4 = getVersioned(0, 0, 0, 0, 1, 1, 1, 1, 1, 1);
        conflict5 = getVersioned(0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
        conflict6 = getVersioned(0, 0, 0, 0, 0, 0, 1, 1, 1, 1);

        Node node = cluster.getNodes().iterator().next();
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        defaultStoreClient = storeClientFactory.getStoreClient("test");
        socketStore = ServerTestUtils.getSocketStore(socketStoreFactory,
                                                     "test",
                                                     node.getSocketPort(),
                                                     RequestFormatType.VOLDEMORT_V1);
    }

    @After
    public void tearDown() throws Exception {
        socketStore.close();
        socketStoreFactory.close();
    }

    private Versioned<String> getVersioned(int... nodes) {
        return new Versioned<String>("my-value", TestUtils.getClock(nodes));
    }

    @Test
    public void testVersionedPut() {
        defaultStoreClient.put(KEY, v1);
        defaultStoreClient.put(KEY, v2);
        Versioned<String> res = defaultStoreClient.get(KEY);
        defaultStoreClient.put(KEY, res);
        List<Versioned<byte[]>> resList = socketStore.get(new ByteArray(KEY.getBytes()), null);
        assertEquals(1, resList.size());
    }

    @Test
    public void testNormalPut() {
        defaultStoreClient.put(KEY, v1);
        defaultStoreClient.put(KEY, v2);
        defaultStoreClient.put(KEY, "my-value2");
        List<Versioned<byte[]>> resList = socketStore.get(new ByteArray(KEY.getBytes()), null);
        assertEquals(1, resList.size());
    }

    @Test
    public void testMoreConflicts() {
        defaultStoreClient.put(KEY, conflict1);
        defaultStoreClient.put(KEY, conflict2);
        defaultStoreClient.put(KEY, conflict3);
        defaultStoreClient.put(KEY, conflict4);
        defaultStoreClient.put(KEY, conflict5);
        defaultStoreClient.put(KEY, conflict6);
        List<Versioned<byte[]>> resList = socketStore.get(new ByteArray(KEY.getBytes()), null);
        assertEquals(6, resList.size());
        Versioned<String> res = defaultStoreClient.get(KEY);
        defaultStoreClient.put(KEY, res);
        resList = socketStore.get(new ByteArray(KEY.getBytes()), null);
        assertEquals(1, resList.size());
    }
}
