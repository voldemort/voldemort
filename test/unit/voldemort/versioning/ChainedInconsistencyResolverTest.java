package voldemort.versioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
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
public class ChainedInconsistencyResolverTest extends TestCase {

    private static final String KEY = "XYZ";
    private Versioned<String> v1, v2;

    private Node node;
    private Cluster cluster;
    private StoreClient<String, String> defaultStoreClient;
    private Store<ByteArray, byte[], byte[]> socketStore;
    private List<VoldemortServer> servers;
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private final boolean useNio = false;
    private static final String STORES_XML = "test/common/voldemort/config/single-store.xml";

    @Override
    public void setUp() throws IOException {
        VoldemortTestConstants.getSingleStoreDefinitionsXml();
        this.cluster = ServerTestUtils.getLocalCluster(2);
        this.node = cluster.getNodes().iterator().next();
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        servers = new ArrayList<VoldemortServer>();
        servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                            0,
                                                                                            TestUtils.createTempDir()
                                                                                                     .getAbsolutePath(),
                                                                                            null,
                                                                                            STORES_XML,
                                                                                            new Properties()),
                                                         cluster));
        servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                            1,
                                                                                            TestUtils.createTempDir()
                                                                                                     .getAbsolutePath(),
                                                                                            null,
                                                                                            STORES_XML,
                                                                                            new Properties()),
                                                         cluster));

        v1 = getVersioned(0, 1, 1, 1, 1, 1);
        v2 = getVersioned(0, 0, 1, 1, 1, 1);
        defaultStoreClient = storeClientFactory.getStoreClient("test");
        socketStore = ServerTestUtils.getSocketStore(socketStoreFactory,
                                                     "test",
                                                     node.getSocketPort(),
                                                     RequestFormatType.VOLDEMORT_V1);
    }

    @Override
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
}
