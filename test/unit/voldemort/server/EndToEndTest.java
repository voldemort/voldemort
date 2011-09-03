package voldemort.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Provides an unmocked end to end unit test of a Voldemort cluster.
 * 
 */
@RunWith(Parameterized.class)
public class EndToEndTest {

    private static final String STORE_NAME = "test-readrepair-memory";
    private static final String STORES_XML = "test/common/voldemort/config/stores.xml";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private final boolean useNio;

    private List<VoldemortServer> servers;
    private Cluster cluster;
    private StoreClient<String, String> storeClient;

    public EndToEndTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 2, 4, 6 }, { 1, 3, 5, 7 } });
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
        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        storeClient = storeClientFactory.getStoreClient(STORE_NAME);
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
    }

    /**
     * Test the basic get/getAll/put/delete functionality.
     */
    @Test
    public void testSanity() {
        storeClient.put("Belarus", "Minsk");
        storeClient.put("Russia", "Moscow");
        storeClient.put("Ukraine", "Kiev");
        storeClient.put("Kazakhstan", "Almaty");

        Versioned<String> v1 = storeClient.get("Belarus");
        assertEquals("get/put work as expected", "Minsk", v1.getValue());

        storeClient.put("Kazakhstan", "Astana");
        Versioned<String> v2 = storeClient.get("Kazakhstan");
        assertEquals("clobbering a value works as expected, we have read-your-writes consistency",
                     "Astana",
                     v2.getValue());

        Map<String, Versioned<String>> capitals = storeClient.getAll(Arrays.asList("Russia",
                                                                                   "Ukraine",
                                                                                   "Japan"));

        assertEquals("getAll works as expected", "Moscow", capitals.get("Russia").getValue());
        assertEquals("getAll works as expected", "Kiev", capitals.get("Ukraine").getValue());

        assertFalse("getAll works as expected", capitals.containsKey("Japan"));

        storeClient.delete("Ukraine");
        assertNull("delete works as expected", storeClient.get("Ukraine"));
    }

    /**
     * Test the new put that returns the new version
     */
    @Test
    public void testPutReturnVersion() {
        Version baseVersion = new VectorClock();
        Version oldVersion = null;
        Version newVersion = null;
        Versioned<String> getVersioned = null;
        String oldValue = null;
        String newValue = null;

        for(int i = 0; i < 5; i++) {
            oldValue = "value" + i;
            newValue = "value" + (i + 1);
            oldVersion = storeClient.put("key1", Versioned.value(oldValue, baseVersion));
            newVersion = storeClient.put("key1",
                                         Versioned.value(newValue,
                                                         ((VectorClock) oldVersion).clone()));
            getVersioned = storeClient.get("key1");
            baseVersion = newVersion;

            verifyResults(oldVersion, newVersion, getVersioned, newValue);
        }
    }

    @Test
    public void testUnversionedPutReturnVersion() {
        Version oldVersion = null;
        Version newVersion = null;
        Versioned<String> getVersioned = null;
        String oldValue = null;
        String newValue = null;

        for(int i = 0; i < 5; i++) {
            oldValue = "value" + i;
            newValue = "value" + i + 1;
            oldVersion = storeClient.put("key1", oldValue);
            newVersion = storeClient.put("key1", newValue);
            assertEquals("Version did not advance", Occurred.AFTER, newVersion.compare(oldVersion));
            getVersioned = storeClient.get("key1");

            verifyResults(oldVersion, newVersion, getVersioned, newValue);
        }
    }

    private void verifyResults(Version oldVersion,
                               Version newVersion,
                               Versioned<String> getVersioned,
                               String newValue) {
        // make sure version advances between two puts
        assertEquals("Versions of put did not advance",
                     Occurred.AFTER,
                     newVersion.compare(oldVersion));

        // make sure version of last put equals version of the get
        assertEquals("Version of put is larger than version of get",
                     Occurred.BEFORE /* before can mean equal, funny! */,
                     newVersion.compare(getVersioned.getVersion()));
        assertEquals("Version of put is smaller than version of get",
                     Occurred.BEFORE /* before can mean equal, funny! */,
                     getVersioned.getVersion().compare(newVersion));

        // make sure we get what we just put in
        assertEquals("Value of put does not match value of get", newValue, getVersioned.getValue());
    }
}
