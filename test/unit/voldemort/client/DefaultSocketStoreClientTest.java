package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class DefaultSocketStoreClientTest {

    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private static AtomicBoolean running = new AtomicBoolean(true);
    private List<StoreDefinition> storeDefs;
    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClient adminClient;

    public static String socketUrl = "tcp://localhost:6667";
    protected StoreClient<String, String> client;
    protected int nodeId;
    protected Time time;

    @Before
    public void setUp() throws Exception {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });

        servers = new VoldemortServer[2];
        storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile));

        servers[0] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(useNio,
                                                                                             0,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);
        servers[1] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(useNio,
                                                                                             1,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Test
    public void test() {
        client.put("k", Versioned.value("v"));
        Versioned<String> v = client.get("k");
        assertEquals("GET should return the version set by PUT.", "v", v.getValue());
        VectorClock expected = new VectorClock();
        expected.incrementVersion(nodeId, time.getMilliseconds());
        assertEquals("The version should be incremented after a put.", expected, v.getVersion());
        try {
            client.put("k", Versioned.value("v"));
            fail("Put of obsolete version should throw exception.");
        } catch(ObsoleteVersionException e) {
            // this is good
        }
        // PUT of a concurrent version should succeed
        client.put("k",
                   new Versioned<String>("v2",
                                         new VectorClock().incremented(nodeId + 1,
                                                                       time.getMilliseconds())));
        assertEquals("GET should return the new value set by PUT.", "v2", client.getValue("k"));
        assertEquals("GET should return the new version set by PUT.",
                     expected.incremented(nodeId + 1, time.getMilliseconds()),
                     client.get("k").getVersion());
        client.delete("k");
        assertNull("After a successful delete(k), get(k) should return null.", client.get("k"));
    }

    @Test
    public void testClientRegistryHappyPath() {
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(socketUrl)
                                                      .setClientContextName("testClientRegistryHappyPath");
        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        this.client = socketFactory.getStoreClient(testStoreName);
        client.put("k", "v");
        adminClient.fetchEntries(0, testStoreName, null, null, false);
        adminClient.fetchEntries(1, testStoreName, null, null, false);
        // TODO: verify that the values in registry are correct.
    }

}
