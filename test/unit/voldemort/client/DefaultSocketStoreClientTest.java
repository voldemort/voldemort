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
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class DefaultSocketStoreClientTest {

    private static String testStoreName = "test-basic-replication-memory";
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

    public static String socketUrl = "";
    protected StoreClient<String, String> client;
    protected int nodeId;
    protected Time time;
    private boolean success = true;

    private class ParallelClient extends Thread {

        private final StoreClient<String, String> parallelClient;

        public ParallelClient(StoreClient<String, String> client) {
            this.parallelClient = client;
        }

        @Override
        public void run() {
            System.out.println("Doing a whole bunch of puts");
            try {
                for(int i = 0; i < 10000; i++)
                    this.parallelClient.put("my-key" + i, "my-value" + i);
            } catch(VoldemortException ve) {
                if(!(ve instanceof ObsoleteVersionException)) {
                    success = false;
                    ve.printStackTrace();
                    fail("Exception occured while doing a put.");
                }
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });

        servers = new VoldemortServer[2];
        storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile));

        boolean useNio = true;
        servers[0] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(true,
                                                                                             0,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);
        servers[1] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(true,
                                                                                             1,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);

        adminClient = ServerTestUtils.getAdminClient(cluster);
        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();

        ClientConfig clientConfig = new ClientConfig().setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(socketUrl);

        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        this.client = socketFactory.getStoreClient(testStoreName);
        this.nodeId = 0;
        this.time = SystemTime.INSTANCE;
    }

    // @Test
    public void test() {
        client.delete("k");
        client.put("k", Versioned.value("v"));
        Versioned<String> v = client.get("k");
        assertEquals("GET should return the version set by PUT.", "v", v.getValue());
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
        client.delete("k");
        assertNull("After a successful delete(k), get(k) should return null.", client.get("k"));
    }

    // @Test
    public void testBatchOps() {
        success = true;

        for(int j = 0; j < 10; j++) {
            (new ParallelClient(client)).start();
        }

        try {
            Thread.sleep(10000);
        } catch(Exception e) {}
        assertEquals(success, true);
    }

    /*
     * Test that a Node failure does not affect system stores.
     */
    @Test
    public void testSystemStoreFailures() {
        success = true;
        for(int j = 0; j < 100; j++) {
            (new ParallelClient(client)).start();
        }

        servers[1].stop();
        try {
            Thread.sleep(10000);
        } catch(Exception e) {}

        assertEquals(success, true);
        try {
            servers[1].start();
        } catch(Exception e) {
            e.printStackTrace();
            fail("Cannot restart Voldemort Server");
        }
    }
}
