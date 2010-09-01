package voldemort.store.rebalancing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Test {@link RebootstrappingStore}
 */
public class RebootstrappingStoreTest {

    private final static String STORE_NAME = "test";
    private final static String STORES_XML = "test/common/voldemort/config/single-store.xml";

    private final int[][] startCluster = new int[][] { { 0, 1 }, {} };

    private Map<String, String> entries;
    private Cluster cluster;
    private List<VoldemortServer> servers;
    private StoreClient<String, String, String> storeClient;

    @Before
    public void setUp() throws Exception {
        entries = Maps.newHashMap();
        entries.put("a", "1");
        entries.put("b", "2");
        cluster = ServerTestUtils.getLocalCluster(2, startCluster);
        servers = Lists.newArrayList();
        Properties props = new Properties();
        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);

        for(Node node: cluster.getNodes()) {
            VoldemortConfig config = ServerTestUtils.createServerConfig(false,
                                                                        node.getId(),
                                                                        TestUtils.createTempDir()
                                                                                 .getAbsolutePath(),
                                                                        null,
                                                                        STORES_XML,
                                                                        props);
            servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory, config, cluster));
        }

        String bootstrapUrl = cluster.getNodeById(0).getSocketUrl().toString();
        storeClient = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)).getStoreClient(STORE_NAME);

        for(Map.Entry<String, String> entry: entries.entrySet())
            storeClient.put(entry.getKey(), entry.getValue());
    }

    @After
    public void tearDown() {
        if(servers != null)
            for(VoldemortServer server: servers)
                server.stop();
    }

    public void rebalance() {
        assert servers != null && servers.size() > 1;

        VoldemortConfig config = servers.get(0).getVoldemortConfig();
        AdminClient adminClient = RebalanceUtils.createTempAdminClient(config, cluster, 2, 4);
        int req = adminClient.migratePartitions(0, 1, STORE_NAME, ImmutableList.of(0, 1), null);
        adminClient.waitForCompletion(1, req, 5, TimeUnit.SECONDS);
        Versioned<Cluster> versionedCluster = adminClient.getRemoteCluster(0);
        Node node0 = versionedCluster.getValue().getNodeById(0);
        Node node1 = versionedCluster.getValue().getNodeById(1);
        Node newNode0 = new Node(node0.getId(),
                                 node0.getHost(),
                                 node0.getHttpPort(),
                                 node0.getSocketPort(),
                                 node0.getAdminPort(),
                                 ImmutableList.<Integer> of());
        Node newNode1 = new Node(node1.getId(),
                                 node1.getHost(),
                                 node1.getHttpPort(),
                                 node1.getSocketPort(),
                                 node1.getAdminPort(),
                                 ImmutableList.of(0, 1));
        int deleted = adminClient.deletePartitions(0, STORE_NAME, ImmutableList.of(0, 1), null);
        assert deleted > 0;
        Cluster newCluster = new Cluster(cluster.getName(), ImmutableList.of(newNode0, newNode1));
        for(Node node: cluster.getNodes()) {
            VectorClock clock = (VectorClock) versionedCluster.getVersion();
            clock.incrementVersion(node.getId(), System.currentTimeMillis());

            adminClient.updateRemoteCluster(node.getId(), newCluster, clock);
        }
    }

    @Test
    public void testGet() {
        Versioned<String> r0 = storeClient.get("a");
        Versioned<String> r1 = storeClient.get("b");
        assertEquals("1", r0.getValue());
        assertEquals("2", r1.getValue());
        rebalance();
        r0 = storeClient.get("a");
        r1 = storeClient.get("b");
        assertEquals("#1 get() okay after re-bootstrap", "1", r0.getValue());
        assertEquals("#2 get() okay after re-bootstrap", "2", r1.getValue());
    }

    @Test
    public void testPut() {
        Versioned<String> r0 = storeClient.get("a");
        assertEquals("1", r0.getValue());
        rebalance();
        storeClient.put("c", "3");
        assertEquals("put() okay after re-bootstrap", "3", storeClient.get("c").getValue());
    }

    @Test
    public void testDelete() {

    }

    @Test
    public void testGetAll() {
        Versioned<String> r0 = storeClient.get("a");
        assertEquals("1", r0.getValue());
        rebalance();
        Map<String, Versioned<String>> res = storeClient.getAll(ImmutableList.of("a", "b"));

        assertTrue("getAll() contains a", res.containsKey("a"));
        assertTrue("getAll() contains b", res.containsKey("b"));
        assertEquals("getAll() returns correct value #1", "1", res.get("a").getValue());
        assertEquals("getAll() returns correct value #2", "2", res.get("b").getValue());
    }
}
