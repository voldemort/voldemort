package voldemort.client;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.AdminClientPool;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;

public class AdminClientPoolTest {
    private final static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private final static int MAX_CLIENTS = 5;
    private final static int NUM_SERVERS = 1;
    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClientPool pool;
    private final ClientConfig clientConfig;

    public AdminClientPoolTest() throws IOException {
        servers = new VoldemortServer[NUM_SERVERS];
        int partitionMap[][] = { { 0, 1, 2, 3 } };
        cluster = ServerTestUtils.startVoldemortCluster(servers, partitionMap, new Properties(), storesXmlfile);

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        clientConfig = new ClientConfig().setBootstrapUrls(bootstrapUrl);
        pool = new AdminClientPool(MAX_CLIENTS, new AdminClientConfig(), clientConfig);
    }

    @After
    public void tearDown() throws IOException {
        pool.close();
        for (VoldemortServer server : servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    @Test
    public void testCreate() {
        AdminClient client1 = pool.checkout();
        Assert.assertEquals("Nothing should exist in cache", 0, pool.size());

        pool.checkin(client1);

        Assert.assertEquals("Size should be 1", 1, pool.size());
        AdminClient client2 = pool.checkout();

        Assert.assertSame("CheckOut after checkin is not returning the same", client1, client2);
        Assert.assertEquals("Size should be 0 again", 0, pool.size());
    }
    
    @Test
    public void testMulitCreate() {
        final int SIZE = MAX_CLIENTS + 2;
        AdminClient[] clients = new AdminClient[SIZE];

        for (int i = 0; i < SIZE; i++) {
            clients[i] = pool.checkout();
            Assert.assertEquals("Nothing should exist in cache", 0, pool.size());
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            pool.checkin(clients[i]);
            Assert.assertEquals("Different size of pool", i + 1, pool.size());
        }

        for (int i = MAX_CLIENTS; i < SIZE; i++) {
            pool.checkin(clients[i]);
            Assert.assertEquals("No more additional elements", MAX_CLIENTS, pool.size());
        }
    }

    @Test
    public void testClose() {
        AdminClient client = pool.checkout();
        
        pool.close();
        try {
            pool.checkout();
            Assert.fail(" checkout should have failed");
        } catch (IllegalStateException ex) {

        }

        try {
            pool.checkin(client);
            Assert.fail(" checkin should have failed");
        } catch (IllegalStateException ex) {

        }

        try {
            pool.size();
            Assert.fail(" size should have failed ");
        } catch (IllegalStateException ex) {

        }
    }
}
