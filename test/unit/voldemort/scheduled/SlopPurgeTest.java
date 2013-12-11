package voldemort.scheduled;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.serialization.SlopSerializer;
import voldemort.server.VoldemortServer;
import voldemort.store.routed.NodeValue;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class SlopPurgeTest {

    private Cluster cluster;
    private VoldemortServer[] servers;
    private AdminClient adminClient;
    private SlopSerializer slopSerializer;
    private VoldemortServer purgedServer;
    private static int PURGE_SERVER_ID = 0;

    @Before
    public void setUp() throws Exception {
        cluster = ServerTestUtils.getLocalZonedCluster(6,
                                                       3,
                                                       new int[] { 0, 0, 1, 1, 2, 2 },
                                                       new int[][] { { 0 }, { 2 }, { 4 }, { 1 },
                                                               { 3 }, { 5 } });

        servers = new VoldemortServer[cluster.getNodes().size()];
        slopSerializer = new SlopSerializer();

        Properties serverProperties = new Properties();
        // Schedule the slop pusher far far far out in the future, so it won't
        // run during the test
        serverProperties.setProperty("slop.frequency.ms", "" + (Integer.MAX_VALUE));
        // Also no auto purging so we are sure that the only thing deleting the
        // slops is the purge job
        serverProperties.setProperty("auto.purge.dead.slops", "false");

        cluster = ServerTestUtils.startVoldemortCluster(servers,
                                                        null,
                                                        null,
                                                        "test/common/voldemort/config/three-stores-with-zones.xml",
                                                        serverProperties,
                                                        cluster);

        for(VoldemortServer server: servers) {
            if(server.getIdentityNode().getId() == PURGE_SERVER_ID) {
                purgedServer = server;
                break;
            }
        }

        Properties adminProperties = new Properties();
        adminProperties.setProperty("max_connections", "2");
        adminClient = new AdminClient(servers[0].getMetadataStore().getCluster(),
                                      new AdminClientConfig(adminProperties),
                                      new ClientConfig());
    }

    private int numSlopsInServer(List<Versioned<Slop>> slops) {
        int count = 0;
        for(Versioned<Slop> slop: slops) {
            List<Versioned<byte[]>> slopEntry = adminClient.storeOps.getNodeKey("slop",
                                                                                PURGE_SERVER_ID,
                                                                                slop.getValue()
                                                                                    .makeKey());
            if(slopEntry.size() > 0) {
                count++;
            }
        }
        return count;
    }

    private void populateSlops(List<Versioned<Slop>> slops) {
        for(Versioned<Slop> slop: slops) {
            VectorClock clock = TestUtils.getClock(1);
            NodeValue<ByteArray, byte[]> nodeValue = new NodeValue<ByteArray, byte[]>(PURGE_SERVER_ID,
                                                                                      slop.getValue()
                                                                                          .makeKey(),
                                                                                      new Versioned<byte[]>(slopSerializer.toBytes(slop.getValue()),
                                                                                                            clock));
            adminClient.storeOps.putNodeKeyValue("slop", nodeValue);
        }
    }

    @Test
    public void testPurgeByNodeIds() {

        // these will perish
        List<Versioned<Slop>> node2Slops = ServerTestUtils.createRandomSlops(2, 10, false, "astore");
        List<Versioned<Slop>> node4Slops = ServerTestUtils.createRandomSlops(4, 10, false, "cstore");

        // these will be around
        List<Versioned<Slop>> node1Slops = ServerTestUtils.createRandomSlops(1, 10, false, "bstore");
        List<Versioned<Slop>> node3Slops = ServerTestUtils.createRandomSlops(3, 10, false, "bstore");

        List<Versioned<Slop>> slops = new ArrayList<Versioned<Slop>>();
        slops.addAll(node2Slops);
        slops.addAll(node3Slops);
        slops.addAll(node1Slops);
        slops.addAll(node4Slops);
        Collections.shuffle(slops);

        // Populate the slops.
        populateSlops(slops);

        // Kick off the slop purge job
        adminClient.storeMntOps.slopPurgeJob(PURGE_SERVER_ID, Lists.newArrayList(2, 4), -1, null);
        ServerTestUtils.waitForAsyncOperationOnServer(purgedServer, "Purge", 5000);

        assertEquals("All slops from node 2 should be gone", 0, numSlopsInServer(node2Slops));
        assertEquals("All slops from node 4 should be gone", 0, numSlopsInServer(node4Slops));
        assertEquals("All slops from node 1 should be still there",
                     node1Slops.size(),
                     numSlopsInServer(node1Slops));
        assertEquals("All slops from node 3 should be still there",
                     node3Slops.size(),
                     numSlopsInServer(node3Slops));
    }

    @Test
    public void testPurgeByZone() {
        // these will perish, since nodes 2,3 are in zone 1.
        List<Versioned<Slop>> node2Slops = ServerTestUtils.createRandomSlops(2, 10, false, "astore");
        List<Versioned<Slop>> node3Slops = ServerTestUtils.createRandomSlops(3, 10, false, "cstore");

        // these will be around
        List<Versioned<Slop>> node1Slops = ServerTestUtils.createRandomSlops(1, 10, false, "bstore");
        List<Versioned<Slop>> node5Slops = ServerTestUtils.createRandomSlops(5, 10, false, "astore");

        List<Versioned<Slop>> slops = new ArrayList<Versioned<Slop>>();
        slops.addAll(node2Slops);
        slops.addAll(node3Slops);
        slops.addAll(node1Slops);
        slops.addAll(node5Slops);
        Collections.shuffle(slops);

        // Populate the slops.
        populateSlops(slops);

        // Kick off the slop purge job
        adminClient.storeMntOps.slopPurgeJob(PURGE_SERVER_ID, null, 1, null);
        ServerTestUtils.waitForAsyncOperationOnServer(purgedServer, "Purge", 5000);

        assertEquals("All slops from node 2 should be gone", 0, numSlopsInServer(node2Slops));
        assertEquals("All slops from node 3 should be gone", 0, numSlopsInServer(node3Slops));
        assertEquals("All slops from node 1 should be still there",
                     node1Slops.size(),
                     numSlopsInServer(node1Slops));
        assertEquals("All slops from node 5 should be still there",
                     node5Slops.size(),
                     numSlopsInServer(node5Slops));
    }

    @Test
    public void testPurgeByStore() {
        // these will perish, since they belong to "astore" and "bstore"
        List<Versioned<Slop>> node2Slops = ServerTestUtils.createRandomSlops(2, 10, false, "astore");
        List<Versioned<Slop>> node5Slops = ServerTestUtils.createRandomSlops(5, 10, false, "bstore");

        // these will be around
        List<Versioned<Slop>> node1Slops = ServerTestUtils.createRandomSlops(1, 10, false, "cstore");

        List<Versioned<Slop>> slops = new ArrayList<Versioned<Slop>>();
        slops.addAll(node2Slops);
        slops.addAll(node1Slops);
        slops.addAll(node5Slops);
        Collections.shuffle(slops);

        // Populate the slops.
        populateSlops(slops);

        // Kick off the slop purge job
        adminClient.storeMntOps.slopPurgeJob(PURGE_SERVER_ID,
                                             null,
                                             -1,
                                             Lists.newArrayList("astore", "bstore"));
        ServerTestUtils.waitForAsyncOperationOnServer(purgedServer, "Purge", 5000);

        assertEquals("All slops from node 2 should be gone", 0, numSlopsInServer(node2Slops));
        assertEquals("All slops from node 5 should be gone", 0, numSlopsInServer(node5Slops));
        assertEquals("All slops from node 1 should be still there",
                     node1Slops.size(),
                     numSlopsInServer(node1Slops));

    }

    @Test
    public void testPurgeMixed() {
        // these will perish, since
        // 1) Node 2 is in zone 1
        List<Versioned<Slop>> node2Slops = ServerTestUtils.createRandomSlops(2, 10, false, "astore");
        // 2) Slops are for store "cstore"
        List<Versioned<Slop>> node3Slops = ServerTestUtils.createRandomSlops(3, 10, false, "cstore");
        // 3) Slop is for node 1
        List<Versioned<Slop>> node1Slops = ServerTestUtils.createRandomSlops(1, 10, false, "bstore");

        // these will be around
        List<Versioned<Slop>> node5Slops = ServerTestUtils.createRandomSlops(5, 10, false, "astore");

        List<Versioned<Slop>> slops = new ArrayList<Versioned<Slop>>();
        slops.addAll(node2Slops);
        slops.addAll(node3Slops);
        slops.addAll(node1Slops);
        slops.addAll(node5Slops);
        Collections.shuffle(slops);

        // Populate the slops.
        populateSlops(slops);

        // Kick off the slop purge job
        adminClient.storeMntOps.slopPurgeJob(PURGE_SERVER_ID,
                                             Lists.newArrayList(1),
                                             1,
                                             Lists.newArrayList("cstore"));
        ServerTestUtils.waitForAsyncOperationOnServer(purgedServer, "Purge", 5000);

        assertEquals("All slops from node 2 should be gone", 0, numSlopsInServer(node2Slops));
        assertEquals("All slops from node 3 should be gone", 0, numSlopsInServer(node3Slops));
        assertEquals("All slops from node 1 should be gone", 0, numSlopsInServer(node1Slops));
        assertEquals("All slops from node 5 should be still there",
                     node5Slops.size(),
                     numSlopsInServer(node5Slops));

    }

    @After
    public void tearDown() {
        for(VoldemortServer server: servers) {
            server.stop();
        }
        adminClient.close();
    }
}
