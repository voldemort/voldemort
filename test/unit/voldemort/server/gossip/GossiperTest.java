package voldemort.server.gossip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.Attempt;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Tests {@link voldemort.server.gossip.Gossiper}
 */
@RunWith(Parameterized.class)
public class GossiperTest extends TestCase {

    private List<VoldemortServer> servers = new ArrayList<VoldemortServer>();
    private Cluster cluster;
    private Properties props = new Properties();
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private final boolean useNio;

    public GossiperTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        props.put("enable.gossip", "true");
        props.put("gossip.interval.ms", "250");

        // Start all in parallel to avoid exceptions during gossip

        cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                { 8, 9, 10, 11 } });
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        final CountDownLatch countDownLatch = new CountDownLatch(3);

        for(int i = 0; i < 3; i++) {
            final int j = i;
            executorService.submit(new Runnable() {

                public void run() {
                    try {
                        servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                                            j,
                                                                                                            TestUtils.createTempDir()
                                                                                                                     .getAbsolutePath(),
                                                                                                            null,
                                                                                                            storesXmlfile,
                                                                                                            props),
                                                                         cluster));
                        countDownLatch.countDown();
                    } catch(IOException e) {
                        throw new RuntimeException();
                    }
                }
            });
        }

        try {
            countDownLatch.await();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    @After
    public void tearDown() {
        socketStoreFactory.close();
    }

    private AdminClient getAdminClient(Cluster newCluster, VoldemortConfig newServerConfig) {
        return new AdminClient(newCluster, new AdminClientConfig());
    }

    @Test
    public void testGossiper() throws Exception {
        // First create a new cluster:
        // Allocate ports for all nodes in the new cluster, to match existing
        // cluster
        int originalSize = cluster.getNumberOfNodes();
        int numOriginalPorts = originalSize * 3;
        int ports[] = new int[numOriginalPorts + 3];
        for(int i = 0, j = 0; i < originalSize; i++, j += 3) {
            Node node = cluster.getNodeById(i);
            System.arraycopy(new int[] { node.getHttpPort(), node.getSocketPort(),
                    node.getAdminPort() }, 0, ports, j, 3);
        }

        System.arraycopy(ServerTestUtils.findFreePorts(3), 0, ports, numOriginalPorts, 3);

        // Create a new partitioning scheme with room for a new server
        final Cluster newCluster = ServerTestUtils.getLocalCluster(originalSize + 1,
                                                                   ports,
                                                                   new int[][] { { 0, 4, 8 },
                                                                           { 1, 5, 9 },
                                                                           { 2, 6, 10 },
                                                                           { 3, 7, 11 } });

        // Create a new server
        VoldemortServer newServer = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                                            3,
                                                                                                            TestUtils.createTempDir()
                                                                                                                     .getAbsolutePath(),
                                                                                                            null,
                                                                                                            storesXmlfile,
                                                                                                            props),
                                                                         newCluster);
        servers.add(newServer);

        // Wait a while until the new server starts
        try {
            Thread.sleep(500);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Get the new cluster.xml
        AdminClient localAdminClient = getAdminClient(newCluster, newServer.getVoldemortConfig());

        Versioned<String> versionedClusterXML = localAdminClient.getRemoteMetadata(3,
                                                                                   MetadataStore.CLUSTER_KEY);

        // Increment the version, let what would be the "donor node" know about
        // it
        // to seed the Gossip.
        Version version = versionedClusterXML.getVersion();
        ((VectorClock) version).incrementVersion(3, ((VectorClock) version).getTimestamp() + 1);
        ((VectorClock) version).incrementVersion(0, ((VectorClock) version).getTimestamp() + 1);

        localAdminClient.updateRemoteMetadata(0, MetadataStore.CLUSTER_KEY, versionedClusterXML);
        localAdminClient.updateRemoteMetadata(3, MetadataStore.CLUSTER_KEY, versionedClusterXML);

        try {
            Thread.sleep(500);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Wait up to five seconds for Gossip to spread
        try {
            TestUtils.assertWithBackoff(5000, new Attempt() {

                public void checkCondition() {
                    int serversSeen = 0;
                    // Now verify that we have gossiped correctly
                    for(VoldemortServer server: servers) {
                        Cluster clusterAtServer = server.getMetadataStore().getCluster();
                        int nodeId = server.getMetadataStore().getNodeId();
                        assertEquals("server " + nodeId + " has heard "
                                             + " the gossip about number of nodes",
                                     clusterAtServer.getNumberOfNodes(),
                                     newCluster.getNumberOfNodes());
                        assertEquals("server " + nodeId + " has heard "
                                             + " the gossip about partitions",
                                     clusterAtServer.getNodeById(nodeId).getPartitionIds(),
                                     newCluster.getNodeById(nodeId).getPartitionIds());
                        serversSeen++;
                    }
                    assertEquals("saw all servers", serversSeen, servers.size());
                }
            });
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
