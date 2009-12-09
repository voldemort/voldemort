package voldemort.server.gossip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import voldemort.Attempt;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * @author afeinberg
 */
public class GossiperTest extends TestCase {

    private List<VoldemortServer> servers = new ArrayList<VoldemortServer>();
    private Cluster cluster;

    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                { 8, 9, 10, 11 } });
        servers.add(ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(0,
                                                                                            TestUtils.createTempDir()
                                                                                                     .getAbsolutePath(),
                                                                                            null,
                                                                                            storesXmlfile),
                                                         cluster));
        servers.add(ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(1,
                                                                                            TestUtils.createTempDir()
                                                                                                     .getAbsolutePath(),
                                                                                            null,
                                                                                            storesXmlfile),
                                                         cluster));
    }

    private AdminClient getAdminClient(Cluster newCluster, VoldemortConfig newServerConfig) {
        ClientConfig clientConfig = new ClientConfig().setMaxConnectionsPerNode(8)
                                                      .setMaxThreads(8)
                                                      .setConnectionTimeout(newServerConfig.getAdminConnectionTimeout(),
                                                                            TimeUnit.MILLISECONDS)
                                                      .setSocketTimeout(newServerConfig.getSocketTimeoutMs(),
                                                                        TimeUnit.MILLISECONDS)
                                                      .setSocketBufferSize(newServerConfig.getAdminSocketBufferSize());
        return new ProtoBuffAdminClientRequestFormat(newCluster, clientConfig);
    }

    public void testGossiper() throws Exception {
        // First create a new cluster:
        // Allocate ports for all nodes in the new cluster, to match existing
        // cluster
        int portIdx = 0;
        int ports[] = new int[3 * (cluster.getNumberOfNodes() + 1)];
        for(Node node: cluster.getNodes()) {
            ports[portIdx++] = node.getHttpPort();
            ports[portIdx++] = node.getSocketPort();
            ports[portIdx++] = node.getAdminPort();
        }

        int[] freeports = ServerTestUtils.findFreePorts(3);
        ports[portIdx++] = freeports[0];
        ports[portIdx++] = freeports[1];
        ports[portIdx] = freeports[2];

        // Create a new partitioning scheme with room for a new server
        final Cluster newCluster = ServerTestUtils.getLocalCluster(cluster.getNumberOfNodes() + 1,
                                                                   ports,
                                                                   new int[][] { { 0, 1, 2 },
                                                                           { 3, 4, 5 },
                                                                           { 6, 7, 8 },
                                                                           { 9, 10, 11 } });

        // Start the new server
        VoldemortServer newServer = ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(3,
                                                                                                            TestUtils.createTempDir()
                                                                                                                     .getAbsolutePath(),
                                                                                                            null,
                                                                                                            storesXmlfile),
                                                                         newCluster);
        servers.add(newServer);

        // Wait a while until it starts
        try {
            Thread.sleep(500);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Get the new cluster.XML
        AdminClient localAdminClient = getAdminClient(newCluster, newServer.getVoldemortConfig());

        Versioned<String> versionedClusterXML = localAdminClient.getRemoteMetadata(3,
                                                                                   MetadataStore.CLUSTER_KEY);

        // Increment the version, let what would be the "donor node" know about
        // it
        // (this will seed the gossip)
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

        // Start a thread pool for gossipers and start gossiping
        ExecutorService executorService = Executors.newFixedThreadPool(newCluster.getNumberOfNodes() + 1);

        List<Gossiper> gossipers = new ArrayList<Gossiper>(newCluster.getNumberOfNodes());
        for(VoldemortServer server: servers) {
            Gossiper gossiper = new Gossiper(server.getMetadataStore(),
                                             getAdminClient(server.getMetadataStore().getCluster(),
                                                            server.getVoldemortConfig()),
                                             50);
            gossiper.start();
            executorService.submit(gossiper);
            gossipers.add(gossiper);
        }

        // Wait up to a second for gossip to spread
        try {
            TestUtils.assertWithBackoff(1000, new Attempt() {

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
        } finally {
            for(Gossiper gossiper: gossipers)
                gossiper.stop();
        }
    }
}
