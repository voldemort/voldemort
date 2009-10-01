package voldemort.client;

import com.google.common.collect.ImmutableList;
import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClientRequestFormat;
import voldemort.client.protocol.admin.NativeAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: afeinber
 * Date: Sep 30, 2009
 * Time: 12:56:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProtoBuffAdminServiceBasicTest extends TestCase {
    private static String storeName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    VoldemortConfig config;
    VoldemortServer server;
    Cluster cluster;

    @Override
    public void setUp() throws IOException {
       // start 2 node cluster with free ports
        int[] ports = ServerTestUtils.findFreePorts(2);
        Node node0 = new Node(0, "localhost", ports[0], ports[1], Arrays.asList(new Integer[] { 0,
                1 }));

        ports = ServerTestUtils.findFreePorts(2);
        Node node1 = new Node(1, "localhost", ports[0], ports[1], Arrays.asList(new Integer[] { 2,
                3 }));

        cluster = new Cluster("admin-service-test", Arrays.asList(new Node[] { node0, node1 }));
        config = ServerTestUtils.createServerConfig(0,
                                                    TestUtils.createTempDir().getAbsolutePath(),
                                                    null,
                                                    storesXmlfile);
        server = new VoldemortServer(config, cluster);
        server.start();
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        server.stop();
    }

    public AdminClientRequestFormat getAdminClient() {
        return ServerTestUtils.getAdminClient(server.getIdentityNode(), server.getMetadataStore(), true);
    }

    public void testUpdateClusterMetadata() {
        Cluster cluster = server.getMetadataStore().getCluster();
        List<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, ImmutableList.of(4,5)));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        AdminClientRequestFormat client = getAdminClient();
        client.updateClusterMetadata(server.getIdentityNode().getId(), updatedCluster);

        assertEquals("Cluster should match", updatedCluster, server.getMetadataStore().getCluster());
        assertEquals("AdminClient.getMetdata() should match",
                     client.getClusterMetadata(server.getIdentityNode().getId()).getValue(),
                     updatedCluster);
        
    }

    
}
