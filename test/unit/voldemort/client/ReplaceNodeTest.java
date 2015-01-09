package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.TestSocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.tools.ReplaceNodeCLI;

public class ReplaceNodeTest {
    
    private static final int PARALELLISM = 3;


    private static final String ORIGINAL_STORES_XML = "test/common/voldemort/config/stores-rw-replication.xml";
    
    private static final String REPLACEMENT_STORES_XML = "test/common/voldemort/config/single-store.xml";

    private static final String STORE322_NAME = "test322";
    private static final String STORE211_NAME = "test211";

    private SocketStoreFactory originalSocketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private SocketStoreFactory replacementSocketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                          10000,
                                                                                          100000,
                                                                                          32 * 1024);

    private VoldemortServer[] originalServers;

    private Cluster originalCluster;

    private String originalBootstrapUrl;

    private VoldemortServer[] otherServers;

    private Map<Integer, VoldemortServer> finalServers;

    private static String getAdminUrl(Node node) {
        return "tcp://" + node.getHost() + ":" + node.getAdminPort();
    }

    @Before
    public void setUp() throws IOException {

        final boolean USE_NIO = true;

        Properties serverProperties = new Properties();
        serverProperties.setProperty("client.max.connections.per.node", "20");
        serverProperties.setProperty("slop.frequency.ms", "8000");

        int originalServerCount = 6;

        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10, 11 },
                { 12, 13, 14, 15 }, { 16, 17, 18, 19 }, { 20, 21, 22, 23 } };

        originalServers = new VoldemortServer[originalServerCount];
        originalCluster = ServerTestUtils.startVoldemortCluster(originalServerCount,
                                                        originalServers,
                                                        partitionMap,
                                                        originalSocketStoreFactory,
                                                        USE_NIO,
                                                        null,
                                                        ORIGINAL_STORES_XML,
                                                        serverProperties);

        Node node = originalCluster.getNodeById(4);
        originalBootstrapUrl = getAdminUrl(node);
        
        finalServers = new HashMap<Integer, VoldemortServer>();
        for(VoldemortServer server : originalServers) {
            finalServers.put( server.getIdentityNode().getId() , server);
        }
        
        int replacementServerCount = 1;
        int replacementPartitionMap[][] = { { 0, 1, 2, 3 } };

        otherServers = new VoldemortServer[replacementServerCount];
        ServerTestUtils.startVoldemortCluster(replacementServerCount,
                                              otherServers,
                                                                replacementPartitionMap,
                                                                replacementSocketStoreFactory,
                                                                USE_NIO,
                                                                null,
                                                                REPLACEMENT_STORES_XML,
                                                                serverProperties);
    }

    @After
    public void tearDown() throws IOException {
        
        for(VoldemortServer server: originalServers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        originalSocketStoreFactory.close();
    }
    
    @Test
    public void testNodeDownReplacement() throws Exception {
        final int NODE_DOWN = 0;
        final int REPLACEMENT_NODE = 0;
        
        // This is to simulate the case where a machine failed but hard disk was intact
        // In this case we will move the hard disk to another machine, edit the cluster.xml
        // to point to this host and start this machine.

        // The case is simulated by remembering the cluster of node A.
        // Replace this node with node B. Now create a new server with cluster of
        // node A ( this is to simulate the cluster.xml edit) and data directory of B
        // ( this is to simulate the hard disk move). Now try replacing the node B with
        // newly created node after shutting down the node B.

        Cluster cluster = originalServers[NODE_DOWN].getMetadataStore().getCluster();
        List<StoreDefinition> storeDefs = originalServers[NODE_DOWN].getMetadataStore()
                                                                    .getStoreDefList();
        Node node = originalServers[NODE_DOWN].getIdentityNode();
        
        //Verify the node down scenario first
        final boolean DO_RESTORE = false;
        final boolean STOP_OLD_NODE = true;
        verifyNodeReplacement(NODE_DOWN,
                              otherServers,
                              REPLACEMENT_NODE,
                              STOP_OLD_NODE,
                              DO_RESTORE );

        SocketStoreFactory ssf = new TestSocketStoreFactory();
        String baseDirPath = otherServers[REPLACEMENT_NODE].getVoldemortConfig().getVoldemortHome();
        otherServers[REPLACEMENT_NODE].stop();

        VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                            node.getId(),
                                                                            baseDirPath,
                                                                            cluster,
                                                                            storeDefs,
                                                                            new Properties());
        Assert.assertTrue(config.isSlopEnabled());
        Assert.assertTrue(config.isSlopPusherJobEnabled());
        Assert.assertTrue(config.getAutoPurgeDeadSlops());
        config.setSlopFrequencyMs(8000L);

        VoldemortServer hardDiskMovedServer = ServerTestUtils.startVoldemortServer(ssf, config, cluster);
        
        VoldemortServer[] newCluster = new VoldemortServer[] { hardDiskMovedServer };

        final boolean SKIP_RESTORE = true;
        final boolean DONOT_STOP_OLD_NODE = true;
        verifyNodeReplacement(NODE_DOWN,
                              newCluster,
                              REPLACEMENT_NODE,
                              DONOT_STOP_OLD_NODE,
                              SKIP_RESTORE);
    }

    @Test
    public void testNodeReplacement() throws Exception {
        final int NODE_DOWN = 0;

        final boolean DONOT_SKIP_RESTORE = false;
        final boolean DONOT_STOP_OLD_NODE = false;
        verifyNodeReplacement(NODE_DOWN,
                              otherServers,
                              NODE_DOWN,
                              DONOT_STOP_OLD_NODE,
                              DONOT_SKIP_RESTORE);

    }

    private void verifyNodeReplacement(int originalNode,
                                       VoldemortServer[] replacementServers,
                                       int newNode,
                                       boolean stopOldNode,
                                       boolean skipRestore) throws Exception {

        if(stopOldNode) {
            originalServers[originalNode].stop();
        }

        VoldemortServer replacementServer = replacementServers[newNode];
        Node replacementNode = replacementServer.getIdentityNode();
        String replacementBootstrapUrl = getAdminUrl(replacementNode);

        ReplaceNodeCLI replacer = new ReplaceNodeCLI(originalBootstrapUrl,
                                                     originalNode,
                                                     replacementBootstrapUrl,
                                                     skipRestore,
                                                     PARALELLISM);

        finalServers.put(originalNode, replacementServer);
        verifyReplaceNode(replacer, replacementServer);
    }

    private void verifyNewNodePartOfCluster(Node replacementNode) {
        // Verify if new node is part of the new cluster.
        Cluster cluster = new AdminClient(originalBootstrapUrl,
                                          new AdminClientConfig(),
                                          new ClientConfig()).getAdminClientCluster();

        boolean isNewNodePresent = false;
        for(Node curNode: cluster.getNodes()) {
            if(curNode.isEqualState(replacementNode)) {
                isNewNodePresent = true;
                break;
            }
        }

        assertTrue(isNewNodePresent);
    }

    private void verifyNewServerOffline(VoldemortServer replacementServer) {
        MetadataStore.VoldemortState state = replacementServer.getMetadataStore()
                                                                  .getServerStateUnlocked();
        assertEquals("State should be changed correctly to offline state",
                     MetadataStore.VoldemortState.OFFLINE_SERVER,
                     state);

    }

    private void verifyPostReplacement(VoldemortServer replacementServer) {
        verifyNewNodePartOfCluster(replacementServer.getIdentityNode());

        verifyNewServerOffline(replacementServer);
    }

    ClientTrafficGenerator trafficGenerator = null;
    private void verifyReplaceNode(ReplaceNodeCLI replacer, VoldemortServer replacementServer)
            throws Exception {
        try {
            if(trafficGenerator == null) {
                Node node = originalCluster.getNodeById(4);
                String clientBootStrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();

                List<String> storeNames = Arrays.asList(new String[] { STORE211_NAME, STORE322_NAME });
                final int numberOfThreads = 4;
                trafficGenerator = new ClientTrafficGenerator(clientBootStrapUrl,
                                                              storeNames,
                                                              numberOfThreads);
            }

            trafficGenerator.start();
            // warm up
            Thread.sleep(5000);

            replacer.execute();

            // cool down
            Thread.sleep(15000);
            trafficGenerator.stop();

            trafficGenerator.verifyIfClientsDetectedNewClusterXMLs();

            ServerTestUtils.waitForSlopDrain(finalServers, 30000L);
        } catch(InterruptedException e) {
            e.printStackTrace();
            throw e;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            trafficGenerator.verifyPostConditions();
        }

        verifyPostReplacement(replacementServer);
    }

}
