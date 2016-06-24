package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.xml.StoreDefinitionsMapper;


@RunWith(Parameterized.class)
public class VerifyOrAddStoreTest {

    private final static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private final static String readOnlyXmlFilePath = "test/common/voldemort/config/readonly-store.xml";
    
    private final static String PROCESS_NAME = "unit-test";

    private VoldemortServer[] servers;

    private Cluster cluster;

    private AdminClient adminClient;

    private StoreDefinition newStoreDef;
    private String newStoreName;
    private final ClientConfig clientConfig;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    public VerifyOrAddStoreTest(boolean fetchAllStores) {
        clientConfig = new ClientConfig().setFetchAllStoresXmlInBootstrap(fetchAllStores);
    }

    @Before
    public void setUp() throws IOException {
        int numServers = 4;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10, 11 }, { 12, 13, 14, 15 } };
        cluster = ServerTestUtils.startVoldemortCluster(servers, partitionMap, new Properties(), storesXmlfile);

        newStoreDef = new StoreDefinitionsMapper().readStoreList(new File(readOnlyXmlFilePath)).get(0);
        newStoreName = newStoreDef.getName();

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        clientConfig.setBootstrapUrls(bootstrapUrl);

        adminClient = new AdminClient(new AdminClientConfig(), clientConfig);
    }

    @After
    public void tearDown() throws IOException {
        adminClient.close();
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    private StoreDefinition retrieveStoreOnNode(String storeName, int nodeId) {
        List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId).getValue();
        StoreDefinition retrieved = null;
        for (StoreDefinition storeDef : storeDefs) {
            if (storeDef.getName().equals(newStoreName)) {
                retrieved = storeDef;
                break;
            }
        }
        return retrieved;
    }

    private void verifyStoreAddedOnAllNodes() {
        for (Integer nodeId : cluster.getNodeIds()) {
            StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
            assertNotNull("Created store can't be retrieved", retrieved);
            assertEquals("Store Created and retrieved are different ", newStoreDef, retrieved);
        }
    }

    private void verifyStoreDoesNotExist() {
        for (Integer nodeId : cluster.getNodeIds()) {
            StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
            assertNull("Store should not be created", retrieved);
        }
    }

    @Test
    public void verifyNewStoreAddition() {
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME);
        verifyStoreAddedOnAllNodes();
    }

    @Test
    public void verifyNoStoreCreated() {
        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, false);
            Assert.fail("Non existent store create with flag disabled should have thrown error");
        } catch (VoldemortException ex) {
            // Pass
        }
        verifyStoreDoesNotExist();
    }

    @Test
    public void verifyPartialCreatedStores() {
        // Add the store on one node with same definition.
        adminClient.storeMgmtOps.addStore(newStoreDef, 0);
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME);
        verifyStoreAddedOnAllNodes();
    }
    
    private StoreDefinition getIncompatibleStoreDef() {
        // Create store with same definition but modify the type.
        return new StoreDefinition(newStoreDef.getName(),
                InMemoryStorageConfiguration.TYPE_NAME,
                newStoreDef.getDescription(),
                newStoreDef.getKeySerializer(), 
                newStoreDef.getValueSerializer(),
                newStoreDef.getTransformsSerializer(),
                newStoreDef.getRoutingPolicy(),
                newStoreDef.getRoutingStrategyType(),
                newStoreDef.getReplicationFactor(),
                newStoreDef.getPreferredReads(),
                newStoreDef.getRequiredReads(),
                newStoreDef.getPreferredWrites(),
                newStoreDef.getRequiredWrites(),
                newStoreDef.getViewTargetStoreName(),
                newStoreDef.getValueTransformation(),
                newStoreDef.getZoneReplicationFactor(),
                newStoreDef.getZoneCountReads(),
                newStoreDef.getZoneCountWrites(),
                newStoreDef.getRetentionDays(),
                newStoreDef.getRetentionScanThrottleRate(),
                newStoreDef.getRetentionFrequencyDays(),
                newStoreDef.getSerializerFactory(),
                newStoreDef.getHintedHandoffStrategyType(),
                newStoreDef.getHintPrefListSize(),
                newStoreDef.getOwners(),
                newStoreDef.getMemoryFootprintMB());
    }

    @Test
    public void verifyMisMatchFails() throws Exception {
        StoreDefinition incompatibleDef = getIncompatibleStoreDef();
        final int NODE_ID = 0;
        // Add the incompatible store definition to Node 0
        adminClient.storeMgmtOps.addStore(incompatibleDef, NODE_ID);

        // Adding the store with different definition should fail.
        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, false);
            Assert.fail("Non existent store create with flag disabled should have thrown error");
        } catch (VoldemortException ex) {
            // Pass
        }

        for (Integer nodeId : cluster.getNodeIds()) {
            StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
            if (nodeId == NODE_ID) {
                assertEquals("mismatched store def should be left intact", incompatibleDef, retrieved);
            } else {
                assertNull("Store should not exist in this node", retrieved);
            }
        }
    }

    @Test
    public void verifyCreateWithNodeFailure() throws Exception {
        final int NODE_ID = servers.length - 1;
        ServerTestUtils.stopVoldemortServer(servers[NODE_ID]);

        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME);
            Assert.fail("Non existent store create with flag disabled should have thrown error");
        } catch(VoldemortException ex) {
            // Pass
        }

        for(Integer nodeId: cluster.getNodeIds()) {
            if(nodeId == NODE_ID) {
                // this is a dead server, continue;
            } else {
                StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
                assertEquals("store successfully created on online nodes", newStoreDef, retrieved);
            }
        }
    }

    @Test
    public void verifyAdminClientAcrossNodeRestart() throws Exception {
        // Do a query create the connections.
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME);

        final int NODE_ID = servers.length - 1;
        ServerTestUtils.stopVoldemortServer(servers[NODE_ID]);

        try {
            retrieveStoreOnNode(newStoreName, NODE_ID);
            Assert.fail("Downed node retrieval should have thrown an error");
        } catch (VoldemortException ex) {
            // Expected
        }

        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME);
            Assert.fail("Downed node should have thrown an error");
        } catch (VoldemortException ex) {
            // Expected
        }

        servers[NODE_ID] = ServerTestUtils.restartServer(servers[NODE_ID], NODE_ID, cluster);
        StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, NODE_ID);
        assertEquals("After restart, should retrieve the store", newStoreDef, retrieved);
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME);
    }
}
