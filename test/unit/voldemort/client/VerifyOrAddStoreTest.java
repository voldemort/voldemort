package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
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
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.quota.QuotaType;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;


@RunWith(Parameterized.class)
public class VerifyOrAddStoreTest {

    private final static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private final static String readOnlyXmlFilePath = "test/common/voldemort/config/readonly-store.xml";
    
    private final static String PROCESS_NAME = "unit-test";

    private VoldemortServer[] servers;

    private Cluster cluster;

    private AdminClient adminClient;
    private final Properties serverProps;
    private final long defaultQuota;

    private StoreDefinition newStoreDef;
    private String newStoreName;
    private final ClientConfig clientConfig;
    private final ExecutorService service;
    private final int FAILED_NODE_ID;
    private static final int NUM_SERVERS = 4;

    @Parameters
    public static Collection<Object[]> configs() {
        List<Object[]> allConfigs = Lists.newArrayList();
        
        for (boolean fetchAllStores : new boolean[] { true, false }) {
            for (int threads : new int[] { 0, 2, 4, 6 }) {
                allConfigs.add(new Object[] { fetchAllStores, threads });
            }
        }
        return allConfigs;
    }

    public VerifyOrAddStoreTest(boolean fetchAllStores, int numberOfThreads) {
        clientConfig = new ClientConfig().setFetchAllStoresXmlInBootstrap(fetchAllStores);
        if (numberOfThreads == 0) {
            service = null;
        } else {
            service = Executors.newFixedThreadPool(numberOfThreads);
        }

        // Set default quota in between 100 and 1100 at random
        defaultQuota = 100 + new Random().nextInt(1000);

        FAILED_NODE_ID = new Random().nextInt(NUM_SERVERS);
        serverProps = new Properties();
        serverProps.setProperty(VoldemortConfig.DEFAULT_STORAGE_SPACE_QUOTA_IN_KB, Long.toString(defaultQuota));
    }

    @Before
    public void setUp() throws IOException {
        servers = new VoldemortServer[NUM_SERVERS];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10, 11 }, { 12, 13, 14, 15 } };
        cluster = ServerTestUtils.startVoldemortCluster(servers, partitionMap, serverProps, storesXmlfile);

        newStoreDef = new StoreDefinitionsMapper().readStoreList(new File(readOnlyXmlFilePath)).get(0);
        newStoreName = newStoreDef.getName();

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        clientConfig.setBootstrapUrls(bootstrapUrl);

        adminClient = new AdminClient(new AdminClientConfig(), clientConfig);
    }

    @After
    public void tearDown() throws Exception {
        adminClient.close();
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        if (service != null) {
            service.shutdown();
            service.awaitTermination(100, TimeUnit.MILLISECONDS);
        }
    }

    private StoreDefinition retrieveStoreOnNode(String storeName, int nodeId) {
        return adminClient.metadataMgmtOps.getStoreDefinition(nodeId, storeName);
    }

    private Long getQuotaForNode(String storeName, QuotaType quotaType, int nodeId) {
        Versioned<String> quotaStr = adminClient.quotaMgmtOps.getQuotaForNode(storeName, quotaType, nodeId);
        if (quotaStr == null) {
            return null;
        }
        return Long.parseLong(quotaStr.getValue());
    }

    private void verifyStoreAddedOnAllNodes() {

        // Get from a random node, verify
        StoreDefinition retrieved = adminClient.metadataMgmtOps.getStoreDefinition(newStoreName);
        assertNotNull("Created store can't be retrieved", retrieved);
        assertEquals("Store Created and retrieved are different ", newStoreDef, retrieved);

        // Get from one by one verify
        for (Integer nodeId : cluster.getNodeIds()) {
            retrieved = retrieveStoreOnNode(newStoreName, nodeId);
            assertNotNull("Created store can't be retrieved", retrieved);
            assertEquals("Store Created and retrieved are different ", newStoreDef, retrieved);

            Long quota = getQuotaForNode(newStoreName, QuotaType.STORAGE_SPACE, nodeId);
            assertEquals("Default quota mismatch", defaultQuota, quota.longValue());
        }
    }

    private void verifyStoreDoesNotExist() {
        StoreDefinition retrieved = adminClient.metadataMgmtOps.getStoreDefinition(newStoreName);
        assertNull("Store should not be created", retrieved);

        for (Integer nodeId : cluster.getNodeIds()) {
            retrieved = retrieveStoreOnNode(newStoreName, nodeId);
            assertNull("Store should not be created", retrieved);

            Long quota = getQuotaForNode(newStoreName, QuotaType.STORAGE_SPACE, nodeId);
            assertNull("Default quota should not exist", quota);
        }
    }

    @Test
    public void verifyNewStoreAddition() {
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, service);
        verifyStoreAddedOnAllNodes();

        // Verify set quota on the stores as well.
        for (QuotaType quotaType : new QuotaType[] { QuotaType.STORAGE_SPACE, QuotaType.GET_THROUGHPUT }) {
            long newQuota = 100000 + new Random().nextInt(10000);
            adminClient.quotaMgmtOps.setQuota(newStoreName, quotaType, newQuota, service);

            for (Integer nodeId : cluster.getNodeIds()) {
                Long retrievedQuota = getQuotaForNode(newStoreName, quotaType, nodeId);
                assertEquals("Set and retrieved different quota", newQuota, retrievedQuota.longValue());
            }
        }
    }

    @Test
    public void verifyNoStoreCreated() {
        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, false, service);
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
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, service);
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
        // Add the incompatible store definition to Node 0
        adminClient.storeMgmtOps.addStore(incompatibleDef, FAILED_NODE_ID);

        // Adding the store with different definition should fail.
        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, false, service);
            Assert.fail("Non existent store create with flag disabled should have thrown error");
        } catch (VoldemortException ex) {
            // Pass
        }

        for (Integer nodeId : cluster.getNodeIds()) {
            StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
            if (nodeId == FAILED_NODE_ID) {
                assertEquals("mismatched store def should be left intact", incompatibleDef, retrieved);
            } else {
                assertNull("Store should not exist in this node", retrieved);
            }
        }
    }

    @Test
    public void verifyCreateWithNodeFailure() throws Exception {
        ServerTestUtils.stopVoldemortServer(servers[FAILED_NODE_ID]);

        // Set CreateStore to false, node down should have thrown an error
        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, false, service);
            Assert.fail("Node failure should have raised an error");
        } catch(VoldemortException ex) {
            // Pass
        }

        // Verify that.
        for (Integer nodeId : cluster.getNodeIds()) {
            if (nodeId == FAILED_NODE_ID) {
                // this is a dead server, continue;
            } else {
                StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
                assertNull("store should not exist", retrieved);
            }
        }

        // Default is to set createStore to true, it should create stores on all 
        // nodes that are up and thrown an exception at the end.
        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, service);
            Assert.fail("Node failure should have raised an error");
        } catch (VoldemortException ex) {
            // Pass
        }

        // Verify that.
        for(Integer nodeId: cluster.getNodeIds()) {
            if (nodeId == FAILED_NODE_ID) {
                // this is a dead server, continue;
            } else {
                StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, nodeId);
                assertEquals("store successfully created on online nodes", newStoreDef, retrieved);
            }
        }

        // Verify set quota on the stores as well.
        for (QuotaType quotaType : new QuotaType[] { QuotaType.STORAGE_SPACE, QuotaType.GET_THROUGHPUT }) {
            long newQuota = 100000 + new Random().nextInt(10000);
            try {
                adminClient.quotaMgmtOps.setQuota(newStoreName, quotaType, newQuota, service);
                Assert.fail("Node failure should have raised an error");
            } catch (VoldemortException ex) {
                // expected.
            }

            for (Integer nodeId : cluster.getNodeIds()) {
                if (nodeId == FAILED_NODE_ID) {
                    // this is dead server, continue.
                } else {
                    Long retrievedQuota = getQuotaForNode(newStoreName, quotaType, nodeId);
                    assertEquals("Set and retrieved different quota", newQuota, retrievedQuota.longValue());
                }
            }
        }
    }

    @Test
    public void verifyAdminClientAcrossNodeRestart() throws Exception {
        long newQuota = 500000 + new Random().nextInt(10000);

        // Do a query create the connections.
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, service);

        // Create client pool as well.
        for (QuotaType quotaType : new QuotaType[] { QuotaType.STORAGE_SPACE, QuotaType.GET_THROUGHPUT }) {
            adminClient.quotaMgmtOps.setQuota(newStoreName, quotaType, newQuota, service);
        }
                
        ServerTestUtils.stopVoldemortServer(servers[FAILED_NODE_ID]);

        try {
            retrieveStoreOnNode(newStoreName, FAILED_NODE_ID);
            Assert.fail("Downed node retrieval should have thrown an error");
        } catch (VoldemortException ex) {
            // Expected
        }

        try {
            adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, service);
            Assert.fail("Downed node should have thrown an error");
        } catch (VoldemortException ex) {
            // Expected
        }

        // setQuota should throw as well.
        for (QuotaType quotaType : new QuotaType[] { QuotaType.STORAGE_SPACE, QuotaType.GET_THROUGHPUT }) {
            try {
                adminClient.quotaMgmtOps.setQuota(newStoreName, quotaType, newQuota, service);
                Assert.fail("Node failure should have raised an error");
            } catch (VoldemortException ex) {
                // expected.
            }
        }

        servers[FAILED_NODE_ID] =
                ServerTestUtils.restartServer(servers[FAILED_NODE_ID], FAILED_NODE_ID, cluster, serverProps);
        StoreDefinition retrieved = retrieveStoreOnNode(newStoreName, FAILED_NODE_ID);
        assertEquals("After restart, should retrieve the store", newStoreDef, retrieved);
        adminClient.storeMgmtOps.verifyOrAddStore(newStoreDef, PROCESS_NAME, service);

        long updatedQuota = newQuota + 100;
        for (QuotaType quotaType : new QuotaType[] { QuotaType.STORAGE_SPACE, QuotaType.GET_THROUGHPUT }) {
            adminClient.quotaMgmtOps.setQuota(newStoreName, quotaType, updatedQuota, service);

            for (Integer nodeId : cluster.getNodeIds()) {
                Long retrievedQuota = getQuotaForNode(newStoreName, quotaType, nodeId);
                assertEquals("Set and retrieved different quota", updatedQuota, retrievedQuota.longValue());
            }
        }

    }
}
