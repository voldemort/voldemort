package voldemort.client.rebalance;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.server.VoldemortServer;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
@RunWith(Parameterized.class)
public class AdminRebalanceTest extends TestCase {

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private StoreDefinition storeDef;
    private VoldemortServer[] servers;
    private Cluster cluster;
    private Cluster targetCluster;
    private AdminClient adminClient;

    private final boolean useNio;

    public AdminRebalanceTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                {} });

        servers = new VoldemortServer[3];
        storeDef = ServerTestUtils.getStoreDef("test",
                                               2,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster,
                                                            cluster.getNodeById(2),
                                                            cluster.getNodeById(0),
                                                            Lists.newArrayList(0));
        File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
        FileUtils.writeStringToFile(tempStoreXml,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef)));
        for(int nodeId = 0; nodeId < 3; nodeId++) {
            servers[nodeId] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                   ServerTestUtils.createServerConfig(useNio,
                                                                                                      nodeId,
                                                                                                      TestUtils.createTempDir()
                                                                                                               .getAbsolutePath(),
                                                                                                      null,
                                                                                                      tempStoreXml.getAbsolutePath(),
                                                                                                      new Properties()),
                                                                   cluster);
        }

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    /**
     * Returns the corresponding server based on the node id
     * 
     * @param nodeId The node id for which we're retrieving the server
     * @return Voldemort server
     */
    private VoldemortServer getServer(int nodeId) {
        return servers[nodeId];
    }

    @Override
    @After
    public void tearDown() throws IOException, InterruptedException {
        if(adminClient != null)
            adminClient.stop();
        for(VoldemortServer server: servers) {
            if(server != null)
                ServerTestUtils.stopVoldemortServer(server);
        }
        socketStoreFactory.close();
    }

    private VoldemortServer getVoldemortServer(int nodeId) {
        return servers[nodeId];
    }

    private AdminClient getAdminClient() {
        return adminClient;
    }

    private Store<ByteArray, byte[], byte[]> getStore(int nodeID, String storeName) {
        Store<ByteArray, byte[], byte[]> store = getVoldemortServer(nodeID).getStoreRepository()
                                                                           .getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);
        return store;
    }

    @Test
    public void testRebalanceNodeRWStore() {

        // Start another node for only this unit test
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(10);

        SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(Lists.newArrayList("tcp://"
                                                                                                                               + cluster.getNodeById(0)
                                                                                                                                        .getHost()
                                                                                                                               + ":"
                                                                                                                               + cluster.getNodeById(0)
                                                                                                                                        .getSocketPort())));
        StoreClient<Object, Object> storeClient = factory.getStoreClient("test");

        List<Integer> primaryPartitionsMoved = Lists.newArrayList(0);
        List<Integer> secondaryPartitionsMoved = Lists.newArrayList(4, 5, 6, 7);

        HashMap<ByteArray, byte[]> primaryEntriesMoved = Maps.newHashMap();
        HashMap<ByteArray, byte[]> secondaryEntriesMoved = Maps.newHashMap();

        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            storeClient.put(new String(entry.getKey().get()), new String(entry.getValue()));
            List<Integer> pList = strategy.getPartitionList(entry.getKey().get());
            System.out.print("ENTRY - " + ByteUtils.toHexString(entry.getKey().get()) + " - "
                             + pList);
            if(primaryPartitionsMoved.contains(pList.get(0))) {
                primaryEntriesMoved.put(entry.getKey(), entry.getValue());
                System.out.print(" - primary");
            } else if(secondaryPartitionsMoved.contains(pList.get(0))) {
                secondaryEntriesMoved.put(entry.getKey(), entry.getValue());
                System.out.print(" - secondary");
            }
            System.out.println();
        }

        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef),
                                                             true);
        List<RebalancePartitionsInfo> plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

        System.out.println("PLANS - " + plans);
        try {
            adminClient.rebalanceNode(plans.get(0));
            fail("Should have thrown an exception since not in rebalancing state");
        } catch(VoldemortException e) {
            e.printStackTrace();
        }

        // Set into rebalancing state
        for(RebalancePartitionsInfo partitionPlan: plans) {
            getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                   .put(MetadataStore.SERVER_STATE_KEY,
                                                        MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        }

        try {
            adminClient.rebalanceNode(plans.get(0));
            fail("Should have thrown an exception since no steal info");
        } catch(VoldemortException e) {
            e.printStackTrace();
        }

        // Set the rebalance info on the stealer node
        for(RebalancePartitionsInfo partitionPlan: plans) {
            getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                   .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                        new RebalancerState(Lists.newArrayList(partitionPlan)));
        }

        // Update the cluster metadata on all three nodes
        for(VoldemortServer server: servers) {
            server.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        }

        // Actually run it
        try {
            for(RebalancePartitionsInfo currentPlan: plans) {
                int asyncId = adminClient.rebalanceNode(currentPlan);
                assertNotSame("Got a valid rebalanceAsyncId", -1, asyncId);
                getAdminClient().waitForCompletion(currentPlan.getStealerId(),
                                                   asyncId,
                                                   300,
                                                   TimeUnit.SECONDS);

                // Test that plan has been removed from the list
                assertFalse(getServer(currentPlan.getStealerId()).getMetadataStore()
                                                                 .getRebalancerState()
                                                                 .getAll()
                                                                 .contains(currentPlan));

            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Should not throw any exceptions");
        }

        Store<ByteArray, byte[], byte[]> store0 = getStore(0, "test");
        Store<ByteArray, byte[], byte[]> store1 = getStore(1, "test");
        Store<ByteArray, byte[], byte[]> store2 = getStore(2, "test");

        // Primary is on Node 0 and not on Node 1
        for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {
            assertSame("entry should be present at store", 1, store0.get(entry.getKey(), null)
                                                                    .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store0.get(entry.getKey(), null).get(0).getValue()));
            assertEquals(store1.get(entry.getKey(), null).size(), 0);
        }

        // Secondary is on Node 2 and not on Node 0
        for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {
            assertSame("entry should be present at store", 1, store2.get(entry.getKey(), null)
                                                                    .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store2.get(entry.getKey(), null).get(0).getValue()));
            assertEquals(store0.get(entry.getKey(), null).size(), 0);
        }

        // All servers should be back to normal state

        for(VoldemortServer server: servers) {
            assertEquals(server.getMetadataStore().getServerState(),
                         MetadataStore.VoldemortState.NORMAL_SERVER);
        }
    }

    @Test
    public void testRebalanceNodeROStore() {

        // Start another node for only this unit test
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(10);

        SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(Lists.newArrayList("tcp://"
                                                                                                                               + cluster.getNodeById(0)
                                                                                                                                        .getHost()
                                                                                                                               + ":"
                                                                                                                               + cluster.getNodeById(0)
                                                                                                                                        .getSocketPort())));
        StoreClient<Object, Object> storeClient = factory.getStoreClient("test");

        List<Integer> primaryPartitionsMoved = Lists.newArrayList(0);
        List<Integer> secondaryPartitionsMoved = Lists.newArrayList(4, 5, 6, 7);

        HashMap<ByteArray, byte[]> primaryEntriesMoved = Maps.newHashMap();
        HashMap<ByteArray, byte[]> secondaryEntriesMoved = Maps.newHashMap();

        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            storeClient.put(new String(entry.getKey().get()), new String(entry.getValue()));
            List<Integer> pList = strategy.getPartitionList(entry.getKey().get());
            System.out.print("ENTRY - " + ByteUtils.toHexString(entry.getKey().get()) + " - "
                             + pList);
            if(primaryPartitionsMoved.contains(pList.get(0))) {
                primaryEntriesMoved.put(entry.getKey(), entry.getValue());
                System.out.print(" - primary");
            } else if(secondaryPartitionsMoved.contains(pList.get(0))) {
                secondaryEntriesMoved.put(entry.getKey(), entry.getValue());
                System.out.print(" - secondary");
            }
            System.out.println();
        }

        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef),
                                                             true);
        List<RebalancePartitionsInfo> plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

        System.out.println("PLANS - " + plans);
        try {
            adminClient.rebalanceNode(plans.get(0));
            fail("Should have thrown an exception since not in rebalancing state");
        } catch(VoldemortException e) {
            e.printStackTrace();
        }

        // Set into rebalancing state
        for(RebalancePartitionsInfo partitionPlan: plans) {
            getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                   .put(MetadataStore.SERVER_STATE_KEY,
                                                        MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        }

        try {
            adminClient.rebalanceNode(plans.get(0));
            fail("Should have thrown an exception since no steal info");
        } catch(VoldemortException e) {
            e.printStackTrace();
        }

        // Set the rebalance info on the stealer node
        for(RebalancePartitionsInfo partitionPlan: plans) {
            getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                   .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                        new RebalancerState(Lists.newArrayList(partitionPlan)));
        }

        // Update the cluster metadata on all three nodes
        for(VoldemortServer server: servers) {
            server.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        }

        // Actually run it
        try {
            for(RebalancePartitionsInfo currentPlan: plans) {
                int asyncId = adminClient.rebalanceNode(currentPlan);
                assertNotSame("Got a valid rebalanceAsyncId", -1, asyncId);
                getAdminClient().waitForCompletion(currentPlan.getStealerId(),
                                                   asyncId,
                                                   300,
                                                   TimeUnit.SECONDS);
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Should not throw any exceptions");
        }

        Store<ByteArray, byte[], byte[]> store0 = getStore(0, "test");
        Store<ByteArray, byte[], byte[]> store1 = getStore(1, "test");
        Store<ByteArray, byte[], byte[]> store2 = getStore(2, "test");

        // Primary is on Node 0 and not on Node 1
        for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {
            assertSame("entry should be present at store", 1, store0.get(entry.getKey(), null)
                                                                    .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store0.get(entry.getKey(), null).get(0).getValue()));
            assertEquals(store1.get(entry.getKey(), null).size(), 0);
        }

        // Secondary is on Node 2 and not on Node 0
        for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {
            assertSame("entry should be present at store", 1, store2.get(entry.getKey(), null)
                                                                    .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store2.get(entry.getKey(), null).get(0).getValue()));
            assertEquals(store0.get(entry.getKey(), null).size(), 0);
        }
    }
}
