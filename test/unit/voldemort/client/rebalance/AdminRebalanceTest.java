package voldemort.client.rebalance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class AdminRebalanceTest extends TestCase {

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private final int TEST_SIZE = 1000;
    private StoreDefinition storeDef1;
    private StoreDefinition storeDef2;
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

    public void startUp1() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                {} });

        servers = new VoldemortServer[3];
        storeDef1 = ServerTestUtils.getStoreDef("test",
                                                1,
                                                1,
                                                1,
                                                1,
                                                1,
                                                RoutingStrategyType.CONSISTENT_STRATEGY);
        storeDef2 = ServerTestUtils.getStoreDef("test2",
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
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef1,
                                                                                                   storeDef2)));
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

    public void startUp2() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                { 8, 9, 10, 11 }, {} });

        servers = new VoldemortServer[4];
        storeDef1 = ServerTestUtils.getStoreDef("test",
                                                2,
                                                1,
                                                1,
                                                1,
                                                1,
                                                RoutingStrategyType.CONSISTENT_STRATEGY);
        storeDef2 = ServerTestUtils.getStoreDef("test2",
                                                3,
                                                1,
                                                1,
                                                1,
                                                1,
                                                RoutingStrategyType.CONSISTENT_STRATEGY);
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster,
                                                            cluster.getNodeById(3),
                                                            cluster.getNodeById(0),
                                                            Lists.newArrayList(0));
        File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
        FileUtils.writeStringToFile(tempStoreXml,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef1,
                                                                                                   storeDef2)));
        for(int nodeId = 0; nodeId < 4; nodeId++) {
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

    public void startUp3() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(4, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                { 8, 9, 10, 11 }, {} });

        servers = new VoldemortServer[4];
        storeDef1 = new StoreDefinitionBuilder().setName("test")
                                                .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                .setKeySerializer(new SerializerDefinition("string"))
                                                .setValueSerializer(new SerializerDefinition("string"))
                                                .setRoutingPolicy(RoutingTier.SERVER)
                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                .setReplicationFactor(2)
                                                .setPreferredReads(1)
                                                .setRequiredReads(1)
                                                .setPreferredWrites(1)
                                                .setRequiredWrites(1)
                                                .build();
        storeDef2 = new StoreDefinitionBuilder().setName("test2")
                                                .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                .setKeySerializer(new SerializerDefinition("string"))
                                                .setValueSerializer(new SerializerDefinition("string"))
                                                .setRoutingPolicy(RoutingTier.SERVER)
                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                .setReplicationFactor(3)
                                                .setPreferredReads(1)
                                                .setRequiredReads(1)
                                                .setPreferredWrites(1)
                                                .setRequiredWrites(1)
                                                .build();
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster,
                                                            cluster.getNodeById(3),
                                                            cluster.getNodeById(0),
                                                            Lists.newArrayList(0));
        File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
        FileUtils.writeStringToFile(tempStoreXml,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef1,
                                                                                                   storeDef2)));
        for(int nodeId = 0; nodeId < 4; nodeId++) {
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

    public void shutDown() throws IOException {
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
    public void testRebalanceNodeRW() throws IOException {

        try {
            startUp1();

            // Start another node for only this unit test
            HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_SIZE);

            SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(Lists.newArrayList("tcp://"
                                                                                                                                   + cluster.getNodeById(0)
                                                                                                                                            .getHost()
                                                                                                                                   + ":"
                                                                                                                                   + cluster.getNodeById(0)
                                                                                                                                            .getSocketPort())));
            StoreClient<Object, Object> storeClient1 = factory.getStoreClient("test"), storeClient2 = factory.getStoreClient("test2");

            List<Integer> primaryPartitionsMoved = Lists.newArrayList(0);
            List<Integer> secondaryPartitionsMoved = Lists.newArrayList(4, 5, 6, 7);

            HashMap<ByteArray, byte[]> primaryEntriesMoved = Maps.newHashMap();
            HashMap<ByteArray, byte[]> secondaryEntriesMoved = Maps.newHashMap();

            RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef2,
                                                                                          cluster);
            for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
                storeClient1.put(new String(entry.getKey().get()), new String(entry.getValue()));
                storeClient2.put(new String(entry.getKey().get()), new String(entry.getValue()));
                List<Integer> pList = strategy.getPartitionList(entry.getKey().get());
                if(primaryPartitionsMoved.contains(pList.get(0))) {
                    primaryEntriesMoved.put(entry.getKey(), entry.getValue());
                } else if(secondaryPartitionsMoved.contains(pList.get(0))) {
                    secondaryEntriesMoved.put(entry.getKey(), entry.getValue());
                }
            }

            RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                                 targetCluster,
                                                                 Lists.newArrayList(storeDef1,
                                                                                    storeDef2),
                                                                 true);
            List<RebalancePartitionsInfo> plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

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

            Store<ByteArray, byte[], byte[]> storeTest0 = getStore(0, "test2");
            Store<ByteArray, byte[], byte[]> storeTest1 = getStore(1, "test2");
            Store<ByteArray, byte[], byte[]> storeTest2 = getStore(2, "test2");

            Store<ByteArray, byte[], byte[]> storeTest10 = getStore(0, "test");

            // Primary is on Node 0 and not on Node 1
            for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {
                assertSame("entry should be present at store", 1, storeTest0.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest0.get(entry.getKey(), null).get(0).getValue()));
                assertEquals(storeTest1.get(entry.getKey(), null).size(), 0);

                // Check in other store
                assertSame("entry should be present in store test2 ",
                           1,
                           storeTest10.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest10.get(entry.getKey(), null).get(0).getValue()));
            }

            // Secondary is on Node 2 and not on Node 0
            for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {
                assertSame("entry should be present at store", 1, storeTest2.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest2.get(entry.getKey(), null).get(0).getValue()));
                assertEquals(storeTest0.get(entry.getKey(), null).size(), 0);
            }

            // All servers should be back to normal state
            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
            }
        } finally {
            shutDown();
        }
    }

    @Test
    public void testRebalanceNodeRW2() throws IOException {

        try {
            startUp2();

            // Start another node for only this unit test
            HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_SIZE);

            SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(Lists.newArrayList("tcp://"
                                                                                                                                   + cluster.getNodeById(0)
                                                                                                                                            .getHost()
                                                                                                                                   + ":"
                                                                                                                                   + cluster.getNodeById(0)
                                                                                                                                            .getSocketPort())));
            StoreClient<Object, Object> storeClient1 = factory.getStoreClient("test"), storeClient2 = factory.getStoreClient("test2");

            List<Integer> primaryPartitionsMoved = Lists.newArrayList(0);
            List<Integer> secondaryPartitionsMoved = Lists.newArrayList(8, 9, 10, 11);
            List<Integer> tertiaryPartitionsMoved = Lists.newArrayList(4, 5, 6, 7);

            HashMap<ByteArray, byte[]> primaryEntriesMoved = Maps.newHashMap();
            HashMap<ByteArray, byte[]> secondaryEntriesMoved = Maps.newHashMap();
            HashMap<ByteArray, byte[]> tertiaryEntriesMoved = Maps.newHashMap();

            RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef2,
                                                                                          cluster);
            for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
                storeClient1.put(new String(entry.getKey().get()), new String(entry.getValue()));
                storeClient2.put(new String(entry.getKey().get()), new String(entry.getValue()));
                List<Integer> pList = strategy.getPartitionList(entry.getKey().get());
                if(primaryPartitionsMoved.contains(pList.get(0))) {
                    primaryEntriesMoved.put(entry.getKey(), entry.getValue());
                } else if(secondaryPartitionsMoved.contains(pList.get(0))) {
                    secondaryEntriesMoved.put(entry.getKey(), entry.getValue());
                } else if(tertiaryPartitionsMoved.contains(pList.get(0))) {
                    tertiaryEntriesMoved.put(entry.getKey(), entry.getValue());
                }
            }

            RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                                 targetCluster,
                                                                 Lists.newArrayList(storeDef1,
                                                                                    storeDef2),
                                                                 true);
            List<RebalancePartitionsInfo> plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

            // Set into rebalancing state
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.SERVER_STATE_KEY,
                                                            MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
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

            Store<ByteArray, byte[], byte[]> storeTest0 = getStore(0, "test2");
            Store<ByteArray, byte[], byte[]> storeTest1 = getStore(1, "test2");
            Store<ByteArray, byte[], byte[]> storeTest2 = getStore(2, "test2");
            Store<ByteArray, byte[], byte[]> storeTest3 = getStore(3, "test2");

            Store<ByteArray, byte[], byte[]> storeTest10 = getStore(0, "test");
            Store<ByteArray, byte[], byte[]> storeTest30 = getStore(3, "test");

            // Primary
            for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {

                // Test 2
                // Present on Node 0
                assertSame("entry should be present at store", 1, storeTest0.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest0.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 1
                assertSame("entry should be present at store", 1, storeTest1.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest1.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 3
                assertSame("entry should be present at store", 1, storeTest3.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest3.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 2
                assertEquals(storeTest2.get(entry.getKey(), null).size(), 0);

                // Test
                // Present on Node 0
                assertSame("entry should be present at store", 1, storeTest10.get(entry.getKey(),
                                                                                  null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest10.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 3
                assertSame("entry should be present at store", 1, storeTest30.get(entry.getKey(),
                                                                                  null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest30.get(entry.getKey(), null).get(0).getValue()));

            }

            // Secondary
            for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {

                // Test 2
                // Present on Node 0
                assertSame("entry should be present at store", 1, storeTest0.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest0.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 3
                assertSame("entry should be present at store", 1, storeTest3.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest3.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 1
                assertEquals(storeTest1.get(entry.getKey(), null).size(), 0);
            }

            // Tertiary
            for(Entry<ByteArray, byte[]> entry: tertiaryEntriesMoved.entrySet()) {

                // Test 2
                // Present on Node 3
                assertSame("entry should be present at store", 1, storeTest3.get(entry.getKey(),
                                                                                 null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest3.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 0
                assertEquals(storeTest0.get(entry.getKey(), null).size(), 0);
            }

            // All servers should be back to normal state
            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
            }
        } finally {
            shutDown();
        }
    }

    @Test
    public void testRebalanceNodeRO() throws IOException {
        try {
            startUp3();

            int numChunks = 5;
            for(StoreDefinition storeDef: Lists.newArrayList(storeDef1, storeDef2)) {
                buildStore(storeDef, numChunks);
            }

            RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                                 targetCluster,
                                                                 Lists.newArrayList(storeDef1,
                                                                                    storeDef2),
                                                                 true);
            List<RebalancePartitionsInfo> plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

            // Set into rebalancing state
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.SERVER_STATE_KEY,
                                                            MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
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

            // Check if files have been copied
            for(StoreDefinition storeDef: Lists.newArrayList(storeDef1, storeDef2)) {

                String storeName = storeDef.getName();
                for(RebalancePartitionsInfo currentPlan: plans) {

                    File currentDir = new File(((ReadOnlyStorageEngine) getStore(currentPlan.getStealerId(),
                                                                                 storeName)).getCurrentDirPath());
                    for(Entry<Integer, List<Integer>> entry: currentPlan.getReplicaToPartitionList()
                                                                        .entrySet()) {
                        if(entry.getKey() < storeDef.getReplicationFactor()) {
                            for(int partitionId: entry.getValue()) {
                                for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                                    assertTrue(new File(currentDir, partitionId + "_"
                                                                    + entry.getKey() + "_"
                                                                    + chunkId + ".data").exists());
                                    assertTrue(new File(currentDir, partitionId + "_"
                                                                    + entry.getKey() + "_"
                                                                    + chunkId + ".index").exists());
                                }
                            }
                        }

                    }

                }
            }

            // All servers should be back to normal state
            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
            }
        } finally {
            shutDown();
        }
    }

    @Test
    // TODO:
    public void testRebalanceChangeState() {

    }

    private void buildStore(StoreDefinition storeDef, int numChunks) throws IOException {
        Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions = RebalanceUtils.getNodeIdToAllPartitions(cluster,
                                                                                                                  Lists.newArrayList(storeDef),
                                                                                                                  true);
        for(Entry<Integer, Set<Pair<Integer, Integer>>> entry: nodeIdToAllPartitions.entrySet()) {
            HashMap<Integer, List<Integer>> tuples = RebalanceUtils.flattenPartitionTuples(entry.getValue());

            File tempDir = new File(((ReadOnlyStorageEngine) getStore(entry.getKey(),
                                                                      storeDef.getName())).getStoreDirPath(),
                                    "version-1");
            Utils.mkdirs(tempDir);
            generateROFiles(numChunks, 1200, 1000, tuples, tempDir);

            // Build for store one
            adminClient.swapStore(entry.getKey(), storeDef.getName(), tempDir.getAbsolutePath());
        }
    }

    private void generateROFiles(int numChunks,
                                 long indexSize,
                                 long dataSize,
                                 HashMap<Integer, List<Integer>> buckets,
                                 File versionDir) throws IOException {

        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());

        File metadataFile = new File(versionDir, ".metadata");
        BufferedWriter writer = new BufferedWriter(new FileWriter(metadataFile));
        writer.write(metadata.toJsonString());
        writer.close();

        for(Entry<Integer, List<Integer>> entry: buckets.entrySet()) {
            int replicaType = entry.getKey();
            for(int partitionId: entry.getValue()) {
                for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                    File index = new File(versionDir, Integer.toString(partitionId) + "_"
                                                      + Integer.toString(replicaType) + "_"
                                                      + Integer.toString(chunkId) + ".index");
                    File data = new File(versionDir, Integer.toString(partitionId) + "_"
                                                     + Integer.toString(replicaType) + "_"
                                                     + Integer.toString(chunkId) + ".data");
                    // write some random crap for index and data
                    FileOutputStream dataOs = new FileOutputStream(data);
                    for(int i = 0; i < dataSize; i++)
                        dataOs.write(i);
                    dataOs.close();
                    FileOutputStream indexOs = new FileOutputStream(index);
                    for(int i = 0; i < indexSize; i++)
                        indexOs.write(i);
                    indexOs.close();
                }
            }
        }
    }
}
