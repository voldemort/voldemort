package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.server.rebalance.RebalancerState;
import voldemort.server.rebalance.VoldemortRebalancingException;
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
public class AdminRebalanceTest {

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private final int TEST_SIZE = 1000;
    private StoreDefinition storeDef1;
    private StoreDefinition storeDef2;
    private StoreDefinition storeDef3;
    private StoreDefinition storeDef4;
    private VoldemortServer[] servers;
    private Cluster cluster;
    private Cluster targetCluster;
    private AdminClient adminClient;
    private List<RebalancePartitionsInfo> plans;

    private final boolean useNio;

    public AdminRebalanceTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    public void startThreeNodeRW() throws IOException {
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
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster, 2, Lists.newArrayList(0));
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

        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef1,
                                                                                storeDef2), true);
        plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    public void startFourNodeRW() throws IOException {
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
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster, 3, Lists.newArrayList(0));
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

        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef1,
                                                                                storeDef2), true);
        plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    public void startFourNodeRO() throws IOException {
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
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster, 3, Lists.newArrayList(0));
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

        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef1,
                                                                                storeDef2), true);
        plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    public void startFourNodeRORW() throws IOException {
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
        storeDef3 = ServerTestUtils.getStoreDef("test3",
                                                2,
                                                1,
                                                1,
                                                1,
                                                1,
                                                RoutingStrategyType.CONSISTENT_STRATEGY);
        storeDef4 = ServerTestUtils.getStoreDef("test4",
                                                3,
                                                1,
                                                1,
                                                1,
                                                1,
                                                RoutingStrategyType.CONSISTENT_STRATEGY);

        targetCluster = RebalanceUtils.createUpdatedCluster(cluster, 3, Lists.newArrayList(0));
        File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
        FileUtils.writeStringToFile(tempStoreXml,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef1,
                                                                                                   storeDef2,
                                                                                                   storeDef3,
                                                                                                   storeDef4)));
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

        // Make plan only with RO stores
        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef1,
                                                                                storeDef2), true);
        plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

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

    @Test(timeout = 60000)
    public void testRebalanceNodeRW() throws IOException {

        try {
            startThreeNodeRW();

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

            try {
                adminClient.rebalanceNode(plans.get(0));
                fail("Should have thrown an exception since not in rebalancing state");
            } catch(VoldemortException e) {}

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

            }

            // Put a plan different from the plan that we actually want to
            // execute
            getServer(plans.get(0).getStealerId()).getMetadataStore()
                                                  .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                       new RebalancerState(Lists.newArrayList(new RebalancePartitionsInfo(100,
                                                                                                                          plans.get(0)
                                                                                                                               .getDonorId(),
                                                                                                                          plans.get(0)
                                                                                                                               .getStoreToReplicaToAddPartitionList(),
                                                                                                                          plans.get(0)
                                                                                                                               .getStoreToReplicaToDeletePartitionList(),
                                                                                                                          plans.get(0)
                                                                                                                               .getInitialCluster(),
                                                                                                                          0))));

            try {
                adminClient.rebalanceNode(plans.get(0));
                fail("Should have thrown an exception since the two plans eventhough have the same donor are different");
            } catch(VoldemortException e) {

            }

            // Set the rebalance info on the stealer node
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                            new RebalancerState(Lists.newArrayList(RebalancePartitionsInfo.create(partitionPlan.toJsonString()))));
            }

            // Update the cluster metadata on all three nodes
            for(VoldemortServer server: servers) {
                server.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
            }

            // Actually run it
            try {
                for(RebalancePartitionsInfo currentPlan: plans) {
                    int asyncId = adminClient.rebalanceNode(currentPlan);

                    // Try submitting the same job again, should throw
                    // AlreadyRebalancingException
                    try {
                        adminClient.rebalanceNode(currentPlan);
                        fail("Should have thrown an exception since it is already rebalancing");
                    } catch(AlreadyRebalancingException e) {}

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

            Store<ByteArray, byte[], byte[]> storeTest00 = getStore(0, "test");
            Store<ByteArray, byte[], byte[]> storeTest20 = getStore(2, "test");

            // Primary is on Node 0 and not on Node 1
            for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {
                assertSame("entry should be present at store",
                           1,
                           storeTest0.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest0.get(entry.getKey(), null).get(0).getValue()));
                assertEquals(storeTest1.get(entry.getKey(), null).size(), 0);

                // Check in other store
                assertSame("entry should be present in store test2 ",
                           1,
                           storeTest20.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest20.get(entry.getKey(), null).get(0).getValue()));
                assertEquals(storeTest00.get(entry.getKey(), null).size(), 0);
            }

            // Secondary is on Node 2 and not on Node 0
            for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {
                assertSame("entry should be present at store",
                           1,
                           storeTest2.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest2.get(entry.getKey(), null).get(0).getValue()));
                assertEquals(storeTest0.get(entry.getKey(), null).size(), 0);
            }

            // All servers should be back to normal state
            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
            }
        } finally {
            shutDown();
        }
    }

    @Test(timeout = 60000)
    public void testRebalanceNodeRW2() throws IOException {

        try {
            startFourNodeRW();

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

            // Set into rebalancing state
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.SERVER_STATE_KEY,
                                                            MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                            new RebalancerState(Lists.newArrayList(RebalancePartitionsInfo.create(partitionPlan.toJsonString()))));
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

            Store<ByteArray, byte[], byte[]> storeTest00 = getStore(0, "test");
            Store<ByteArray, byte[], byte[]> storeTest10 = getStore(1, "test");
            Store<ByteArray, byte[], byte[]> storeTest30 = getStore(3, "test");

            // Primary
            for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {

                // Test 2
                // Present on Node 0
                assertSame("entry should be present at store",
                           1,
                           storeTest0.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest0.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 1
                assertSame("entry should be present at store",
                           1,
                           storeTest1.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest1.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 3
                assertSame("entry should be present at store",
                           1,
                           storeTest3.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest3.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 2
                assertEquals(storeTest2.get(entry.getKey(), null).size(), 0);

                // Test
                // Present on Node 0
                assertSame("entry should be present at store",
                           1,
                           storeTest00.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest00.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 3
                assertSame("entry should be present at store",
                           1,
                           storeTest30.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest30.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 1
                assertEquals(storeTest10.get(entry.getKey(), null).size(), 0);

            }

            // Secondary
            for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {

                // Test 2
                // Present on Node 0
                assertSame("entry should be present at store",
                           1,
                           storeTest0.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest0.get(entry.getKey(), null).get(0).getValue()));

                // Present on Node 3
                assertSame("entry should be present at store",
                           1,
                           storeTest3.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest3.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 1
                assertEquals(storeTest1.get(entry.getKey(), null).size(), 0);

                // Test
                // Present on Node 3
                assertSame("entry should be present at store",
                           1,
                           storeTest30.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest30.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 0
                assertEquals(storeTest00.get(entry.getKey(), null).size(), 0);

            }

            // Tertiary
            for(Entry<ByteArray, byte[]> entry: tertiaryEntriesMoved.entrySet()) {

                // Test 2
                // Present on Node 3
                assertSame("entry should be present at store",
                           1,
                           storeTest3.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(storeTest3.get(entry.getKey(), null).get(0).getValue()));

                // Not present on Node 0
                assertEquals(storeTest0.get(entry.getKey(), null).size(), 0);
            }

            // All servers should be back to normal state
            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
            }
        } finally {
            shutDown();
        }
    }

    @Test(timeout = 60000)
    public void testRebalanceNodeRO() throws IOException {
        try {
            startFourNodeRO();

            int numChunks = 5;
            for(StoreDefinition storeDef: Lists.newArrayList(storeDef1, storeDef2)) {
                buildROStore(storeDef, numChunks);
            }

            // Set into rebalancing state
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.SERVER_STATE_KEY,
                                                            MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                            new RebalancerState(Lists.newArrayList(RebalancePartitionsInfo.create(partitionPlan.toJsonString()))));
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
                    if(currentPlan.getUnbalancedStoreList().contains(storeDef.getName())) {
                        for(Entry<Integer, List<Integer>> entry: currentPlan.getReplicaToAddPartitionList(storeName)
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
            }

            // All servers should be back to normal state
            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
            }

            // Test the "cluster + swap" changes
            // Test 1) Fail some swap by adding a dummy store
            servers[2].getMetadataStore()
                      .put(MetadataStore.STORES_KEY,
                           Lists.newArrayList(storeDef1,
                                              storeDef2,
                                              new StoreDefinitionBuilder().setName("test3")
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
                                                                          .build()));

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 true,
                                                 true,
                                                 false,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since one node doesn't have the store");
            } catch(VoldemortException e) {}

            servers[2].getMetadataStore().put(MetadataStore.STORES_KEY,
                                              Lists.newArrayList(storeDef1, storeDef2));

            // Test that all servers are still using the old cluster and have
            // swapped successfully
            checkRO(cluster);

            // Test 2) All passes scenario
            adminClient.rebalanceStateChange(cluster,
                                             targetCluster,
                                             plans,
                                             true,
                                             true,
                                             false,
                                             true,
                                             true);

            checkRO(targetCluster);

            // Test 3) Now try fetching files again even though they are
            // mmap-ed. Should fail...
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.SERVER_STATE_KEY,
                                                            MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                            new RebalancerState(Lists.newArrayList(RebalancePartitionsInfo.create(partitionPlan.toJsonString()))));
            }

            // Actually run it
            try {
                int asyncId = adminClient.rebalanceNode(plans.get(0));
                getAdminClient().waitForCompletion(plans.get(0).getStealerId(),
                                                   asyncId,
                                                   300,
                                                   TimeUnit.SECONDS);
                fail("Should throw an exception");
            } catch(Exception e) {}
        } finally {
            shutDown();
        }
    }

    @Test(timeout = 60000)
    public void testRebalanceNodeRORW() throws IOException, InterruptedException {

        try {
            startFourNodeRORW();

            int numChunks = 5;
            for(StoreDefinition storeDef: Lists.newArrayList(storeDef1, storeDef2)) {
                buildROStore(storeDef, numChunks);
            }

            // Set into rebalancing state
            for(RebalancePartitionsInfo partitionPlan: plans) {
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.SERVER_STATE_KEY,
                                                            MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
                getServer(partitionPlan.getStealerId()).getMetadataStore()
                                                       .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                            new RebalancerState(Lists.newArrayList(RebalancePartitionsInfo.create(partitionPlan.toJsonString()))));
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

            // Test 1) Change one of the rebalance partitions info to force a
            // failure

            servers[3].getMetadataStore()
                      .getRebalancerState()
                      .update(new RebalancePartitionsInfo(3,
                                                          0,
                                                          new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                          new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                          cluster,
                                                          0));

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since we added state before hand");
            } catch(VoldemortRebalancingException e) {}

            // All nodes should have nothing in their rebalancing state
            // except node 3
            for(VoldemortServer server: servers) {
                if(server.getMetadataStore().getNodeId() != 3) {
                    assertEquals(server.getMetadataStore().getRebalancerState(),
                                 new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                    assertEquals(server.getMetadataStore().getServerState(),
                                 MetadataStore.VoldemortState.NORMAL_SERVER);
                }
                assertEquals(server.getMetadataStore().getCluster(), cluster);
            }
            checkRO(cluster);

            // Clean-up everything
            cleanUpAllState();

            // Test 2 ) Add another store to trigger a failure
            servers[2].getMetadataStore()
                      .put(MetadataStore.STORES_KEY,
                           Lists.newArrayList(storeDef1,
                                              storeDef2,
                                              storeDef3,
                                              storeDef4,
                                              new StoreDefinitionBuilder().setName("test5")
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
                                                                          .build()));

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since we added state before hand");
            } catch(VoldemortRebalancingException e) {}

            Thread.sleep(1000);

            for(VoldemortServer server: servers) {
                assertEquals(server.getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                assertEquals(server.getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
                assertEquals(server.getMetadataStore().getCluster(), cluster);
            }
            checkRO(cluster);

            // Clean-up everything
            cleanUpAllState();

            // Put back server 2 back to normal state
            servers[2].getMetadataStore().put(MetadataStore.STORES_KEY,
                                              Lists.newArrayList(storeDef1,
                                                                 storeDef2,
                                                                 storeDef3,
                                                                 storeDef4));

            // Test 3) Everything should work
            adminClient.rebalanceStateChange(cluster,
                                             targetCluster,
                                             plans,
                                             true,
                                             true,
                                             true,
                                             true,
                                             true);

            List<Integer> nodesChecked = Lists.newArrayList();
            for(RebalancePartitionsInfo plan: plans) {
                nodesChecked.add(plan.getStealerId());
                assertEquals(servers[plan.getStealerId()].getMetadataStore().getRebalancerState(),
                             new RebalancerState(Lists.newArrayList(plan)));
                assertEquals(servers[plan.getStealerId()].getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
                assertEquals(servers[plan.getStealerId()].getMetadataStore().getCluster(),
                             targetCluster);
            }

            List<Integer> allNodes = Lists.newArrayList(RebalanceUtils.getNodeIds(Lists.newArrayList(cluster.getNodes())));
            allNodes.removeAll(nodesChecked);

            // Check all other nodes
            for(int nodeId: allNodes) {
                assertEquals(servers[nodeId].getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                assertEquals(servers[nodeId].getMetadataStore().getServerState(),
                             MetadataStore.VoldemortState.NORMAL_SERVER);
                assertEquals(servers[nodeId].getMetadataStore().getCluster(), targetCluster);
            }

            checkRO(targetCluster);
        } finally {
            shutDown();
        }
    }

    private void checkRO(Cluster cluster) {
        for(StoreDefinition storeDef: Lists.newArrayList(storeDef1, storeDef2)) {
            Map<Integer, Set<Pair<Integer, Integer>>> nodeToPartitions = RebalanceUtils.getNodeIdToAllPartitions(cluster,
                                                                                                                 storeDef,
                                                                                                                 true);

            for(Map.Entry<Integer, Set<Pair<Integer, Integer>>> entry: nodeToPartitions.entrySet()) {
                int nodeId = entry.getKey();
                Set<Pair<Integer, Integer>> buckets = entry.getValue();

                assertEquals(servers[nodeId].getMetadataStore().getCluster(), cluster);

                ReadOnlyStorageEngine engine = (ReadOnlyStorageEngine) servers[nodeId].getStoreRepository()
                                                                                      .getStorageEngine(storeDef.getName());
                HashMap<Object, Integer> storeBuckets = engine.getChunkedFileSet()
                                                              .getChunkIdToNumChunks();

                for(Pair<Integer, Integer> bucket: buckets) {
                    if(bucket.getFirst() < storeDef.getReplicationFactor())
                        assertEquals(storeBuckets.containsKey(Pair.create(bucket.getSecond(),
                                                                          bucket.getFirst())), true);
                }
            }
        }
    }

    @Test(timeout = 60000)
    public void testRebalanceStateChange() throws IOException {

        try {
            startFourNodeRW();

            // Test 1) Normal case where-in all are up
            adminClient.rebalanceStateChange(cluster,
                                             targetCluster,
                                             plans,
                                             false,
                                             false,
                                             true,
                                             true,
                                             true);

            List<Integer> nodesChecked = Lists.newArrayList();
            for(RebalancePartitionsInfo plan: plans) {
                nodesChecked.add(plan.getStealerId());
                assertEquals(servers[plan.getStealerId()].getMetadataStore().getRebalancerState(),
                             new RebalancerState(Lists.newArrayList(plan)));
            }

            List<Integer> allNodes = Lists.newArrayList(RebalanceUtils.getNodeIds(Lists.newArrayList(cluster.getNodes())));
            allNodes.removeAll(nodesChecked);

            // Check all other nodes
            for(int nodeId: allNodes) {
                assertEquals(servers[nodeId].getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
            }

            // Clean-up everything
            cleanUpAllState();

            // Test 2) Add a plan before hand on one of them which should
            // trigger a rollback
            servers[3].getMetadataStore()
                      .getRebalancerState()
                      .update(new RebalancePartitionsInfo(3,
                                                          0,
                                                          new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                          new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                          cluster,
                                                          0));

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 false,
                                                 false,
                                                 true,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since we added state before hand");
            } catch(VoldemortRebalancingException e) {}

            // All nodes should have nothing in their rebalancing state
            // except node 3
            for(VoldemortServer server: servers) {
                if(server.getMetadataStore().getNodeId() != 3) {
                    assertEquals(server.getMetadataStore().getRebalancerState(),
                                 new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                }
            }

            // Clean-up everything
            cleanUpAllState();

            // Test 3) Shut one node down
            ServerTestUtils.stopVoldemortServer(servers[3]);
            servers[3] = null;

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 false,
                                                 false,
                                                 true,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since we added state before hand");
            } catch(VoldemortRebalancingException e) {}

            // All nodes should have nothing in their rebalancing state
            // exception node 3
            for(VoldemortServer server: servers) {
                if(server != null) {
                    assertEquals(server.getMetadataStore().getRebalancerState(),
                                 new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                }
            }
        } finally {
            shutDown();
        }
    }

    @Test(timeout = 60000)
    public void testClusterAndRebalanceStateChange() throws IOException {

        try {
            startFourNodeRW();

            // Test 1) Normal case where-in all are up
            adminClient.rebalanceStateChange(cluster,
                                             targetCluster,
                                             plans,
                                             false,
                                             true,
                                             true,
                                             true,
                                             true);

            List<Integer> nodesChecked = Lists.newArrayList();
            for(RebalancePartitionsInfo plan: plans) {
                nodesChecked.add(plan.getStealerId());
                assertEquals(servers[plan.getStealerId()].getMetadataStore().getRebalancerState(),
                             new RebalancerState(Lists.newArrayList(plan)));
                assertEquals(servers[plan.getStealerId()].getMetadataStore().getCluster(),
                             targetCluster);
            }

            List<Integer> allNodes = Lists.newArrayList(RebalanceUtils.getNodeIds(Lists.newArrayList(cluster.getNodes())));
            allNodes.removeAll(nodesChecked);

            // Check all other nodes
            for(int nodeId: allNodes) {
                assertEquals(servers[nodeId].getMetadataStore().getRebalancerState(),
                             new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                assertEquals(servers[nodeId].getMetadataStore().getCluster(), targetCluster);
            }

            // Clean-up everything
            cleanUpAllState();

            // Test 2) Add a plan before hand on one of them which should
            // trigger a rollback
            servers[3].getMetadataStore()
                      .getRebalancerState()
                      .update(new RebalancePartitionsInfo(3,
                                                          0,
                                                          new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                          new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                          cluster,
                                                          0));

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 false,
                                                 true,
                                                 true,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since we added state before hand");
            } catch(VoldemortRebalancingException e) {}

            // All nodes should have nothing in their rebalancing state
            // except node 3 + all of them should have old cluster metadata
            for(VoldemortServer server: servers) {
                if(server.getMetadataStore().getNodeId() != 3) {
                    assertEquals(server.getMetadataStore().getRebalancerState(),
                                 new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                }
                assertEquals(server.getMetadataStore().getCluster(), cluster);
            }

            // Clean-up everything
            cleanUpAllState();

            // Test 3) Shut one node down
            ServerTestUtils.stopVoldemortServer(servers[3]);
            servers[3] = null;

            try {
                adminClient.rebalanceStateChange(cluster,
                                                 targetCluster,
                                                 plans,
                                                 false,
                                                 true,
                                                 true,
                                                 true,
                                                 true);
                fail("Should have thrown an exception since we added state before hand");
            } catch(VoldemortRebalancingException e) {}

            // All nodes should have nothing in their rebalancing state
            // exception node 3
            for(VoldemortServer server: servers) {
                if(server != null) {
                    assertEquals(server.getMetadataStore().getRebalancerState(),
                                 new RebalancerState(new ArrayList<RebalancePartitionsInfo>()));
                    assertEquals(server.getMetadataStore().getCluster(), cluster);
                }
            }
        } finally {
            shutDown();
        }
    }

    private void cleanUpAllState() {
        for(VoldemortServer server: servers) {

            if(server != null) {
                // Put back the old cluster metadata
                server.getMetadataStore().put(MetadataStore.CLUSTER_KEY, cluster);

                // Clear all the rebalancing state
                server.getMetadataStore().cleanAllRebalancingState();
            }
        }
    }

    private void buildROStore(StoreDefinition storeDef, int numChunks) throws IOException {
        Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions = RebalanceUtils.getNodeIdToAllPartitions(cluster,
                                                                                                                  storeDef,
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
