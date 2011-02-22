/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.grandfather.GrandfatherState;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.slop.Slop;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
@RunWith(Parameterized.class)
public class AdminServiceBasicTest extends TestCase {

    private static int NUM_RUNS = 100;
    private static int TEST_STREAM_KEYS_SIZE = 10000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClient adminClient;

    private final boolean useNio;

    public AdminServiceBasicTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        servers = new VoldemortServer[2];

        servers[0] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(useNio,
                                                                                             0,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);
        servers[1] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(useNio,
                                                                                             1,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Override
    @After
    public void tearDown() throws IOException, InterruptedException {
        adminClient.stop();
        for(VoldemortServer server: servers) {
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

    private boolean isKeyPartition(ByteArray key,
                                   int nodeId,
                                   String storeName,
                                   List<Integer> deletePartitionsList) {
        RoutingStrategy routing = getVoldemortServer(nodeId).getMetadataStore()
                                                            .getRoutingStrategy(storeName);
        for(int partition: routing.getPartitionList(key.get())) {
            if(deletePartitionsList.contains(partition)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testUpdateClusterMetadata() {
        Cluster updatedCluster = ServerTestUtils.getLocalCluster(4);
        AdminClient client = getAdminClient();
        for(int i = 0; i < NUM_RUNS; i++) {
            VectorClock clock = ((VectorClock) client.getRemoteCluster(0).getVersion()).incremented(0,
                                                                                                    System.currentTimeMillis());
            client.updateRemoteCluster(0, updatedCluster, clock);

            assertEquals("Cluster should match",
                         updatedCluster,
                         getVoldemortServer(0).getMetadataStore().getCluster());
            assertEquals("AdminClient.getMetdata() should match", client.getRemoteCluster(0)
                                                                        .getValue(), updatedCluster);

            // version should match
            assertEquals("versions should match as well.", clock, client.getRemoteCluster(0)
                                                                        .getVersion());
        }

    }

    @Test
    public void testAddStore() throws Exception {
        AdminClient adminClient = getAdminClient();

        StoreDefinition definition = new StoreDefinitionBuilder().setName("updateTest")
                                                                 .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(1)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();
        adminClient.addStore(definition);

        // now test the store
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(cluster.getNodeById(0)
                                                                                                             .getSocketUrl()
                                                                                                             .toString()));

        StoreClient<Object, Object> client = factory.getStoreClient("updateTest");
        client.put("abc", "123");
        String s = (String) client.get("abc").getValue();
        assertEquals(s, "123");

        // test again with a unknown store
        try {
            client = factory.getStoreClient("updateTest2");
            client.put("abc", "123");
            s = (String) client.get("abc").getValue();
            assertEquals(s, "123");
            fail("Should have received bootstrap failure exception");
        } catch(Exception e) {
            if(!(e instanceof BootstrapFailureException))
                throw e;
        }

        // make sure that the store list we get back from AdminClient
        Versioned<List<StoreDefinition>> list = adminClient.getRemoteStoreDefList(0);
        assertTrue(list.getValue().contains(definition));
    }

    @Test
    public void testReplicationMapping() {
        List<Node> nodes = Lists.newArrayList();
        nodes.add(new Node(0, "localhost", 1, 2, 3, 0, Lists.newArrayList(0, 4, 8)));
        nodes.add(new Node(1, "localhost", 1, 2, 3, 0, Lists.newArrayList(1, 5, 9)));
        nodes.add(new Node(2, "localhost", 1, 2, 3, 1, Lists.newArrayList(2, 6, 10)));
        nodes.add(new Node(3, "localhost", 1, 2, 3, 1, Lists.newArrayList(3, 7, 11)));

        RoutingStrategyFactory factory = new RoutingStrategyFactory();

        // Test 1 - With consistent routing strategy
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("consistent",
                                                               2,
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        Cluster newCluster = new Cluster("single_zone_cluster", nodes);
        RoutingStrategy strategy = factory.updateRoutingStrategy(storeDef, newCluster);
        Map<Integer, List<Integer>> replicationMapping = adminClient.getReplicationMapping(newCluster,
                                                                                           0,
                                                                                           strategy);
        Map<Integer, List<Integer>> expected = Maps.newHashMap();
        expected.put(1, Lists.newArrayList(1, 5, 9));
        expected.put(3, Lists.newArrayList(3, 7, 11));
        assertEquals(replicationMapping, expected);

        replicationMapping = adminClient.getReplicationMapping(newCluster, 2, strategy);
        assertEquals(replicationMapping, expected);

        replicationMapping = adminClient.getReplicationMapping(newCluster, 1, strategy);
        expected = Maps.newHashMap();
        expected.put(0, Lists.newArrayList(0, 4, 8));
        expected.put(2, Lists.newArrayList(2, 6, 10));
        assertEquals(replicationMapping, expected);

        // Test 2 - With zone routing strategy
        List<Zone> zones = ServerTestUtils.getZones(2);
        HashMap<Integer, Integer> zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < 2; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        storeDef = ServerTestUtils.getStoreDef("zone",
                                               2,
                                               1,
                                               1,
                                               1,
                                               0,
                                               0,
                                               zoneReplicationFactors,
                                               HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                               RoutingStrategyType.ZONE_STRATEGY);
        strategy = factory.updateRoutingStrategy(storeDef, newCluster);
        newCluster = new Cluster("multi_zone_cluster", nodes, zones);

        replicationMapping = adminClient.getReplicationMapping(newCluster, 0, strategy);
        expected = Maps.newHashMap();
        expected.put(2, Lists.newArrayList(2, 6, 10));
        expected.put(3, Lists.newArrayList(3, 7, 11));
        assertEquals(replicationMapping, expected);

        replicationMapping = adminClient.getReplicationMapping(newCluster, 1, strategy);
        expected = Maps.newHashMap();
        expected.put(2, Lists.newArrayList(2, 6, 10));
        assertEquals(replicationMapping, expected);

        replicationMapping = adminClient.getReplicationMapping(newCluster, 2, strategy);
        expected = Maps.newHashMap();
        expected.put(0, Lists.newArrayList(4, 8, 0));
        expected.put(1, Lists.newArrayList(1, 5, 9));
        assertEquals(replicationMapping, expected);

    }

    @Test
    public void testDeleteStore() throws Exception {
        AdminClient adminClient = getAdminClient();

        StoreDefinition definition = new StoreDefinitionBuilder().setName("deleteTest")
                                                                 .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(1)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();
        adminClient.addStore(definition);

        // now test the store
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(cluster.getNodeById(0)
                                                                                                             .getSocketUrl()
                                                                                                             .toString()));

        StoreClient<Object, Object> client = factory.getStoreClient("deleteTest");

        int numStores = adminClient.getRemoteStoreDefList(0).getValue().size();

        // delete the store
        assertEquals(adminClient.getRemoteStoreDefList(0).getValue().contains(definition), true);
        adminClient.deleteStore("deleteTest");
        assertEquals(adminClient.getRemoteStoreDefList(0).getValue().size(), numStores - 1);
        assertEquals(adminClient.getRemoteStoreDefList(0).getValue().contains(definition), false);

        // test with deleted store
        try {
            client = factory.getStoreClient("deleteTest");
            client.put("abc", "123");
            String s = (String) client.get("abc").getValue();
            assertEquals(s, "123");
            fail("Should have received bootstrap failure exception");
        } catch(Exception e) {
            if(!(e instanceof BootstrapFailureException))
                throw e;
        }
        // try adding the store again
        adminClient.addStore(definition);

        client = factory.getStoreClient("deleteTest");
        client.put("abc", "123");
        String s = (String) client.get("abc").getValue();
        assertEquals(s, "123");
    }

    @Test
    public void testStateTransitions() {
        // change to REBALANCING STATE
        AdminClient client = getAdminClient();
        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        MetadataStore.VoldemortState state = getVoldemortServer(0).getMetadataStore()
                                                                  .getServerState();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                     state);

        // change back to NORMAL state
        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.NORMAL_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerState();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);

        // lets revert back to REBALANCING STATE AND CHECK
        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerState();

        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                     state);

        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.NORMAL_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerState();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);
    }

    @Test
    public void testUpdateGrandfatherMetadata() {
        AdminClient client = getAdminClient();
        HashMap<String, String> roToDir = new HashMap<String, String>();
        roToDir.put("a", "b");
        roToDir.put("c", "d");
        roToDir.put("e", "f");
        List<RebalancePartitionsInfo> rebalancePartitionsInfos = Arrays.asList(new RebalancePartitionsInfo(2,
                                                                                                           0,
                                                                                                           Arrays.asList(1,
                                                                                                                         2,
                                                                                                                         3,
                                                                                                                         4),
                                                                                                           Arrays.asList(0,
                                                                                                                         1),
                                                                                                           Arrays.asList(0,
                                                                                                                         1,
                                                                                                                         2),
                                                                                                           Arrays.asList("test1",
                                                                                                                         "test2"),
                                                                                                           roToDir,
                                                                                                           roToDir,
                                                                                                           0),
                                                                               new RebalancePartitionsInfo(3,
                                                                                                           1,
                                                                                                           Arrays.asList(5,
                                                                                                                         6,
                                                                                                                         7,
                                                                                                                         8),
                                                                                                           new ArrayList<Integer>(0),
                                                                                                           new ArrayList<Integer>(0),
                                                                                                           Arrays.asList("test1",
                                                                                                                         "test2"),
                                                                                                           new HashMap<String, String>(),
                                                                                                           new HashMap<String, String>(),
                                                                                                           0));

        Versioned<String> currentState = client.getRemoteMetadata(getVoldemortServer(0).getIdentityNode()
                                                                                       .getId(),
                                                                  MetadataStore.SERVER_STATE_KEY);

        // Check if initially the server is set up correctly
        assertEquals(getVoldemortServer(0).getMetadataStore().getGrandfatherState(),
                     new GrandfatherState(new ArrayList<RebalancePartitionsInfo>()));
        // Update the metadata
        Versioned<String> updatedState = client.updateGrandfatherMetadata(getVoldemortServer(0).getIdentityNode()
                                                                                               .getId(),
                                                                          rebalancePartitionsInfos);
        assertEquals(updatedState.getValue(),
                     MetadataStore.VoldemortState.GRANDFATHERING_SERVER.toString());
        assertEquals(updatedState.getVersion().compare(currentState.getVersion()), Occured.AFTER);
        assertEquals(getVoldemortServer(0).getMetadataStore().getServerState(),
                     MetadataStore.VoldemortState.GRANDFATHERING_SERVER);
        assertEquals(getVoldemortServer(0).getMetadataStore().getGrandfatherState(),
                     new GrandfatherState(rebalancePartitionsInfos));

        // Now try updating since its already in grandfathering state
        try {
            client.updateGrandfatherMetadata(getVoldemortServer(0).getIdentityNode().getId(),
                                             new ArrayList<RebalancePartitionsInfo>());
            Assert.fail("Should have thrown an exception");
        } catch(Exception e) {

        }
        assertEquals(getVoldemortServer(0).getMetadataStore().getServerState(),
                     MetadataStore.VoldemortState.GRANDFATHERING_SERVER);
        assertEquals(getVoldemortServer(0).getMetadataStore().getGrandfatherState(),
                     new GrandfatherState(rebalancePartitionsInfos));

        // Lets set the state of the server back to normal
        VectorClock updatedVectorClock = ((VectorClock) updatedState.getVersion()).incremented(getVoldemortServer(0).getIdentityNode()
                                                                                                                    .getId(),
                                                                                               System.currentTimeMillis());
        adminClient.updateRemoteMetadata(getVoldemortServer(0).getIdentityNode().getId(),
                                         MetadataStore.SERVER_STATE_KEY,
                                         Versioned.value(MetadataStore.VoldemortState.NORMAL_SERVER.toString(),
                                                         updatedVectorClock));
        assertEquals(getVoldemortServer(0).getMetadataStore().getServerState(),
                     MetadataStore.VoldemortState.NORMAL_SERVER);

        Versioned<String> newState = client.getRemoteMetadata(getVoldemortServer(0).getIdentityNode()
                                                                                   .getId(),
                                                              MetadataStore.SERVER_STATE_KEY);
        assertTrue(((VectorClock) newState.getVersion()).equals(updatedVectorClock));
    }

    @Test
    public void testDeletePartitionEntries() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);

        // insert it into server-0 store
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
        }

        List<Integer> deletePartitionsList = Arrays.asList(0, 2);

        // do delete partitions request
        getAdminClient().deletePartitions(0, testStoreName, deletePartitionsList, null);

        store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            if(isKeyPartition(entry.getKey(), 0, testStoreName, deletePartitionsList)) {
                assertEquals("deleted partitions should be missing.", 0, store.get(entry.getKey(),
                                                                                   null).size());
            }
        }
    }

    @Test
    public void testFetchPartitionKeys() {

        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchPartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchPartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        Iterator<ByteArray> fetchIt = getAdminClient().fetchKeys(0,
                                                                 testStoreName,
                                                                 fetchPartitionsList,
                                                                 null,
                                                                 false);
        // check values
        int count = 0;
        while(fetchIt.hasNext()) {
            assertEquals("Fetched key should belong to asked partitions",
                         true,
                         isKeyPartition(fetchIt.next(), 0, testStoreName, fetchPartitionsList));
            count++;
        }

        // assert all keys for asked partitions are returned.
        assertEquals("All keys for asked partitions should be received",
                     fetchPartitionKeyCount,
                     count);
    }

    @Test
    public void testFetchPartitionFiles() throws IOException {
        if(useNio)
            generateAndFetchFiles(10, 1, 1000, 1000);
    }

    private void generateFiles(int numChunks,
                               long indexSize,
                               long dataSize,
                               List<Integer> partitions,
                               File versionDir) throws IOException {

        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V1.getCode());

        File metadataFile = new File(versionDir, ".metadata");
        BufferedWriter writer = new BufferedWriter(new FileWriter(metadataFile));
        writer.write(metadata.toJsonString());
        writer.close();

        for(Integer partitionId: partitions) {
            for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                File index = new File(versionDir, Integer.toString(partitionId) + "_"
                                                  + Integer.toString(chunkId) + ".index");
                File data = new File(versionDir, Integer.toString(partitionId) + "_"
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

    private void generateAndFetchFiles(int numChunks, long versionId, long indexSize, long dataSize)
            throws IOException {
        for(Node node: cluster.getNodes()) {
            ReadOnlyStorageEngine store = (ReadOnlyStorageEngine) getStore(node.getId(),
                                                                           "test-readonly-fetchfiles");

            // Generate data
            File newVersionDir = new File(store.getStoreDirPath(), "version-"
                                                                   + Long.toString(versionId));
            Utils.mkdirs(newVersionDir);
            generateFiles(numChunks, indexSize, dataSize, node.getPartitionIds(), newVersionDir);

            // Swap it...
            store.swapFiles(newVersionDir.getAbsolutePath());

            // Fetch it...
            File tempDir = TestUtils.createTempDir();

            getAdminClient().fetchPartitionFiles(node.getId(),
                                                 "test-readonly-fetchfiles",
                                                 node.getPartitionIds(),
                                                 tempDir.getAbsolutePath());

            // Check it...
            assertEquals(tempDir.list().length, 2 * node.getPartitionIds().size() * numChunks);

            for(Integer partitionId: node.getPartitionIds()) {
                for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                    File indexFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                       + Integer.toString(chunkId) + ".index");
                    File dataFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                      + Integer.toString(chunkId) + ".data");

                    assertTrue(indexFile.exists());
                    assertTrue(dataFile.exists());
                    assertEquals(indexFile.length(), indexSize);
                    assertEquals(dataFile.length(), dataSize);
                }

            }
        }
    }

    @Test
    public void testGetROVersions() throws IOException {

        // Tests get current version
        Map<String, Long> storesToVersions = getAdminClient().getROCurrentVersion(0,
                                                                                  Lists.newArrayList("test-readonly-fetchfiles",
                                                                                                     "test-readonly-versions"));
        assertEquals(storesToVersions.size(), 2);
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);
        assertEquals(storesToVersions.get("test-readonly-versions").longValue(), 0);

        // Tests get maximum version
        storesToVersions = getAdminClient().getROMaxVersion(0,
                                                            Lists.newArrayList("test-readonly-fetchfiles",
                                                                               "test-readonly-versions"));
        assertEquals(storesToVersions.size(), 2);
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);
        assertEquals(storesToVersions.get("test-readonly-versions").longValue(), 0);

        // Tests global get maximum versions
        storesToVersions = getAdminClient().getROMaxVersion(Lists.newArrayList("test-readonly-fetchfiles",
                                                                               "test-readonly-versions"));
        assertEquals(storesToVersions.size(), 2);
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);
        assertEquals(storesToVersions.get("test-readonly-versions").longValue(), 0);

        ReadOnlyStorageEngine storeNode0 = (ReadOnlyStorageEngine) getStore(0,
                                                                            "test-readonly-fetchfiles");
        ReadOnlyStorageEngine storeNode1 = (ReadOnlyStorageEngine) getStore(1,
                                                                            "test-readonly-fetchfiles");

        Utils.mkdirs(new File(storeNode0.getStoreDirPath(), "version-10"));
        File newVersionNode1 = new File(storeNode1.getStoreDirPath(), "version-11");
        Utils.mkdirs(newVersionNode1);
        generateFiles(10, 20, 20, cluster.getNodeById(1).getPartitionIds(), newVersionNode1);
        storeNode1.swapFiles(newVersionNode1.getAbsolutePath());

        // Node 0
        // Test current version
        storesToVersions = getAdminClient().getROCurrentVersion(0,
                                                                Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);

        // Test max version
        storesToVersions = getAdminClient().getROMaxVersion(0,
                                                            Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 10);

        // Node 1
        // Test current version
        storesToVersions = getAdminClient().getROCurrentVersion(1,
                                                                Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 11);

        // Test max version
        storesToVersions = getAdminClient().getROMaxVersion(1,
                                                            Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 11);

        // Test global max
        storesToVersions = getAdminClient().getROMaxVersion(Lists.newArrayList("test-readonly-fetchfiles",
                                                                               "test-readonly-versions"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 11);
        assertEquals(storesToVersions.get("test-readonly-versions").longValue(), 0);

    }

    @Test
    public void testTruncate() throws Exception {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);

        // insert it into server-0 store
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
        }

        // do truncate request
        getAdminClient().truncate(0, testStoreName);

        store = getStore(0, testStoreName);

        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            assertEquals("Deleted key should be missing.", 0, store.get(entry.getKey(), null)
                                                                   .size());
        }
    }

    @Test
    public void testFetch() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchPartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchPartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchIt = getAdminClient().fetchEntries(0,
                                                                                             testStoreName,
                                                                                             fetchPartitionsList,
                                                                                             null,
                                                                                             false);
        // check values
        int count = 0;
        while(fetchIt.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = fetchIt.next();
            assertEquals("Fetched entries should belong to asked partitions",
                         true,
                         isKeyPartition(entry.getFirst(), 0, testStoreName, fetchPartitionsList));
            assertEquals("entry value should match",
                         new String(entry.getSecond().getValue()),
                         new String(entrySet.get(entry.getFirst())));
            count++;
        }

        // assert all keys for asked partitions are returned.
        assertEquals("All entries for asked partitions should be received",
                     fetchPartitionKeyCount,
                     count);

    }

    @Test
    public void testUpdate() {
        final HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);

        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            final Iterator<Entry<ByteArray, byte[]>> entrySetItr = entrySet.entrySet().iterator();

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                while(entrySetItr.hasNext()) {
                    Entry<ByteArray, byte[]> entry = entrySetItr.next();
                    return new Pair<ByteArray, Versioned<byte[]>>(entry.getKey(),
                                                                  new Versioned<byte[]>(entry.getValue()));
                }
                return endOfData();
            }
        };

        getAdminClient().updateEntries(0, testStoreName, iterator, null);

        // check updated values
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            assertNotSame("entry should be present at store", 0, store.get(entry.getKey(), null)
                                                                      .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store.get(entry.getKey(), null).get(0).getValue()));
        }
    }

    @Test
    public void testUpdateSlops() {
        final List<Versioned<Slop>> entrySet = ServerTestUtils.createRandomSlops(0,
                                                                                 10000,
                                                                                 testStoreName,
                                                                                 "users",
                                                                                 "test-replication-persistent",
                                                                                 "test-readrepair-memory",
                                                                                 "test-consistent",
                                                                                 "test-consistent-with-pref-list");

        Iterator<Versioned<Slop>> slopIterator = entrySet.iterator();
        getAdminClient().updateSlopEntries(0, slopIterator);

        // check updated values
        Iterator<Versioned<Slop>> entrysetItr = entrySet.iterator();

        while(entrysetItr.hasNext()) {
            Versioned<Slop> versioned = entrysetItr.next();
            Slop nextSlop = versioned.getValue();
            Store<ByteArray, byte[], byte[]> store = getStore(0, nextSlop.getStoreName());

            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store", 0, store.get(nextSlop.getKey(),
                                                                               null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }
        }
    }

    // check the basic rebalanceNode call.
    @Test
    public void testRebalanceNode() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchAndUpdatePartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchAndUpdatePartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        List<Integer> rebalancePartitionList = Arrays.asList(1, 3);
        RebalancePartitionsInfo stealInfo = new RebalancePartitionsInfo(1,
                                                                        0,
                                                                        rebalancePartitionList,
                                                                        rebalancePartitionList,
                                                                        rebalancePartitionList,
                                                                        Arrays.asList(testStoreName),
                                                                        new HashMap<String, String>(),
                                                                        new HashMap<String, String>(),
                                                                        0);
        int asyncId = adminClient.rebalanceNode(stealInfo);
        assertNotSame("Got a valid rebalanceAsyncId", -1, asyncId);

        getAdminClient().waitForCompletion(1, asyncId, 120, TimeUnit.SECONDS);

        // assert data is copied correctly
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            if(isKeyPartition(entry.getKey(), 1, testStoreName, rebalancePartitionList)) {
                assertSame("entry should be present at store", 1, store.get(entry.getKey(), null)
                                                                       .size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(store.get(entry.getKey(), null).get(0).getValue()));
            }
        }
    }

    @Test
    public void testRecoverData() {
        // use store with replication 2, required write 2 for this test.
        String testStoreName = "test-recovery-data";

        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        // insert it into server-0 store
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
        }

        // assert server 1 is empty
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            assertSame("entry should NOT be present at store", 0, store.get(entry.getKey(), null)
                                                                       .size());
        }

        // recover all data
        adminClient.restoreDataFromReplications(1, 2);

        // assert server 1 has all entries for its partitions
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            ByteArray key = entry.getKey();
            assertSame("entry should be present for key " + key, 1, store.get(entry.getKey(), null)
                                                                         .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store.get(entry.getKey(), null).get(0).getValue()));
        }
    }

    /**
     * @throws IOException
     */
    @Test
    public void testFetchAndUpdate() throws IOException {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchAndUpdatePartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchAndUpdatePartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        // assert that server1 is empty.
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet())
            assertEquals("server1 should be empty at start.", 0, store.get(entry.getKey(), null)
                                                                      .size());

        // do fetch And update call server1 <-- server0
        AdminClient client = getAdminClient();
        int id = client.migratePartitions(0, 1, testStoreName, fetchAndUpdatePartitionsList, null);
        client.waitForCompletion(1, id, 60, TimeUnit.SECONDS);

        // check values
        int count = 0;
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchAndUpdatePartitionsList)) {
                assertEquals("server1 store should contain fetchAndupdated partitions.",
                             1,
                             store.get(entry.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(store.get(entry.getKey(), null).get(0).getValue()));
                count++;
            }
        }

        // assert all keys for asked partitions are returned.
        assertEquals("All keys for asked partitions should be received",
                     fetchPartitionKeyCount,
                     count);

    }
}
