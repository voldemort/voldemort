/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
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
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
@RunWith(Parameterized.class)
public class AdminServiceBasicTest {

    private static int NUM_RUNS = 100;
    private static int TEST_STREAM_KEYS_SIZE = 10000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private static AtomicBoolean running = new AtomicBoolean(true);
    private List<StoreDefinition> storeDefs;
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

    @Before
    public void setUp() throws IOException {
        int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };
        Properties serverProperties = new Properties();
        serverProperties.setProperty("client.max.connections.per.node", "20");
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        useNio,
                                                        null,
                                                        storesXmlfile,
                                                        serverProperties);

        storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile));

        Properties adminProperties = new Properties();
        adminProperties.setProperty("max_connections", "20");
        adminClient = new AdminClient(cluster, new AdminClientConfig(adminProperties));
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

    @After
    public void tearDown() throws IOException {
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

        // Try to add a store whose replication factor is greater than the
        // number of nodes
        StoreDefinition definition = new StoreDefinitionBuilder().setName("updateTest")
                                                                 .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();
        try {
            adminClient.addStore(definition);
            fail("Should have thrown an exception because we cannot add a store with a replication factor greater than number of nodes");
        } catch(Exception e) {}

        definition = new StoreDefinitionBuilder().setName("updateTest")
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

        // Test 0 - With rep-factor 1
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("consistent",
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Cluster newCluster = new Cluster("single_zone_cluster", nodes);

        try {
            adminClient.getReplicationMapping(0, newCluster, storeDef);
            fail("Should have thrown an exception since rep-factor = 1");
        } catch(VoldemortException e) {}

        // Test 1 - With consistent routing strategy
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               2,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        // On node 0
        Map<Integer, HashMap<Integer, List<Integer>>> replicationMapping = adminClient.getReplicationMapping(0,
                                                                                                             newCluster,
                                                                                                             storeDef);
        {
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.clear();
            partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(1, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple2);
            // {1={1=[0, 4, 8]}, 3={0=[3, 7, 11]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        {
            // On node 1
            replicationMapping = adminClient.getReplicationMapping(1, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, partitionTuple2);
            // {0={0=[0, 4, 8]}, 2={1=[1, 5, 9]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        {
            // On node 2
            replicationMapping = adminClient.getReplicationMapping(2, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(1, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(1, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(3, partitionTuple2);
            // {1={0=[1, 5, 9]}, 3={1=[2, 6, 10]}}
            assertEquals(replicationMapping, expectedMapping);
        }
        {
            // On node 3
            replicationMapping = adminClient.getReplicationMapping(3, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(1, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(2, partitionTuple2);
            // {0={1=[3, 7, 11]}, 2={0=[2, 6, 10]}}
            assertEquals(replicationMapping, expectedMapping);
        }

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
        newCluster = new Cluster("multi_zone_cluster", nodes, zones);

        {
            // On node 0
            replicationMapping = adminClient.getReplicationMapping(0, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(2, 6, 10));
            partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(2, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple2);
            // {2={0=[2, 6, 10], 1=[0, 4, 8]}, 3={0=[3, 7, 11]}}
            assertEquals(replicationMapping, expectedMapping);
        }
        {
            // On node 1
            replicationMapping = adminClient.getReplicationMapping(1, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, partitionTuple);
            // {2={1=[1, 5, 9]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        {
            // On node 2
            replicationMapping = adminClient.getReplicationMapping(2, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            partitionTuple.put(1, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(1, partitionTuple2);
            // {0={0=[0, 4, 8], 1=[2, 6, 10]}, 1={0=[1, 5, 9]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        {
            // On node 3
            replicationMapping = adminClient.getReplicationMapping(3, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(1, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(0, partitionTuple);
            // {0={1=[3, 7, 11]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        // Test 3 - Consistent with rep factor 3
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               3,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        newCluster = new Cluster("single_zone_cluster", nodes);

        {
            replicationMapping = adminClient.getReplicationMapping(0, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(1, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(0, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(2, partitionTuple3);
            // {1={1=[0, 4, 8]}, 2={0=[2, 6, 10]}, 3={0=[3, 7, 11]}}
            assertEquals(replicationMapping, expectedMapping);

        }

        {
            replicationMapping = adminClient.getReplicationMapping(1, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, partitionTuple3);
            // {0={0=[0, 4, 8]}, 2={1=[1, 5, 9]}, 3={0=[3, 7, 11]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        {
            replicationMapping = adminClient.getReplicationMapping(2, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(1, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(1, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(3, partitionTuple3);
            // {0={0=[0, 4, 8]}, 1={0=[1, 5, 9]}, 3={1=[2, 6, 10]}}
            assertEquals(replicationMapping, expectedMapping);

        }

        {
            replicationMapping = adminClient.getReplicationMapping(3, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(1, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(1, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(0, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(2, partitionTuple3);
            // {0={1=[3, 7, 11]}, 1={0=[1, 5, 9]}, 2={0=[2, 6, 10]}}
            assertEquals(replicationMapping, expectedMapping);

        }

        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < 2; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }

        storeDef = ServerTestUtils.getStoreDef("zone",
                                               1,
                                               1,
                                               1,
                                               1,
                                               0,
                                               0,
                                               zoneReplicationFactors,
                                               HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                               RoutingStrategyType.ZONE_STRATEGY);
        newCluster = new Cluster("multi_zone_cluster", nodes, zones);
        {
            replicationMapping = adminClient.getReplicationMapping(0, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(1, 5, 9));
            partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(1, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(2, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple3);
            // {1={0=[1, 5, 9], 1=[0, 4, 8]}, 2={0=[2, 6, 10]}, 3={0=[3, 7,
            // 11]}}
            assertEquals(replicationMapping, expectedMapping);

        }

        {
            replicationMapping = adminClient.getReplicationMapping(1, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(2, 6, 10));
            partitionTuple2.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple3);
            // {0={0=[0, 4, 8]}, 2={0=[2, 6, 10], 1=[1, 5, 9]}, 3={0=[3, 7,
            // 11]}}
            assertEquals(replicationMapping, expectedMapping);

        }

        {
            replicationMapping = adminClient.getReplicationMapping(2, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(1, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(0, Lists.newArrayList(3, 7, 11));
            partitionTuple3.put(1, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(3, partitionTuple3);
            // {0={0=[0, 4, 8]}, 1={0=[1, 5, 9]}, 3={0=[3, 7, 11], 1=[2, 6,
            // 10]}}
            assertEquals(replicationMapping, expectedMapping);

        }

        {
            replicationMapping = adminClient.getReplicationMapping(3, newCluster, storeDef);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(0, 4, 8));
            partitionTuple.put(1, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(0, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(1, partitionTuple2);
            HashMap<Integer, List<Integer>> partitionTuple3 = Maps.newHashMap();
            partitionTuple3.put(0, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(2, partitionTuple3);
            // {0={0=[0, 4, 8], 1=[3, 7, 11]}, 1={0=[1, 5, 9]}, 2={0=[2, 6,
            // 10]}}
            assertEquals(replicationMapping, expectedMapping);
        }
    }

    @Test
    public void testReplicationMappingWithZonePreference() {
        List<Node> nodes = Lists.newArrayList();
        nodes.add(new Node(0, "localhost", 1, 2, 3, 0, Lists.newArrayList(0, 4, 8)));
        nodes.add(new Node(1, "localhost", 1, 2, 3, 0, Lists.newArrayList(1, 5, 9)));
        nodes.add(new Node(2, "localhost", 1, 2, 3, 1, Lists.newArrayList(2, 6, 10)));
        nodes.add(new Node(3, "localhost", 1, 2, 3, 1, Lists.newArrayList(3, 7, 11)));

        // Test 0 - With rep-factor 1; zone 1
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("consistent",
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Cluster newCluster = new Cluster("single_zone_cluster", nodes);

        try {
            adminClient.getReplicationMapping(0, newCluster, storeDef, 1);
            fail("Should have thrown an exception since rep-factor = 1");
        } catch(VoldemortException e) {}

        // With rep-factor 1; zone 0
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               1,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        newCluster = new Cluster("single_zone_cluster", nodes);

        try {
            adminClient.getReplicationMapping(0, newCluster, storeDef, 0);
            fail("Should have thrown an exception since rep-factor = 1");
        } catch(VoldemortException e) {}

        // Test 1 - With consistent routing strategy
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               4,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        // On node 0; zone id 1
        Map<Integer, HashMap<Integer, List<Integer>>> replicationMapping = adminClient.getReplicationMapping(0,
                                                                                                             newCluster,
                                                                                                             storeDef,
                                                                                                             1);
        {
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(2, 6, 10));
            partitionTuple.put(1, Lists.newArrayList(1, 5, 9));
            partitionTuple.put(2, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(2, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple2);
            // {2={0=[2, 6, 10], 1=[1, 5, 9], 2=[0, 4, 8]}, 3={0=[3, 7, 11]}}
            assertEquals(replicationMapping, expectedMapping);
        }

        // On node 0; zone id 0
        replicationMapping = adminClient.getReplicationMapping(0, newCluster, storeDef, 0);
        {
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.clear();
            partitionTuple.put(0, Lists.newArrayList(1, 5, 9));
            partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            partitionTuple.put(2, Lists.newArrayList(3, 7, 11));
            partitionTuple.put(3, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(1, partitionTuple);
            // {1={0=[1, 5, 9], 1=[0, 4, 8]}, 2=[3, 7, 11], 3=[2, 6, 10]}
            assertEquals(replicationMapping, expectedMapping);
        }

        // Test 2 - With zone routing strategy, and zone replication factor 1
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
        newCluster = new Cluster("multi_zone_cluster", nodes, zones);

        {
            // On node 0, zone 0 - failure case since zoneReplicationFactor is 1

            try {
                replicationMapping = adminClient.getReplicationMapping(0, newCluster, storeDef, 0);
                fail("Should have thrown an exception since  zoneReplicationFactor is 1");
            } catch(VoldemortException e) {}
        }

        {
            // On node 0, zone 1
            replicationMapping = adminClient.getReplicationMapping(0, newCluster, storeDef, 1);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(0, Lists.newArrayList(2, 6, 10));
            partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(2, partitionTuple);
            HashMap<Integer, List<Integer>> partitionTuple2 = Maps.newHashMap();
            partitionTuple2.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(3, partitionTuple2);
            // {2={0=[2, 6, 10], 1=[0, 4, 8]}, 3={0=[3, 7, 11]}}}
            assertEquals(replicationMapping, expectedMapping);
        }

        {
            // On node 1, zone 1
            replicationMapping = adminClient.getReplicationMapping(1, newCluster, storeDef, 1);
            HashMap<Integer, HashMap<Integer, List<Integer>>> expectedMapping = Maps.newHashMap();
            HashMap<Integer, List<Integer>> partitionTuple = Maps.newHashMap();
            partitionTuple.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, partitionTuple);
            // {2={1=[1, 5, 9]}}
            assertEquals(replicationMapping, expectedMapping);
        }
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
                assertEquals("deleted partitions should be missing.",
                             0,
                             store.get(entry.getKey(), null).size());
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
        generateAndFetchFiles(2, 1, 1200, 1000);
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

    @SuppressWarnings("unchecked")
    private void generateAndFetchFiles(int numChunks, long versionId, long indexSize, long dataSize)
            throws IOException {
        Map<Integer, Set<Pair<Integer, Integer>>> buckets = RebalanceUtils.getNodeIdToAllPartitions(cluster,
                                                                                                    RebalanceUtils.getStoreDefinitionWithName(storeDefs,
                                                                                                                                              "test-readonly-fetchfiles"),
                                                                                                    true);
        for(Node node: cluster.getNodes()) {
            ReadOnlyStorageEngine store = (ReadOnlyStorageEngine) getStore(node.getId(),
                                                                           "test-readonly-fetchfiles");

            // Create list of buckets ( replica to partition )
            Set<Pair<Integer, Integer>> nodeBucketsSet = buckets.get(node.getId());
            HashMap<Integer, List<Integer>> nodeBuckets = RebalanceUtils.flattenPartitionTuples(nodeBucketsSet);

            // Split the buckets into primary and replica buckets
            HashMap<Integer, List<Integer>> primaryNodeBuckets = Maps.newHashMap();
            primaryNodeBuckets.put(0, nodeBuckets.get(0));
            int primaryPartitions = nodeBuckets.get(0).size();

            HashMap<Integer, List<Integer>> replicaNodeBuckets = Maps.newHashMap(nodeBuckets);
            replicaNodeBuckets.remove(0);

            int replicaPartitions = 0;
            for(List<Integer> partitions: replicaNodeBuckets.values()) {
                replicaPartitions += partitions.size();
            }

            // Generate data...
            File newVersionDir = new File(store.getStoreDirPath(), "version-"
                                                                   + Long.toString(versionId));
            Utils.mkdirs(newVersionDir);
            generateROFiles(numChunks, indexSize, dataSize, nodeBuckets, newVersionDir);

            // Swap it...
            store.swapFiles(newVersionDir.getAbsolutePath());

            // Check if everything got mmap-ed correctly...
            HashMap<Object, Integer> chunkIdToNumChunks = store.getChunkedFileSet()
                                                               .getChunkIdToNumChunks();
            for(Object bucket: chunkIdToNumChunks.keySet()) {
                Pair<Integer, Integer> partitionToReplicaBucket = (Pair<Integer, Integer>) bucket;
                Pair<Integer, Integer> replicaToPartitionBucket = Pair.create(partitionToReplicaBucket.getSecond(),
                                                                              partitionToReplicaBucket.getFirst());
                assertTrue(nodeBucketsSet.contains(replicaToPartitionBucket));
            }

            // Test 0) Try to fetch a partition which doesn't exist
            File tempDir = TestUtils.createTempDir();

            HashMap<Integer, List<Integer>> dumbMap = Maps.newHashMap();
            dumbMap.put(0, Lists.newArrayList(100));
            try {
                getAdminClient().fetchPartitionFiles(node.getId(),
                                                     "test-readonly-fetchfiles",
                                                     dumbMap,
                                                     tempDir.getAbsolutePath(),
                                                     null,
                                                     running);
                fail("Should throw exception since partition map passed is bad");
            } catch(VoldemortException e) {}

            // Test 1) Fetch all the primary partitions...
            tempDir = TestUtils.createTempDir();

            getAdminClient().fetchPartitionFiles(node.getId(),
                                                 "test-readonly-fetchfiles",
                                                 primaryNodeBuckets,
                                                 tempDir.getAbsolutePath(),
                                                 null,
                                                 running);

            // Check it...
            assertEquals(tempDir.list().length, 2 * primaryPartitions * numChunks + 1);

            for(Entry<Integer, List<Integer>> entry: primaryNodeBuckets.entrySet()) {
                int replicaType = entry.getKey();
                for(int partitionId: entry.getValue()) {
                    for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                        File indexFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                           + Integer.toString(replicaType) + "_"
                                                           + Integer.toString(chunkId) + ".index");
                        File dataFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                          + Integer.toString(replicaType) + "_"
                                                          + Integer.toString(chunkId) + ".data");

                        assertTrue(indexFile.exists());
                        assertTrue(dataFile.exists());
                        assertEquals(indexFile.length(), indexSize);
                        assertEquals(dataFile.length(), dataSize);
                    }

                }
            }

            // Check if metadata file exists
            File metadataFile = new File(tempDir, ".metadata");
            assertEquals(metadataFile.exists(), true);

            // Test 2) Fetch all the replica partitions...
            tempDir = TestUtils.createTempDir();

            getAdminClient().fetchPartitionFiles(node.getId(),
                                                 "test-readonly-fetchfiles",
                                                 replicaNodeBuckets,
                                                 tempDir.getAbsolutePath(),
                                                 null,
                                                 running);

            // Check it...
            assertEquals(tempDir.list().length, 2 * replicaPartitions * numChunks + 1);

            for(Entry<Integer, List<Integer>> entry: replicaNodeBuckets.entrySet()) {
                int replicaType = entry.getKey();
                for(int partitionId: entry.getValue()) {
                    for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                        File indexFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                           + Integer.toString(replicaType) + "_"
                                                           + Integer.toString(chunkId) + ".index");
                        File dataFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                          + Integer.toString(replicaType) + "_"
                                                          + Integer.toString(chunkId) + ".data");

                        assertTrue(indexFile.exists());
                        assertTrue(dataFile.exists());
                        assertEquals(indexFile.length(), indexSize);
                        assertEquals(dataFile.length(), dataSize);
                    }

                }
            }
            // Check if metadata file exists
            metadataFile = new File(tempDir, ".metadata");
            assertEquals(metadataFile.exists(), true);

            // Test 3) Fetch all the partitions...
            tempDir = TestUtils.createTempDir();
            getAdminClient().fetchPartitionFiles(node.getId(),
                                                 "test-readonly-fetchfiles",
                                                 nodeBuckets,
                                                 tempDir.getAbsolutePath(),
                                                 null,
                                                 running);

            // Check it...
            assertEquals(tempDir.list().length, 2 * (primaryPartitions + replicaPartitions)
                                                * numChunks + 1);

            for(Entry<Integer, List<Integer>> entry: nodeBuckets.entrySet()) {
                int replicaType = entry.getKey();
                for(int partitionId: entry.getValue()) {
                    for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                        File indexFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                           + Integer.toString(replicaType) + "_"
                                                           + Integer.toString(chunkId) + ".index");
                        File dataFile = new File(tempDir, Integer.toString(partitionId) + "_"
                                                          + Integer.toString(replicaType) + "_"
                                                          + Integer.toString(chunkId) + ".data");

                        assertTrue(indexFile.exists());
                        assertTrue(dataFile.exists());
                        assertEquals(indexFile.length(), indexSize);
                        assertEquals(dataFile.length(), dataSize);
                    }

                }
            }

            // Check if metadata file exists
            metadataFile = new File(tempDir, ".metadata");
            assertEquals(metadataFile.exists(), true);
        }
    }

    @Test
    public void testGetROStorageFormat() {

        Map<String, String> storesToStorageFormat = getAdminClient().getROStorageFormat(0,
                                                                                        Lists.newArrayList("test-readonly-fetchfiles",
                                                                                                           "test-readonly-versions"));
        assertEquals(storesToStorageFormat.size(), 2);
        assertEquals(storesToStorageFormat.get("test-readonly-fetchfiles"),
                     ReadOnlyStorageFormat.READONLY_V0.getCode());
        assertEquals(storesToStorageFormat.get("test-readonly-versions"),
                     ReadOnlyStorageFormat.READONLY_V0.getCode());
    }

    @Test
    public void testGetROVersions() {

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
    public void testQuery() {
        HashMap<ByteArray, byte[]> belongToAndInsideServer0 = new HashMap<ByteArray, byte[]>();
        HashMap<ByteArray, byte[]> belongToAndInsideServer1 = new HashMap<ByteArray, byte[]>();
        HashMap<ByteArray, byte[]> notBelongServer0ButInsideServer0 = new HashMap<ByteArray, byte[]>();
        HashMap<ByteArray, byte[]> belongToServer0ButOutsideBoth = new HashMap<ByteArray, byte[]>();
        HashMap<ByteArray, byte[]> notBelongToServer0AndOutsideBoth = new HashMap<ByteArray, byte[]>();

        Store<ByteArray, byte[], byte[]> store0 = getStore(0, testStoreName);
        Store<ByteArray, byte[], byte[]> store1 = getStore(1, testStoreName);

        HashMap<ByteArray, byte[]> entrySet = null;
        Iterator<ByteArray> keys = null;
        RoutingStrategy strategy = servers[0].getMetadataStore().getRoutingStrategy(testStoreName);
        while(true) {
            ByteArray key;
            byte[] value;
            if(keys == null || !keys.hasNext()) {
                entrySet = ServerTestUtils.createRandomKeyValuePairs(100);
                keys = entrySet.keySet().iterator();
            }
            key = keys.next();
            value = entrySet.get(key);
            List<Node> routedNodes = strategy.routeRequest(key.get());
            boolean keyShouldBeInNode0 = false;
            boolean keyShouldBeInNode1 = false;
            for(Node node: routedNodes) {
                keyShouldBeInNode0 = keyShouldBeInNode0 || (node.getId() == 0);
                keyShouldBeInNode1 = keyShouldBeInNode1 || (node.getId() == 1);
            }

            if(belongToAndInsideServer0.size() < 10) {
                if(keyShouldBeInNode0) {
                    belongToAndInsideServer0.put(key, value);
                    store0.put(key, new Versioned<byte[]>(value), null);
                }
            } else if(belongToAndInsideServer1.size() < 10) {
                if(keyShouldBeInNode1) {
                    belongToAndInsideServer1.put(key, value);
                    store1.put(key, new Versioned<byte[]>(value), null);
                }
            } else if(notBelongServer0ButInsideServer0.size() < 5) {
                if(!keyShouldBeInNode0) {
                    notBelongServer0ButInsideServer0.put(key, value);
                    store0.put(key, new Versioned<byte[]>(value), null);
                }
            } else if(belongToServer0ButOutsideBoth.size() < 5) {
                if(keyShouldBeInNode0) {
                    belongToServer0ButOutsideBoth.put(key, value);
                }
            } else if(notBelongToServer0AndOutsideBoth.size() < 5) {
                if(!keyShouldBeInNode0) {
                    notBelongToServer0AndOutsideBoth.put(key, value);
                }
            } else {
                break;
            }
        }

        ArrayList<ByteArray> belongToAndInsideServer0Keys = new ArrayList<ByteArray>(belongToAndInsideServer0.keySet());
        ArrayList<ByteArray> belongToAndInsideServer1Keys = new ArrayList<ByteArray>(belongToAndInsideServer1.keySet());
        ArrayList<ByteArray> notBelongServer0ButInsideServer0Keys = new ArrayList<ByteArray>(notBelongServer0ButInsideServer0.keySet());
        ArrayList<ByteArray> belongToServer0ButOutsideBothKeys = new ArrayList<ByteArray>(belongToServer0ButOutsideBoth.keySet());
        ArrayList<ByteArray> notBelongToServer0AndOutsideBothKeys = new ArrayList<ByteArray>(notBelongToServer0AndOutsideBoth.keySet());

        List<ByteArray> queryKeys;
        Iterator<Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>>> results;
        Pair<ByteArray, Pair<List<Versioned<byte[]>>, Exception>> entry;
        // test one key on store 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer0Keys.get(0));
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertEquals(queryKeys.get(0), entry.getFirst());
        assertNull("There should not be exception in response", entry.getSecond().getSecond());
        assertEquals("There should be only 1 value in versioned list", 1, entry.getSecond()
                                                                               .getFirst()
                                                                               .size());
        assertEquals("Two byte[] should be equal",
                     0,
                     ByteUtils.compare(belongToAndInsideServer0.get(queryKeys.get(0)),
                                       entry.getSecond().getFirst().get(0).getValue()));
        assertFalse("There should be only one result", results.hasNext());

        // test one key belongs to but not exists in server 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToServer0ButOutsideBothKeys.get(0));
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getFirst());
        assertNotNull("Response should be non-null", entry.getSecond());
        assertEquals("Value should be empty list", 0, entry.getSecond().getFirst().size());
        assertNull("There should not be exception", entry.getSecond().getSecond());

        // test one key not exist and does not belong to server 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(notBelongToServer0AndOutsideBothKeys.get(0));
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getFirst());
        assertNotNull("Response should be non-null", entry.getSecond());
        assertNull("Value should be null", entry.getSecond().getFirst());
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getSecond().getSecond() instanceof InvalidMetadataException);

        // test one key that exists on server 0 but does not belong to server 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(notBelongServer0ButInsideServer0Keys.get(0));
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getFirst());
        assertNotNull("Response should be non-null", entry.getSecond());
        assertNull("Value should be null", entry.getSecond().getFirst());
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getSecond().getSecond() instanceof InvalidMetadataException);

        // test one key deleted
        store0.delete(belongToAndInsideServer0Keys.get(4), null);
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer0Keys.get(4));
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getFirst());
        assertNotNull("Response should be non-null", entry.getSecond());
        assertEquals("Value should be empty list", 0, entry.getSecond().getFirst().size());
        assertNull("There should not be exception", entry.getSecond().getSecond());

        // test empty request
        queryKeys = new ArrayList<ByteArray>();
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertFalse("Results should be empty", results.hasNext());

        // test null key
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(null);
        assertEquals(1, queryKeys.size());
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertNotNull("Response should be non-null", entry.getSecond());
        assertNull("Value should be null", entry.getSecond().getFirst());
        assertTrue("There should be IllegalArgumentException exception",
                   entry.getSecond().getSecond() instanceof IllegalArgumentException);

        // test multiple keys (3) on store 1
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer1Keys.get(0));
        queryKeys.add(belongToAndInsideServer1Keys.get(1));
        queryKeys.add(belongToAndInsideServer1Keys.get(2));
        results = getAdminClient().queryKeys(1, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        Map<ByteArray, List<Versioned<byte[]>>> entries = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        int resultCount = 0;
        while(results.hasNext()) {
            resultCount++;
            entry = results.next();
            assertNull("There should not be exception in response", entry.getSecond().getSecond());
            assertNotNull("Value should not be null for Key: ", entry.getSecond().getFirst());
            entries.put(entry.getFirst(), entry.getSecond().getFirst());
        }
        assertEquals("There should 3 and only 3 results", 3, resultCount);
        for(ByteArray key: queryKeys) {
            // this loop and the count ensure one-to-one mapping
            assertNotNull("This key should exist in the results: " + key, entries.get(key));
            assertEquals("Two byte[] should be equal for key: " + key,
                         0,
                         ByteUtils.compare(belongToAndInsideServer1.get(key),
                                           entries.get(key).get(0).getValue()));
        }

        // test multiple keys, mixed situation
        // key 0: Exists and belongs to
        // key 1: Exists but does not belong to
        // key 2: Does not exist but belongs to
        // key 3: Does not belong and not exist
        // key 4: Same situation with key0
        // key 5: Deleted
        // key 6: Same situation with key2
        store0.delete(belongToAndInsideServer0Keys.get(5), null);
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer0Keys.get(2));
        queryKeys.add(notBelongServer0ButInsideServer0Keys.get(1));
        queryKeys.add(belongToServer0ButOutsideBothKeys.get(1));
        queryKeys.add(notBelongToServer0AndOutsideBothKeys.get(1));
        queryKeys.add(belongToAndInsideServer0Keys.get(3));
        queryKeys.add(belongToAndInsideServer0Keys.get(5));
        queryKeys.add(notBelongServer0ButInsideServer0Keys.get(2));
        results = getAdminClient().queryKeys(0, testStoreName, queryKeys.iterator());
        // key 0
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(0).get(), entry.getFirst().get()));
        assertEquals(0, ByteUtils.compare(belongToAndInsideServer0.get(queryKeys.get(0)),
                                          entry.getSecond().getFirst().get(0).getValue()));
        assertNull(entry.getSecond().getSecond());
        // key 1
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(1).get(), entry.getFirst().get()));
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getSecond().getSecond() instanceof InvalidMetadataException);
        // key 2
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(2).get(), entry.getFirst().get()));
        assertEquals(0, entry.getSecond().getFirst().size());
        assertNull(entry.getSecond().getSecond());
        // key 3
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(3).get(), entry.getFirst().get()));
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getSecond().getSecond() instanceof InvalidMetadataException);
        // key 4
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(4).get(), entry.getFirst().get()));
        assertEquals(0, ByteUtils.compare(belongToAndInsideServer0.get(queryKeys.get(4)),
                                          entry.getSecond().getFirst().get(0).getValue()));
        assertNull(entry.getSecond().getSecond());
        // key 5
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(5).get(), entry.getFirst().get()));
        assertEquals(0, entry.getSecond().getFirst().size());
        assertNull(entry.getSecond().getSecond());
        // key 6
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(6).get(), entry.getFirst().get()));
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getSecond().getSecond() instanceof InvalidMetadataException);
        // no more keys
        assertFalse(results.hasNext());
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
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }
        }
    }

    @Test
    public void testRecoverData() {
        // use store with replication 2, required write 2 for this test.
        String testStoreName = "test-recovery-data";

        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(5);
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
     * Tests the basic RW fetch and update
     */
    @Test
    public void testFetchAndUpdateRW() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> primaryMoved = Arrays.asList(0, 2);
        List<Integer> secondaryMoved = Arrays.asList(1, 4);

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(cluster, 1, primaryMoved);
        HashMap<Integer, List<Integer>> replicaToPartitions = Maps.newHashMap();
        replicaToPartitions.put(0, primaryMoved);
        replicaToPartitions.put(1, secondaryMoved);

        HashMap<ByteArray, byte[]> keysMoved = Maps.newHashMap();

        // insert it into server-0 store
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(RebalanceUtils.getStoreDefinitionWithName(storeDefs,
                                                                                                                                "test-recovery-data"),
                                                                                      cluster);

        Store<ByteArray, byte[], byte[]> store0 = getStore(0, "test-recovery-data");
        Store<ByteArray, byte[], byte[]> store1 = getStore(1, "test-recovery-data");
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store0.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()), null);
            List<Integer> partitions = strategy.getPartitionList(entry.getKey().get());
            if(primaryMoved.contains(partitions.get(0))
               || (secondaryMoved.contains(partitions.get(0)) && cluster.getNodeById(0)
                                                                        .getPartitionIds()
                                                                        .contains(partitions.get(1)))) {
                keysMoved.put(entry.getKey(), entry.getValue());
            }
        }

        // Assert that server1 is empty.
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet())
            assertEquals("server1 should be empty at start.", 0, store1.get(entry.getKey(), null)
                                                                       .size());

        // Set some other metadata, so as to pick the right up later
        getServer(0).getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);

        // Migrate the partition
        AdminClient client = getAdminClient();
        int id = client.migratePartitions(0,
                                          1,
                                          "test-recovery-data",
                                          replicaToPartitions,
                                          null,
                                          cluster,
                                          false);
        client.waitForCompletion(1, id, 120, TimeUnit.SECONDS);

        // Check the values
        for(Entry<ByteArray, byte[]> entry: keysMoved.entrySet()) {
            assertEquals("server1 store should contain fetchAndupdated partitions.",
                         1,
                         store1.get(entry.getKey(), null).size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store1.get(entry.getKey(), null).get(0).getValue()));
        }

    }

}
