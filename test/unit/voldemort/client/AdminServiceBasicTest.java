/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

import voldemort.ROTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.StringSerializer;
import voldemort.server.RequestRoutingType;
import voldemort.server.VoldemortServer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.quota.QuotaType;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.UpdateClusterUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
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

    private static final String STORE_NAME = "test-basic-replication-memory";

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

    private StoreClient<String, String> storeClient;

    private final boolean useNio;

    private final boolean onlineRetention;

    public AdminServiceBasicTest(boolean useNio, boolean onlineRetention) {
        this.useNio = useNio;
        this.onlineRetention = onlineRetention;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, false }, { true, true }, { false, false },
                { false, true } });
    }

    @Before
    public void setUp() throws IOException {
        int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };
        Properties serverProperties = new Properties();
        serverProperties.setProperty("client.max.connections.per.node", "20");
        serverProperties.setProperty("enforce.retention.policy.on.read",
                                     Boolean.toString(onlineRetention));
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
        adminClient = new AdminClient(cluster,
                                      new AdminClientConfig(adminProperties),
                                      new ClientConfig());

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        storeClient = storeClientFactory.getStoreClient(STORE_NAME);

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
        adminClient.close();
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
            VectorClock clock = ((VectorClock) client.metadataMgmtOps.getRemoteCluster(0)
                                                                     .getVersion()).incremented(0,
                                                                                                System.currentTimeMillis());
            client.metadataMgmtOps.updateRemoteCluster(0, updatedCluster, clock);

            assertEquals("Cluster should match",
                         updatedCluster,
                         getVoldemortServer(0).getMetadataStore().getCluster());
            assertEquals("AdminClient.getMetdata() should match",
                         client.metadataMgmtOps.getRemoteCluster(0).getValue(),
                         updatedCluster);

            // version should match
            assertEquals("versions should match as well.",
                         clock,
                         client.metadataMgmtOps.getRemoteCluster(0).getVersion());
        }

    }

    /**
     * Function to return the String representation of a Metadata key
     * (stores.xml or an individual store)
     * 
     * @param key specifies the metadata key to retrieve
     * @return String representation of the value associated with the metadata
     *         key
     */
    private String bootstrapMetadata(String metadataKey) {
        Node serverNode = servers[0].getIdentityNode();
        Store<ByteArray, byte[], byte[]> remoteStore = socketStoreFactory.create(MetadataStore.METADATA_STORE_NAME,
                                                                                 serverNode.getHost(),
                                                                                 serverNode.getSocketPort(),
                                                                                 RequestFormatType.VOLDEMORT_V2,
                                                                                 RequestRoutingType.NORMAL);
        Store<String, String, byte[]> store = SerializingStore.wrap(remoteStore,
                                                                    new StringSerializer("UTF-8"),
                                                                    new StringSerializer("UTF-8"),
                                                                    new IdentitySerializer());

        List<Versioned<String>> found = store.get(metadataKey, null);

        assertEquals(found.size(), 1);
        String valueStr = found.get(0).getValue();
        return valueStr;
    }

    /**
     * Function to retrieve the set of store names contained in the given list
     * of store definitions. This is used for comparing two store lists by only
     * their names
     * 
     * @param defs list of store definitions
     * @return set of store names contained in the given list of store
     *         definitions
     */
    private Set<String> getStoreNames(List<StoreDefinition> defs) {
        Set<String> storeNameSet = new HashSet<String>();
        for(StoreDefinition def: defs) {
            storeNameSet.add(def.getName());
        }
        return storeNameSet;
    }

    private void doClientOperation() {
        for(int i = 0; i < 100; i++) {
            String key = "key-" + System.currentTimeMillis();
            String value = "Value for " + key;
            this.storeClient.put(key, value);

            String returnedValue = this.storeClient.getValue(key);
            assertEquals(returnedValue, value);
        }
    }

    @Test
    public void testFetchSingleStoreFromMetadataStore() throws Exception {
        String storeName = "test-replication-memory";
        String storeDefStr = bootstrapMetadata(storeName);

        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefList = mapper.readStoreList(new StringReader(storeDefStr));
        assertEquals(storeDefList.size(), 1);

        StoreDefinition storeDef = storeDefList.get(0);
        assertEquals(storeDef.getName(), storeName);
    }

    @Test
    public void testFetchAllStoresFromMetadataStore() throws Exception {
        String storeName = MetadataStore.STORES_KEY;
        String storeDefStr = bootstrapMetadata(storeName);

        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefList = mapper.readStoreList(new StringReader(storeDefStr));
        assertEquals(storeDefList.size(), this.storeDefs.size());

        Set<String> receivedStoreNames = getStoreNames(storeDefList);
        Set<String> originalStoreNames = getStoreNames(this.storeDefs);
        assertEquals(receivedStoreNames, originalStoreNames);
    }

    /**
     * Function to update the given stores and then reset the stores.xml back to
     * its original state. This is used for confirming that the updates only
     * affect the specified stores in the server. Rest of the stores remain
     * untouched.
     * 
     * @param storesToBeUpdatedList specifies list of stores to be updated
     */
    private void updateAndResetStoreDefinitions(List<StoreDefinition> storesToBeUpdatedList) {

        // Track the names of the stores to be updated
        Set<String> storesNamesToBeUpdated = getStoreNames(storesToBeUpdatedList);

        // Keep track of the original store definitions for the specific stores
        // about to be updated
        List<StoreDefinition> originalStoreDefinitionsList = new ArrayList<StoreDefinition>();
        for(StoreDefinition def: this.storeDefs) {
            if(storesNamesToBeUpdated.contains(def.getName())) {
                originalStoreDefinitionsList.add(def);
            }
        }

        // Update the definitions on all the nodes
        AdminClient adminClient = getAdminClient();
        adminClient.metadataMgmtOps.updateRemoteStoreDefList(storesToBeUpdatedList);

        // Retrieve stores list and check that other definitions are unchanged
        String allStoresDefStr = bootstrapMetadata(MetadataStore.STORES_KEY);
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefList = mapper.readStoreList(new StringReader(allStoresDefStr));
        assertEquals(storeDefList.size(), this.storeDefs.size());

        // Insert original stores in the map
        Map<String, StoreDefinition> storeNameToDefMap = new HashMap<String, StoreDefinition>();
        for(StoreDefinition def: this.storeDefs) {
            storeNameToDefMap.put(def.getName(), def);
        }

        // Now validate the received definitions. Only the updated store
        // definition should be different. Everything else should be as is
        for(StoreDefinition def: storeDefList) {
            if(!storesNamesToBeUpdated.contains(def.getName())) {
                assertEquals(def, storeNameToDefMap.get(def.getName()));
            }
        }

        // Reset the store definition back to original
        adminClient.metadataMgmtOps.updateRemoteStoreDefList(originalStoreDefinitionsList,
                                                             this.cluster.getNodeIds());

    }

    @Test
    public void testFetchSingleStoreFromAdminClient() {
        String storeName = "test-replication-memory";
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();

        for(int nodeId: this.cluster.getNodeIds()) {
            Versioned<String> storeDef = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                       storeName);
            List<StoreDefinition> def = mapper.readStoreList(new StringReader(storeDef.getValue()));
            assertEquals(def.get(0).getName(), storeName);
        }
    }

    @Test
    public void testUpdateSingleStore() {

        doClientOperation();

        // Create a store definition for an existing store with a different
        // replication factor
        List<StoreDefinition> storesToBeUpdatedList = new ArrayList<StoreDefinition>();
        String storeName = "test-replication-memory";
        StoreDefinition definitionNew = new StoreDefinitionBuilder().setName(storeName)
                                                                    .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                    .setKeySerializer(new SerializerDefinition("string"))
                                                                    .setValueSerializer(new SerializerDefinition("string"))
                                                                    .setRoutingPolicy(RoutingTier.CLIENT)
                                                                    .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                    .setReplicationFactor(2)
                                                                    .setPreferredReads(1)
                                                                    .setRequiredReads(1)
                                                                    .setPreferredWrites(1)
                                                                    .setRequiredWrites(1)
                                                                    .build();
        storesToBeUpdatedList.add(definitionNew);
        updateAndResetStoreDefinitions(storesToBeUpdatedList);

        doClientOperation();
    }

    @Test
    public void testUpdateMultipleStores() {

        doClientOperation();

        // Create store definitions for existing stores with a different
        // replication factor
        List<StoreDefinition> storesToBeUpdatedList = new ArrayList<StoreDefinition>();
        StoreDefinition definition1 = new StoreDefinitionBuilder().setName("test-replication-memory")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                  .setReplicationFactor(2)
                                                                  .setPreferredReads(1)
                                                                  .setRequiredReads(1)
                                                                  .setPreferredWrites(1)
                                                                  .setRequiredWrites(1)
                                                                  .build();

        StoreDefinition definition2 = new StoreDefinitionBuilder().setName("test-recovery-data")
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

        StoreDefinition definition3 = new StoreDefinitionBuilder().setName("test-basic-replication-memory")
                                                                  .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                  .setReplicationFactor(2)
                                                                  .setPreferredReads(2)
                                                                  .setRequiredReads(2)
                                                                  .setPreferredWrites(2)
                                                                  .setRequiredWrites(2)
                                                                  .build();
        storesToBeUpdatedList.add(definition1);
        storesToBeUpdatedList.add(definition2);
        storesToBeUpdatedList.add(definition3);
        updateAndResetStoreDefinitions(storesToBeUpdatedList);

        doClientOperation();
    }

    @Test
    public void testFetchAndUpdateStoresMetadata() {
        AdminClient client = getAdminClient();
        int nodeId = 0;
        String storeNameToBeUpdated = "users";

        doClientOperation();

        // Fetch the original list of stores
        Versioned<List<StoreDefinition>> originalStoreDefinitions = client.metadataMgmtOps.getRemoteStoreDefList(nodeId);
        List<StoreDefinition> updatedStoreDefList = new ArrayList<StoreDefinition>();

        // Create an updated store definition for store: 'users'
        StoreDefinition newDefinition = new StoreDefinitionBuilder().setName(storeNameToBeUpdated)
                                                                    .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                    .setKeySerializer(new SerializerDefinition("string"))
                                                                    .setValueSerializer(new SerializerDefinition("string"))
                                                                    .setRoutingPolicy(RoutingTier.CLIENT)
                                                                    .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                    .setReplicationFactor(2)
                                                                    .setPreferredReads(1)
                                                                    .setRequiredReads(1)
                                                                    .setPreferredWrites(2)
                                                                    .setRequiredWrites(2)
                                                                    .build();

        updatedStoreDefList.add(newDefinition);

        // Update the 'users' store
        client.metadataMgmtOps.fetchAndUpdateRemoteStore(nodeId, updatedStoreDefList);

        // Fetch the stores list again and check that the 'users' store has been
        // updated
        Versioned<List<StoreDefinition>> newStoreDefinitions = client.metadataMgmtOps.getRemoteStoreDefList(nodeId);
        assertFalse(originalStoreDefinitions.getValue().equals(newStoreDefinitions.getValue()));

        for(StoreDefinition def: newStoreDefinitions.getValue()) {
            if(def.getName().equalsIgnoreCase(storeNameToBeUpdated)) {
                assertTrue(def.equals(newDefinition));
            }
        }

        // Restore the old set of store definitions
        client.metadataMgmtOps.updateRemoteStoreDefList(nodeId, originalStoreDefinitions.getValue());

        doClientOperation();
    }

    @Test
    public void testAddStore() throws Exception {
        AdminClient adminClient = getAdminClient();

        doClientOperation();

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
            adminClient.storeMgmtOps.addStore(definition);
            fail("Should have thrown an exception because we cannot add a store with a replication factor greater than number of nodes");
        } catch(Exception e) {}

        // Try adding a legit store using inmemorystorage engine
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
        adminClient.storeMgmtOps.addStore(definition);

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
        Versioned<List<StoreDefinition>> list = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
        assertTrue(list.getValue().contains(definition));

        // Now add a RO store
        definition = new StoreDefinitionBuilder().setName("addStoreROFormatTest")
                                                 .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
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

        adminClient.storeMgmtOps.addStore(definition);

        // Retrieve list of read-only stores
        List<String> storeNames = Lists.newArrayList();
        for(StoreDefinition storeDef: adminClient.metadataMgmtOps.getRemoteStoreDefList(0)
                                                                 .getValue()) {
            if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
                storeNames.add(storeDef.getName());
            }
        }

        Map<String, String> storeToStorageFormat = adminClient.readonlyOps.getROStorageFormat(0,
                                                                                              storeNames);
        for(String storeName: storeToStorageFormat.keySet()) {
            assertEquals(storeToStorageFormat.get(storeName), "ro2");
        }

        doClientOperation();
    }

    @Test
    public void testReplicationMapping() {
        List<Zone> zones = ServerTestUtils.getZones(2);

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
        Cluster newCluster = new Cluster("single_zone_cluster", nodes, zones);

        try {
            adminClient.replicaOps.getReplicationMapping(0, newCluster, storeDef);
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
        Map<Integer, List<Integer>> replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                                                      newCluster,
                                                                                                      storeDef);
        {
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 1
            replicationMapping = adminClient.replicaOps.getReplicationMapping(1,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(2, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 2
            replicationMapping = adminClient.replicaOps.getReplicationMapping(2,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(3, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }
        {
            // On node 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(2, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        // Test 2 - With zone routing strategy
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
            replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(2, Lists.newArrayList(0, 4, 8, 2, 6, 10));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }
        {
            // On node 1
            replicationMapping = adminClient.replicaOps.getReplicationMapping(1,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(2, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 2
            replicationMapping = adminClient.replicaOps.getReplicationMapping(2,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8, 2, 6, 10));
            expectedMapping.put(1, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        // Test 3 - Consistent with rep factor 3
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               3,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        newCluster = new Cluster("single_zone_cluster", nodes, zones);

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(1, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(2, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(1,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(2, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(2,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(3, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
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
            replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(1, Lists.newArrayList(0, 4, 8, 1, 5, 9));
            expectedMapping.put(2, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(1,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(2, Lists.newArrayList(1, 5, 9, 2, 6, 10));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(2,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(3, Lists.newArrayList(2, 6, 10, 3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(0, Lists.newArrayList(0, 4, 8, 3, 7, 11));
            expectedMapping.put(1, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(2, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }
    }

    @Test
    public void testReplicationMappingWithNonContiguousZones() {
        int[] zoneIds = { 1, 3 };
        List<Zone> zones = ServerTestUtils.getZonesFromZoneIds(zoneIds);

        List<Node> nodes = Lists.newArrayList();
        nodes.add(new Node(3, "localhost", 1, 2, 3, 1, Lists.newArrayList(0, 4, 8)));
        nodes.add(new Node(4, "localhost", 1, 2, 3, 1, Lists.newArrayList(1, 5, 9)));
        nodes.add(new Node(5, "localhost", 1, 2, 3, 3, Lists.newArrayList(2, 6, 10)));
        nodes.add(new Node(6, "localhost", 1, 2, 3, 3, Lists.newArrayList(3, 7, 11)));

        // Node 3 - With rep-factor 1
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("consistent",
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Cluster newCluster = new Cluster("single_zone_cluster", nodes, zones);

        try {
            adminClient.replicaOps.getReplicationMapping(3, newCluster, storeDef);
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

        // On node 3
        Map<Integer, List<Integer>> replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                                                      newCluster,
                                                                                                      storeDef);
        {
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(4, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 4
            replicationMapping = adminClient.replicaOps.getReplicationMapping(4,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(5, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 5
            replicationMapping = adminClient.replicaOps.getReplicationMapping(5,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(4, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(6, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }
        {
            // On node 6
            replicationMapping = adminClient.replicaOps.getReplicationMapping(6,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(5, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        // Test 2 - With zone routing strategy
        HashMap<Integer, Integer> zoneReplicationFactors = Maps.newHashMap();
        for(int index = 0; index < zoneIds.length; index++) {
            zoneReplicationFactors.put(zoneIds[index], 1);
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
            // On node 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(5, Lists.newArrayList(0, 4, 8, 2, 6, 10));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }
        {
            // On node 4
            replicationMapping = adminClient.replicaOps.getReplicationMapping(4,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(5, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 5
            replicationMapping = adminClient.replicaOps.getReplicationMapping(5,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8, 2, 6, 10));
            expectedMapping.put(4, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 6
            replicationMapping = adminClient.replicaOps.getReplicationMapping(6,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        // Test 3 - Consistent with rep factor 3
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               3,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        newCluster = new Cluster("single_zone_cluster", nodes, zones);

        {
            // On node 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(4, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(5, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 4
            replicationMapping = adminClient.replicaOps.getReplicationMapping(4,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(5, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 5
            replicationMapping = adminClient.replicaOps.getReplicationMapping(5,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(4, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(6, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 6
            replicationMapping = adminClient.replicaOps.getReplicationMapping(6,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            expectedMapping.put(4, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(5, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }

        zoneReplicationFactors = Maps.newHashMap();
        for(int index = 0; index < zoneIds.length; index++) {
            zoneReplicationFactors.put(zoneIds[index], 2);
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
            // On node 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(4, Lists.newArrayList(0, 4, 8, 1, 5, 9));
            expectedMapping.put(5, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 4
            replicationMapping = adminClient.replicaOps.getReplicationMapping(4,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(5, Lists.newArrayList(1, 5, 9, 2, 6, 10));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 5
            replicationMapping = adminClient.replicaOps.getReplicationMapping(5,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8));
            expectedMapping.put(4, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(6, Lists.newArrayList(2, 6, 10, 3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 6
            replicationMapping = adminClient.replicaOps.getReplicationMapping(6,
                                                                              newCluster,
                                                                              storeDef);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(3, Lists.newArrayList(0, 4, 8, 3, 7, 11));
            expectedMapping.put(4, Lists.newArrayList(1, 5, 9));
            expectedMapping.put(5, Lists.newArrayList(2, 6, 10));
            assertEquals(expectedMapping, replicationMapping);
        }
    }

    @Test
    public void testReplicationMappingWithZonePreference() {
        List<Zone> zones = ServerTestUtils.getZones(2);

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
        Cluster newCluster = new Cluster("single_zone_cluster", nodes, zones);

        try {
            adminClient.replicaOps.getReplicationMapping(0, newCluster, storeDef, 1);
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
        newCluster = new Cluster("single_zone_cluster", nodes, zones);

        try {
            adminClient.replicaOps.getReplicationMapping(0, newCluster, storeDef, 0);
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
        Map<Integer, List<Integer>> replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                                                      newCluster,
                                                                                                      storeDef,
                                                                                                      1);
        {
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(2, Lists.newArrayList(0, 4, 8, 1, 5, 9, 2, 6, 10));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        // On node 0; zone id 0
        replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                          newCluster,
                                                                          storeDef,
                                                                          0);
        {
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            // partitionTuple.put(1, Lists.newArrayList(0, 4, 8));
            // partitionTuple.put(2, Lists.newArrayList(3, 7, 11));
            // partitionTuple.put(3, Lists.newArrayList(2, 6, 10));
            expectedMapping.put(1, Lists.newArrayList(0, 4, 8, 1, 5, 9, 2, 6, 10, 3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        // Test 2 - With zone routing strategy, and zone replication factor 1
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
                replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                                  newCluster,
                                                                                  storeDef,
                                                                                  0);
                fail("Should have thrown an exception since  zoneReplicationFactor is 1");
            } catch(VoldemortException e) {}
        }

        {
            // On node 0, zone 1
            replicationMapping = adminClient.replicaOps.getReplicationMapping(0,
                                                                              newCluster,
                                                                              storeDef,
                                                                              1);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(2, Lists.newArrayList(0, 4, 8, 2, 6, 10));
            expectedMapping.put(3, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 1, zone 1
            replicationMapping = adminClient.replicaOps.getReplicationMapping(1,
                                                                              newCluster,
                                                                              storeDef,
                                                                              1);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(2, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }
    }

    @Test
    public void testReplicationMappingWithZonePreferenceWithNonContiguousZones() {

        int[] zoneIds = { 1, 3 };
        List<Zone> zones = ServerTestUtils.getZonesFromZoneIds(zoneIds);

        List<Node> nodes = Lists.newArrayList();
        nodes.add(new Node(3, "localhost", 1, 2, 3, 1, Lists.newArrayList(0, 4, 8)));
        nodes.add(new Node(4, "localhost", 1, 2, 3, 1, Lists.newArrayList(1, 5, 9)));
        nodes.add(new Node(5, "localhost", 1, 2, 3, 3, Lists.newArrayList(2, 6, 10)));
        nodes.add(new Node(6, "localhost", 1, 2, 3, 3, Lists.newArrayList(3, 7, 11)));

        // Node 3 - With rep-factor 1; zone 1
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("consistent",
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Cluster newCluster = new Cluster("single_zone_cluster", nodes, zones);

        try {
            adminClient.replicaOps.getReplicationMapping(3, newCluster, storeDef, 1);
            fail("Should have thrown an exception since rep-factor = 1");
        } catch(VoldemortException e) {}

        // With rep-factor 1; zone 1
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               1,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        newCluster = new Cluster("single_zone_cluster", nodes, zones);

        try {
            adminClient.replicaOps.getReplicationMapping(3, newCluster, storeDef, 1);
            fail("Should have thrown an exception since rep-factor = 1");
        } catch(VoldemortException e) {}

        // Node 1 - With consistent routing strategy
        storeDef = ServerTestUtils.getStoreDef("consistent",
                                               4,
                                               1,
                                               1,
                                               1,
                                               1,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        // On node 3; zone id 1
        Map<Integer, List<Integer>> replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                                                      newCluster,
                                                                                                      storeDef,
                                                                                                      1);
        {
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(4, Lists.newArrayList(0, 4, 8, 1, 5, 9, 2, 6, 10, 3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        // On node 3; zone id 1
        replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                          newCluster,
                                                                          storeDef,
                                                                          1);
        {
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(4, Lists.newArrayList(0, 4, 8, 1, 5, 9, 2, 6, 10, 3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        // Test 2 - With zone routing strategy, and zone replication factor 1
        HashMap<Integer, Integer> zoneReplicationFactors = Maps.newHashMap();
        for(int index = 0; index < zoneIds.length; index++) {
            zoneReplicationFactors.put(zoneIds[index], 1);
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
            // On node 3, zone 1 - failure case since zoneReplicationFactor is 1

            try {
                replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                                  newCluster,
                                                                                  storeDef,
                                                                                  1);
                fail("Should have thrown an exception since  zoneReplicationFactor is 1");
            } catch(VoldemortException e) {}
        }

        {
            // On node 3, zone 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(3,
                                                                              newCluster,
                                                                              storeDef,
                                                                              3);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(5, Lists.newArrayList(0, 4, 8, 2, 6, 10));
            expectedMapping.put(6, Lists.newArrayList(3, 7, 11));
            assertEquals(expectedMapping, replicationMapping);
        }

        {
            // On node 4, zone 3
            replicationMapping = adminClient.replicaOps.getReplicationMapping(4,
                                                                              newCluster,
                                                                              storeDef,
                                                                              3);
            HashMap<Integer, List<Integer>> expectedMapping = Maps.newHashMap();
            expectedMapping.put(5, Lists.newArrayList(1, 5, 9));
            assertEquals(expectedMapping, replicationMapping);
        }
    }

    @Test
    public void testDeleteStore() throws Exception {
        AdminClient adminClient = getAdminClient();

        doClientOperation();

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
        adminClient.storeMgmtOps.addStore(definition);

        // now test the store
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setCacheStoreClients(false)
                                                                                    .setBootstrapUrls(cluster.getNodeById(0)
                                                                                                             .getSocketUrl()
                                                                                                             .toString()));

        StoreClient<Object, Object> client = factory.getStoreClient("deleteTest");

        int numStores = adminClient.metadataMgmtOps.getRemoteStoreDefList(0).getValue().size();

        // delete the store
        assertEquals(adminClient.metadataMgmtOps.getRemoteStoreDefList(0)
                                                .getValue()
                                                .contains(definition),
                     true);
        adminClient.storeMgmtOps.deleteStore("deleteTest");
        assertEquals(adminClient.metadataMgmtOps.getRemoteStoreDefList(0).getValue().size(),
                     numStores - 1);
        assertEquals(adminClient.metadataMgmtOps.getRemoteStoreDefList(0)
                                                .getValue()
                                                .contains(definition),
                     false);

        // test with deleted store
        // (Turning off store client caching above will ensures the new client
        // will attempt to bootstrap)
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

        doClientOperation();

        // try adding the store again
        adminClient.storeMgmtOps.addStore(definition);

        client = factory.getStoreClient("deleteTest");
        client.put("abc", "123");
        String s = (String) client.get("abc").getValue();
        assertEquals(s, "123");

        doClientOperation();
    }

    /**
     * Update the server state (
     * {@link voldemort.store.metadata.MetadataStore.VoldemortState}) on a
     * remote node.
     * 
     * @param nodeId The node id on which we want to update the state
     * @param state The state to update to
     * @param clock The vector clock
     */
    private void updateRemoteServerState(AdminClient client,
                                         int nodeId,
                                         MetadataStore.VoldemortState state,
                                         Version clock) {
        client.metadataMgmtOps.updateRemoteMetadata(nodeId,
                                                    MetadataStore.SERVER_STATE_KEY,
                                                    new Versioned<String>(state.toString(), clock));
    }

    @Test
    public void testStateTransitions() {
        // change to REBALANCING STATE
        AdminClient client = getAdminClient();
        updateRemoteServerState(client,
                                getVoldemortServer(0).getIdentityNode().getId(),
                                MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                                ((VectorClock) client.rebalanceOps.getRemoteServerState(0)
                                                                  .getVersion()).incremented(0,
                                                                                             System.currentTimeMillis()));

        MetadataStore.VoldemortState state = getVoldemortServer(0).getMetadataStore()
                                                                  .getServerStateUnlocked();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                     state);

        // change back to NORMAL state
        updateRemoteServerState(client,
                                getVoldemortServer(0).getIdentityNode().getId(),
                                MetadataStore.VoldemortState.NORMAL_SERVER,
                                ((VectorClock) client.rebalanceOps.getRemoteServerState(0)
                                                                  .getVersion()).incremented(0,
                                                                                             System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerStateUnlocked();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);

        // lets revert back to REBALANCING STATE AND CHECK
        updateRemoteServerState(client,
                                getVoldemortServer(0).getIdentityNode().getId(),
                                MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                                ((VectorClock) client.rebalanceOps.getRemoteServerState(0)
                                                                  .getVersion()).incremented(0,
                                                                                             System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerStateUnlocked();

        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                     state);

        // change back to NORMAL_SERVER
        updateRemoteServerState(client,
                                getVoldemortServer(0).getIdentityNode().getId(),
                                MetadataStore.VoldemortState.NORMAL_SERVER,
                                ((VectorClock) client.rebalanceOps.getRemoteServerState(0)
                                                                  .getVersion()).incremented(0,
                                                                                             System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerStateUnlocked();
        assertEquals("State should be changed correctly to normal state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);

        // change to OFFLINE_SERVER
        client.metadataMgmtOps.setRemoteOfflineState(getVoldemortServer(0).getIdentityNode()
                                                                          .getId(), true);

        state = getVoldemortServer(0).getMetadataStore().getServerStateUnlocked();
        assertEquals("State should be changed correctly to offline state",
                     MetadataStore.VoldemortState.OFFLINE_SERVER,
                     state);

        // change back to NORMAL_SERVER
        client.metadataMgmtOps.setRemoteOfflineState(getVoldemortServer(0).getIdentityNode()
                                                                          .getId(), false);

        state = getVoldemortServer(0).getMetadataStore().getServerStateUnlocked();
        assertEquals("State should be changed correctly to normal state",
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
        getAdminClient().storeMntOps.deletePartitions(0, testStoreName, deletePartitionsList, null);

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

        Iterator<ByteArray> fetchIt = getAdminClient().bulkFetchOps.fetchKeys(0,
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
                    String fileName = Integer.toString(partitionId) + "_"
                                      + Integer.toString(replicaType) + "_"
                                      + Integer.toString(chunkId);
                    File index = new File(versionDir, fileName + ".index");
                    File data = new File(versionDir, fileName + ".data");
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
        Map<Integer, Set<Pair<Integer, Integer>>> buckets = ROTestUtils.getNodeIdToAllPartitions(cluster,
                                                                                                 StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                                                                                 "test-readonly-fetchfiles"),
                                                                                                 true);
        for(Node node: cluster.getNodes()) {
            ReadOnlyStorageEngine store = (ReadOnlyStorageEngine) getStore(node.getId(),
                                                                           "test-readonly-fetchfiles");

            // Create list of buckets ( replica to partition )
            Set<Pair<Integer, Integer>> nodeBucketsSet = buckets.get(node.getId());
            HashMap<Integer, List<Integer>> nodeBuckets = ROTestUtils.flattenPartitionTuples(nodeBucketsSet);

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
                getAdminClient().readonlyOps.fetchPartitionFiles(node.getId(),
                                                                 "test-readonly-fetchfiles",
                                                                 Lists.newArrayList(100),
                                                                 tempDir.getAbsolutePath(),
                                                                 null,
                                                                 running);
                fail("Should throw exception since partition map passed is bad");
            } catch(VoldemortException e) {}

            // Test 1) Fetch all the primary partitions...
            tempDir = TestUtils.createTempDir();

            getAdminClient().readonlyOps.fetchPartitionFiles(node.getId(),
                                                             "test-readonly-fetchfiles",
                                                             primaryNodeBuckets.get(0),
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

            for(Entry<Integer, List<Integer>> entry: replicaNodeBuckets.entrySet()) {
                getAdminClient().readonlyOps.fetchPartitionFiles(node.getId(),
                                                                 "test-readonly-fetchfiles",
                                                                 entry.getValue(),
                                                                 tempDir.getAbsolutePath(),
                                                                 null,
                                                                 running);
            }

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
            for(Entry<Integer, List<Integer>> entry: nodeBuckets.entrySet()) {
                getAdminClient().readonlyOps.fetchPartitionFiles(node.getId(),
                                                                 "test-readonly-fetchfiles",
                                                                 entry.getValue(),
                                                                 tempDir.getAbsolutePath(),
                                                                 null,
                                                                 running);
            }

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

            // testGetROStorageFileList
            List<String> fileList = getAdminClient().readonlyOps.getROStorageFileList(node.getId(),
                                                                                      "test-readonly-fetchfiles");
            int fileCount = 0;
            for(Entry<Integer, List<Integer>> entry: nodeBuckets.entrySet()) {
                int replicaType = entry.getKey();
                for(int partitionId: entry.getValue()) {
                    for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                        String fileName = Integer.toString(partitionId) + "_"
                                          + Integer.toString(replicaType) + "_"
                                          + Integer.toString(chunkId);
                        assertTrue("Assuming file exists:" + fileName, fileList.contains(fileName));
                        fileCount++;
                    }
                }
            }
            assertEquals(fileCount, fileList.size());
        }
    }

    @Test
    public void testGetROStorageFormat() {

        Map<String, String> storesToStorageFormat = getAdminClient().readonlyOps.getROStorageFormat(0,
                                                                                                    Lists.newArrayList("test-readonly-fetchfiles",
                                                                                                                       "test-readonly-versions"));
        assertEquals(storesToStorageFormat.size(), 2);
        assertEquals(storesToStorageFormat.get("test-readonly-fetchfiles"),
                     ReadOnlyStorageFormat.READONLY_V2.getCode());
        assertEquals(storesToStorageFormat.get("test-readonly-versions"),
                     ReadOnlyStorageFormat.READONLY_V2.getCode());
    }

    @Test
    public void testGetROVersions() {

        // Tests get current version
        Map<String, Long> storesToVersions = getAdminClient().readonlyOps.getROCurrentVersion(0,
                                                                                              Lists.newArrayList("test-readonly-fetchfiles",
                                                                                                                 "test-readonly-versions"));
        assertEquals(storesToVersions.size(), 2);
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);
        assertEquals(storesToVersions.get("test-readonly-versions").longValue(), 0);

        // Tests get maximum version
        storesToVersions = getAdminClient().readonlyOps.getROMaxVersion(0,
                                                                        Lists.newArrayList("test-readonly-fetchfiles",
                                                                                           "test-readonly-versions"));
        assertEquals(storesToVersions.size(), 2);
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);
        assertEquals(storesToVersions.get("test-readonly-versions").longValue(), 0);

        // Tests global get maximum versions
        storesToVersions = getAdminClient().readonlyOps.getROMaxVersion(Lists.newArrayList("test-readonly-fetchfiles",
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
        storesToVersions = getAdminClient().readonlyOps.getROCurrentVersion(0,
                                                                            Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 0);

        // Test max version
        storesToVersions = getAdminClient().readonlyOps.getROMaxVersion(0,
                                                                        Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 10);

        // Node 1
        // Test current version
        storesToVersions = getAdminClient().readonlyOps.getROCurrentVersion(1,
                                                                            Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 11);

        // Test max version
        storesToVersions = getAdminClient().readonlyOps.getROMaxVersion(1,
                                                                        Lists.newArrayList("test-readonly-fetchfiles"));
        assertEquals(storesToVersions.get("test-readonly-fetchfiles").longValue(), 11);

        // Test global max
        storesToVersions = getAdminClient().readonlyOps.getROMaxVersion(Lists.newArrayList("test-readonly-fetchfiles",
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
        getAdminClient().storeMntOps.truncate(0, testStoreName);

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

        Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchIt = getAdminClient().bulkFetchOps.fetchEntries(0,
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
        Iterator<QueryKeyResult> results;
        QueryKeyResult entry;
        // test one key on store 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer0Keys.get(0));
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertEquals(queryKeys.get(0), entry.getKey());
        assertNull("There should not be exception in response", entry.getException());
        assertEquals("There should be only 1 value in versioned list", 1, entry.getValues().size());
        assertEquals("Two byte[] should be equal",
                     0,
                     ByteUtils.compare(belongToAndInsideServer0.get(queryKeys.get(0)),
                                       entry.getValues().get(0).getValue()));
        assertFalse("There should be only one result", results.hasNext());

        // test one key belongs to but not exists in server 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToServer0ButOutsideBothKeys.get(0));
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getKey());
        assertFalse("There should not be exception", entry.hasException());
        assertTrue("There should be values", entry.hasValues());
        assertNotNull("Response should be non-null", entry.getValues());
        assertEquals("Value should be empty list", 0, entry.getValues().size());
        assertNull("There should not be exception", entry.getException());

        // test one key not exist and does not belong to server 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(notBelongToServer0AndOutsideBothKeys.get(0));
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getKey());
        assertTrue("There should be exception", entry.hasException());
        assertFalse("There should not be values", entry.hasValues());
        assertNull("Value should be null", entry.getValues());
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getException() instanceof InvalidMetadataException);

        // test one key that exists on server 0 but does not belong to server 0
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(notBelongServer0ButInsideServer0Keys.get(0));
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertEquals("Not the right key", queryKeys.get(0), entry.getKey());
        assertTrue("There should be exception", entry.hasException());
        assertFalse("There should not be values", entry.hasValues());
        assertNull("Value should be null", entry.getValues());
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getException() instanceof InvalidMetadataException);

        // test one key deleted
        store0.delete(belongToAndInsideServer0Keys.get(4), null);
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer0Keys.get(4));
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertFalse("There should not be exception", entry.hasException());
        assertTrue("There should be values", entry.hasValues());
        assertEquals("Not the right key", queryKeys.get(0), entry.getKey());
        assertEquals("Value should be empty list", 0, entry.getValues().size());

        // test empty request
        queryKeys = new ArrayList<ByteArray>();
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertFalse("Results should be empty", results.hasNext());

        // test null key
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(null);
        assertEquals(1, queryKeys.size());
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        entry = results.next();
        assertFalse("There should not be more results", results.hasNext());
        assertTrue("There should be exception", entry.hasException());
        assertFalse("There should not be values", entry.hasValues());
        assertNull("Value should be null", entry.getValues());
        assertTrue("There should be IllegalArgumentException exception",
                   entry.getException() instanceof IllegalArgumentException);

        // test multiple keys (3) on store 1
        queryKeys = new ArrayList<ByteArray>();
        queryKeys.add(belongToAndInsideServer1Keys.get(0));
        queryKeys.add(belongToAndInsideServer1Keys.get(1));
        queryKeys.add(belongToAndInsideServer1Keys.get(2));
        results = getAdminClient().streamingOps.queryKeys(1, testStoreName, queryKeys.iterator());
        assertTrue("Results should not be empty", results.hasNext());
        Map<ByteArray, List<Versioned<byte[]>>> entries = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        int resultCount = 0;
        while(results.hasNext()) {
            resultCount++;
            entry = results.next();
            assertNull("There should not be exception in response", entry.getException());
            assertNotNull("Value should not be null for Key: ", entry.getValues());
            entries.put(entry.getKey(), entry.getValues());
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
        results = getAdminClient().streamingOps.queryKeys(0, testStoreName, queryKeys.iterator());
        // key 0
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(0).get(), entry.getKey().get()));
        assertEquals(0, ByteUtils.compare(belongToAndInsideServer0.get(queryKeys.get(0)),
                                          entry.getValues().get(0).getValue()));
        assertNull(entry.getException());
        // key 1
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(1).get(), entry.getKey().get()));
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getException() instanceof InvalidMetadataException);
        // key 2
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(2).get(), entry.getKey().get()));
        assertEquals(0, entry.getValues().size());
        assertNull(entry.getException());
        // key 3
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(3).get(), entry.getKey().get()));
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getException() instanceof InvalidMetadataException);
        // key 4
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(4).get(), entry.getKey().get()));
        assertEquals(0, ByteUtils.compare(belongToAndInsideServer0.get(queryKeys.get(4)),
                                          entry.getValues().get(0).getValue()));
        assertNull(entry.getException());
        // key 5
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(5).get(), entry.getKey().get()));
        assertEquals(0, entry.getValues().size());
        assertNull(entry.getException());
        // key 6
        entry = results.next();
        assertEquals(0, ByteUtils.compare(queryKeys.get(6).get(), entry.getKey().get()));
        assertTrue("There should be InvalidMetadataException exception",
                   entry.getException() instanceof InvalidMetadataException);
        // no more keys
        assertFalse(results.hasNext());
    }

    @Test
    public void testUpdate() {
        // TODO what guarantees these randomly generated keys don't collide
        // between test cases??
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

        getAdminClient().streamingOps.updateEntries(0, testStoreName, iterator, null);

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
    public void testUpdateTimeBased() {

        final long baseTimeMs = System.currentTimeMillis();
        String storeName = "test-replication-persistent";

        final HashMap<ByteArray, byte[]> entries = ServerTestUtils.createRandomKeyValuePairs(5);
        final List<ByteArray> keys = new ArrayList<ByteArray>(entries.keySet());

        // Insert some data for the keys
        Store<ByteArray, byte[], byte[]> nodeStore = getStore(0, storeName);

        for(int i = 0; i < keys.size(); i++) {
            ByteArray key = keys.get(i);
            byte[] val = entries.get(key);
            long ts = 0;
            if(i == 0) {
                // have multiple conflicting versions.. one lower ts and one
                // higher ts, than the streaming version
                Versioned<byte[]> v1 = new Versioned<byte[]>(val,
                                                             TestUtils.getClockWithTs(baseTimeMs - 1,
                                                                                      1));
                nodeStore.put(key, v1, null);
                Versioned<byte[]> v2 = new Versioned<byte[]>(val,
                                                             TestUtils.getClockWithTs(baseTimeMs + 1,
                                                                                      2));
                nodeStore.put(key, v2, null);
            } else {
                if(i % 2 == 0) {
                    // even keys : streaming write wins
                    ts = baseTimeMs + i;
                } else {
                    // odd keys : storage version wins
                    ts = baseTimeMs - i;
                }
                nodeStore.put(key, new Versioned<byte[]>(val, new VectorClock(ts)), null);
            }
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            final Iterator<ByteArray> keysItr = keys.iterator();
            int keyCount = 0;

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                while(keysItr.hasNext()) {
                    ByteArray key = keysItr.next();
                    byte[] val = entries.get(key);
                    long ts = 0;
                    if(keyCount == 0) {
                        // streaming put will be in the middle of two version on
                        // storage
                        keyCount++;
                        return new Pair<ByteArray, Versioned<byte[]>>(key,
                                                                      new Versioned<byte[]>(val,
                                                                                            new VectorClock(baseTimeMs)));
                    } else {
                        if(keyCount % 2 == 0) {
                            // even keys : streaming write wins
                            ts = baseTimeMs - keyCount;
                        } else {
                            // odd keys : storage version wins
                            ts = baseTimeMs + keyCount;
                        }
                        keyCount++;
                        return new Pair<ByteArray, Versioned<byte[]>>(key,
                                                                      new Versioned<byte[]>(val,
                                                                                            new VectorClock(ts)));
                    }
                }
                return endOfData();
            }
        };

        getAdminClient().streamingOps.updateEntriesTimeBased(0, storeName, iterator, null);

        // check updated values
        for(int i = 0; i < keys.size(); i++) {
            ByteArray key = keys.get(i);
            List<Versioned<byte[]>> vals = nodeStore.get(key, null);

            if(i == 0) {
                assertEquals("Must contain exactly two versions", 2, vals.size());
                Set<Long> storageTimeSet = new HashSet<Long>();
                storageTimeSet.add(((VectorClock) vals.get(0).getVersion()).getTimestamp());
                storageTimeSet.add(((VectorClock) vals.get(1).getVersion()).getTimestamp());
                Set<Long> expectedTimeSet = new HashSet<Long>();
                expectedTimeSet.add(baseTimeMs - 1);
                expectedTimeSet.add(baseTimeMs + 1);
                assertEquals("Streaming put should have backed off since atleast one version has greater timestamp",
                             expectedTimeSet,
                             storageTimeSet);
            } else {
                assertEquals("Must contain exactly one version", 1, vals.size());
                assertEquals("Must contain the version the the maximum timestamp",
                             baseTimeMs + i,
                             ((VectorClock) vals.get(0).getVersion()).getTimestamp());
            }

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
        getAdminClient().streamingOps.updateSlopEntries(0, slopIterator);

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
        adminClient.restoreOps.restoreDataFromReplications(1, 2);

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
        List<Integer> combinedLists = Arrays.asList(0, 1, 2, 4);

        Cluster targetCluster = UpdateClusterUtils.createUpdatedCluster(cluster, 1, primaryMoved);

        HashMap<ByteArray, byte[]> keysMovedWith0AsSecondary = Maps.newHashMap();

        // insert it into server-0 store
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
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
                keysMovedWith0AsSecondary.put(entry.getKey(), entry.getValue());
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
        int id = client.storeMntOps.migratePartitions(0,
                                                      1,
                                                      "test-recovery-data",
                                                      combinedLists,
                                                      null,
                                                      cluster);
        client.rpcOps.waitForCompletion(1, id, 120, TimeUnit.SECONDS);

        // Check the values
        for(Entry<ByteArray, byte[]> entry: keysMovedWith0AsSecondary.entrySet()) {
            assertEquals("server1 store should contain fetchAndupdated partitions.",
                         1,
                         store1.get(entry.getKey(), null).size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store1.get(entry.getKey(), null).get(0).getValue()));
        }

    }

    @Test
    public void testQuotaOpsForNode() throws InterruptedException {
        AdminClient client = getAdminClient();
        String storeName = storeDefs.get(0).getName();
        QuotaType quotaType = QuotaType.GET_THROUGHPUT;
        Integer nodeId = 0;
        Integer quota = 1000;

        // test set quota
        client.quotaMgmtOps.setQuotaForNode(storeName, quotaType, nodeId, quota);
        Integer getQuota = Integer.parseInt(client.quotaMgmtOps.getQuotaForNode(storeName,
                                                                                quotaType,
                                                                                nodeId).getValue());
        assertEquals(quota, getQuota);
        // Sometimes the former set and the newer set are execute in same
        // millisecond which causes the later set to fail with
        // ObsoleteVersionException. Add 5ms sleep.
        Thread.sleep(5);

        // test reset quota
        quota = 10;
        client.quotaMgmtOps.setQuotaForNode(storeName, quotaType, nodeId, quota);
        getQuota = Integer.parseInt(client.quotaMgmtOps.getQuotaForNode(storeName,
                                                                        quotaType,
                                                                        nodeId).getValue());
        assertEquals(quota, getQuota);
        Thread.sleep(5);

        // test delete quota
        assertTrue(client.quotaMgmtOps.deleteQuotaForNode(storeName, quotaType, nodeId));
        Versioned<String> quotaVal = client.quotaMgmtOps.getQuotaForNode(storeName,
                                                                         quotaType,
                                                                         nodeId);
        assertEquals(null, quotaVal);
        Thread.sleep(5);

    }

    @Test
    public void testRebalanceQuota() throws InterruptedException {
        AdminClient client = getAdminClient();
        String storeName = storeDefs.get(0).getName();
        QuotaType quotaType = QuotaType.GET_THROUGHPUT;
        Integer quota = 1000, targetQuota = quota / 2;
        client.quotaMgmtOps.setQuotaForNode(storeName, quotaType, 0, quota);
        client.quotaMgmtOps.rebalanceQuota(storeName, quotaType);
        // rebalanceQuota use put. Put completes as soon as the required nodes
        // are completed and rest of them are done in async. There is a race
        // condition here if you poll too soon you will see inconsistent result,
        // as some of the puts are still in async. Sleep here to avoid those
        // conditions.
        Thread.sleep(100);
        Integer getQuota0 = Integer.parseInt(client.quotaMgmtOps.getQuotaForNode(storeName,
                                                                                 quotaType,
                                                                                 0).getValue());
        Integer getQuota1 = Integer.parseInt(client.quotaMgmtOps.getQuotaForNode(storeName,
                                                                                 quotaType,
                                                                                 1).getValue());
        assertEquals(targetQuota, getQuota0);
        assertEquals(targetQuota, getQuota1);
    }
}
