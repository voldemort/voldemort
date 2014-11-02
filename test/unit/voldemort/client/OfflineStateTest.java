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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Tests voldemort server behavior under NORMAL_SERVER and OFFLINE_SERVER
 * states, especially after state transitions.
 */
@RunWith(Parameterized.class)
public class OfflineStateTest {

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

    public OfflineStateTest(boolean useNio, boolean onlineRetention) {
        this.useNio = useNio;
        this.onlineRetention = onlineRetention;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, false } });
        // , { true, true }, { false, false }, { false, true } });
    }

    @Before
    public void setUp() throws IOException {
        int numServers = 1;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 } };
        // , { 4, 5, 6, 7 } };
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

    private boolean testOnlineTraffic() {
        String key = "k-e-y", value = Long.toString(System.nanoTime());
        try {
            storeClient.put(key, value);
            Versioned<String> versioned = storeClient.get(key);
            if(versioned.getValue().equals(value)) {
                return true;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean testSlopStreaming() {
        final List<Versioned<Slop>> entrySet = ServerTestUtils.createRandomSlops(0,
                                                                                 10000,
                                                                                 testStoreName,
                                                                                 "users",
                                                                                 "test-replication-persistent",
                                                                                 "test-readrepair-memory",
                                                                                 "test-consistent",
                                                                                 "test-consistent-with-pref-list");

        Iterator<Versioned<Slop>> slopIterator = entrySet.iterator();
        try {
            getAdminClient().streamingOps.updateSlopEntries(0, slopIterator);
        } catch(VoldemortException e) {
            return false;
        }

        // check updated values
        Iterator<Versioned<Slop>> entrysetItr = entrySet.iterator();

        while(entrysetItr.hasNext()) {
            Versioned<Slop> versioned = entrysetItr.next();
            Slop nextSlop = versioned.getValue();
            Store<ByteArray, byte[], byte[]> store = getStore(0, nextSlop.getStoreName());

            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                return store.get(nextSlop.getKey(), null).size() != 0;
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                return store.get(nextSlop.getKey(), null).size() == 0;
            }
        }
        return false;
    }

    private void toOfflineState(AdminClient client) {
        // change to OFFLINE_SERVER
        client.metadataMgmtOps.setRemoteOfflineState(getVoldemortServer(0).getIdentityNode()
                                                                          .getId(), true);
        MetadataStore.VoldemortState state = getVoldemortServer(0).getMetadataStore()
                                                                  .getServerStateUnlocked();
        assertEquals("State should be changed correctly to offline state",
                     MetadataStore.VoldemortState.OFFLINE_SERVER,
                     state);
        assertFalse(testOnlineTraffic());
    }

    private void toNormalState(AdminClient client) {
        // change back to NORMAL_SERVER
        client.metadataMgmtOps.setRemoteOfflineState(getVoldemortServer(0).getIdentityNode()
                                                                          .getId(), false);
        MetadataStore.VoldemortState state = getVoldemortServer(0).getMetadataStore()
                                                                  .getServerStateUnlocked();
        assertEquals("State should be changed correctly to normal state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);
        assertTrue(testOnlineTraffic());
    }

    @Test
    public void testStateTransitions() {
        AdminClient client = getAdminClient();
        assertTrue(testOnlineTraffic());
        assertTrue(testSlopStreaming());
        toOfflineState(client);
        assertFalse(testOnlineTraffic());
        assertFalse(testSlopStreaming());
        toNormalState(client);
        assertTrue(testOnlineTraffic());
        assertTrue(testSlopStreaming());
        toOfflineState(client);
        assertFalse(testOnlineTraffic());
        assertFalse(testSlopStreaming());
        toNormalState(client);
        assertTrue(testOnlineTraffic());
        assertTrue(testSlopStreaming());
        toOfflineState(client);
        assertFalse(testOnlineTraffic());
        assertFalse(testSlopStreaming());
        toNormalState(client);
        assertTrue(testOnlineTraffic());
        assertTrue(testSlopStreaming());
    }
}
