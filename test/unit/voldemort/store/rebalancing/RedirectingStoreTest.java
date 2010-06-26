/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.store.rebalancing;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

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
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.NoopFailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

@RunWith(Parameterized.class)
public class RedirectingStoreTest extends TestCase {

    private static int TEST_VALUES_SIZE = 1000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    VoldemortServer server0;
    VoldemortServer server1;
    Cluster targetCluster;
    private final boolean useNio;
    private final SocketStoreFactory storeFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    public RedirectingStoreTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] { {}, { 0, 1 } });
        targetCluster = RebalanceUtils.createUpdatedCluster(cluster,
                                                            cluster.getNodeById(0),
                                                            cluster.getNodeById(1),
                                                            Arrays.asList(1));

        server0 = startServer(0, storesXmlfile, cluster);

        server1 = startServer(1, storesXmlfile, cluster);
    }

    @Override
    @After
    public void tearDown() {
        try {
            server0.stop();
            FileUtils.deleteDirectory(new File(server0.getVoldemortConfig().getVoldemortHome()));

            server1.stop();
            FileUtils.deleteDirectory(new File(server1.getVoldemortConfig().getVoldemortHome()));
        } catch(Exception e) {
            // ignore exceptions here
        }

        storeFactory.close();
    }

    private VoldemortServer startServer(int node, String storesXmlfile, Cluster cluster)
            throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfig(useNio,
                                                                    node,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile,
                                                                    new Properties());
        // enable metadata checking for this test.
        config.setEnableMetadataChecking(true);
        config.setEnableRebalanceService(false);

        VoldemortServer server = new VoldemortServer(config, cluster);
        server.start();
        return server;
    }

    private RedirectingStore getRedirectingStore(MetadataStore metadata, String storeName) {
        return new RedirectingStore(ServerTestUtils.getSocketStore(storeFactory,
                                                                   storeName,
                                                                   server0.getIdentityNode()
                                                                          .getSocketPort(),
                                                                   RequestFormatType.VOLDEMORT_V1),
                                    metadata,
                                    server0.getStoreRepository(),
                                    new NoopFailureDetector(),
                                    new ClientRequestExecutorPool(10, 1000, 10000, 10000));
    }

    @Test
    public void testProxyGetAll() {
        Map<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        Store<ByteArray, byte[]> store = server1.getStoreRepository()
                       .getStorageEngine(testStoreName);
        for (Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            store.put(entry.getKey(),
                      Versioned.value(entry.getValue(),
                                      new VectorClock().incremented(0, System.currentTimeMillis())));
        }

        server0.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        server1.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);

        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.SERVER_STATE_KEY,
                               MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.REBALANCING_STEAL_INFO,
                               new RebalancerState(Arrays.asList(new RebalancePartitionsInfo(0,
                                                                                             1,
                                                                                             Arrays.asList(1),
                                                                                             new ArrayList<Integer>(0),
                                                                                             Arrays.asList(testStoreName),
                                                                                             0))));
        checkGetAllEntries(entryMap, server0, getRedirectingStore(server0.getMetadataStore(),
                                                                  testStoreName), Arrays.asList(1));
    }
    
    @Test
    public void testProxyGet() {
        // create bunch of key-value pairs
        HashMap<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        // populate all entries in server1
        Store<ByteArray, byte[]> store = server1.getStoreRepository()
                                                .getStorageEngine(testStoreName);
        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            store.put(entry.getKey(),
                      Versioned.value(entry.getValue(),
                                      new VectorClock().incremented(0, System.currentTimeMillis())));
        }

        // set cluster.xml for invalidMetadata sake
        server0.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        server1.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);

        // set rebalancing 0 <-- 1 for partitions 2 only.
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.SERVER_STATE_KEY,
                               MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.REBALANCING_STEAL_INFO,
                               new RebalancerState(Arrays.asList(new RebalancePartitionsInfo(0,
                                                                                             1,
                                                                                             Arrays.asList(1),
                                                                                             new ArrayList<Integer>(0),
                                                                                             Arrays.asList(testStoreName),
                                                                                             0))));

        // for Rebalancing State we should see proxyGet()
        checkGetEntries(entryMap, server0, getRedirectingStore(server0.getMetadataStore(),
                                                               testStoreName), Arrays.asList(1));
    }

    @Test
    public void testProxyPut() {
        // create bunch of key-value pairs
        HashMap<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        // populate all entries in server1
        Store<ByteArray, byte[]> store = server1.getStoreRepository()
                                                .getStorageEngine(testStoreName);
        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            store.put(entry.getKey(),
                      Versioned.value(entry.getValue(),
                                      new VectorClock().incremented(0, System.currentTimeMillis())));
        }

        // set cluster.xml for invalidMetadata sake
        server0.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        server1.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);

        // set rebalancing 0 <-- 1 for partitions 2 only.
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.SERVER_STATE_KEY,
                               MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.REBALANCING_STEAL_INFO,
                               new RebalancerState(Arrays.asList(new RebalancePartitionsInfo(0,
                                                                                             1,
                                                                                             Arrays.asList(1),
                                                                                             new ArrayList<Integer>(0),
                                                                                             Arrays.asList(testStoreName),
                                                                                             0))));

        // for Rebalancing State we should see proxyPut()
        checkPutEntries(entryMap, server0, testStoreName, Arrays.asList(1));
    }

    private void checkGetAllEntries(Map<ByteArray, byte[]> entryMap,
                                    VoldemortServer server,
                                    Store<ByteArray, byte[]> store,
                                    List<Integer> availablePartition) {
        RoutingStrategy routing = server.getMetadataStore().getRoutingStrategy(store.getName());
        List<ByteArray> keysInPartitions = new ArrayList<ByteArray>();
        for (ByteArray key: entryMap.keySet()) {
            List<Integer> partitions = routing.getPartitionList(key.get());
            if (availablePartition.containsAll(partitions)) {
                keysInPartitions.add(key);
            }
        }
        Map<ByteArray, List<Versioned<byte[]>>> results = store.getAll(keysInPartitions);
        for (Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            assertEquals("Values should match",
                         new String(entry.getValue().get(0).getValue()),
                         new String(entryMap.get(entry.getKey())));
        }
    }
    
    private void checkGetEntries(HashMap<ByteArray, byte[]> entryMap,
                                 VoldemortServer server,
                                 Store<ByteArray, byte[]> store,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = server.getMetadataStore().getRoutingStrategy(store.getName());

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routing.getPartitionList(entry.getKey().get());
            if(availablePartitions.containsAll(partitions)) {
                assertEquals("Keys for partition:" + partitions + " should be present.",
                             1,
                             store.get(entry.getKey()).size());
                assertEquals("Values should match.",
                             new String(entry.getValue()),
                             new String(store.get(entry.getKey()).get(0).getValue()));
            }
        }
    }

    private void checkPutEntries(HashMap<ByteArray, byte[]> entryMap,
                                 VoldemortServer server,
                                 String storeName,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = server.getMetadataStore().getRoutingStrategy(storeName);
        RedirectingStore redirectingStore = getRedirectingStore(server0.getMetadataStore(),
                                                                storeName);

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routing.getPartitionList(entry.getKey().get());
            if(availablePartitions.containsAll(partitions)) {
                try {
                    // should see obsoleteVersionException for same vectorClock
                    redirectingStore.put(entry.getKey(),
                                         Versioned.value(entry.getValue(),
                                                         new VectorClock().incremented(0,
                                                                                       System.currentTimeMillis())));
                    fail("Should see obsoleteVersionException here.");
                } catch(ObsoleteVersionException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * helper function to auto update version and put()
     * 
     * @param key
     * @param value
     */
    private void incrementVersionAndPut(MetadataStore metadataStore, String keyString, Object value) {
        ByteArray key = new ByteArray(ByteUtils.getBytes(keyString, "UTF-8"));
        VectorClock current = (VectorClock) metadataStore.getVersions(key).get(0);

        metadataStore.put(keyString,
                          new Versioned<Object>(value,
                                                current.incremented(0, System.currentTimeMillis())));
    }
}
