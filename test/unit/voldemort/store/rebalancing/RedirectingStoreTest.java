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

package voldemort.store.rebalancing;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * @author bbansal
 * 
 */
public class RedirectingStoreTest extends TestCase {

    private static int TEST_VALUES_SIZE = 1000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    VoldemortServer server0;
    VoldemortServer server1;
    Cluster targetCluster;

    @Override
    public void setUp() throws IOException {
        Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] { {}, { 0, 1 } });
        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 1 }, { 0 } });

        server0 = startServer(0, storesXmlfile, cluster);

        server1 = startServer(1, storesXmlfile, cluster);
    }

    @Override
    public void tearDown() {
        try {
            server0.stop();
            FileUtils.deleteDirectory(new File(server0.getVoldemortConfig().getVoldemortHome()));

            server1.stop();
            FileUtils.deleteDirectory(new File(server1.getVoldemortConfig().getVoldemortHome()));
        } catch(Exception e) {
            // ignore exceptions here
        }
    }

    private VoldemortServer startServer(int node, String storesXmlfile, Cluster cluster)
            throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfig(node,
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
        return new RedirectingStore(ServerTestUtils.getSocketStore(storeName,
                                                                   server0.getIdentityNode()
                                                                          .getSocketPort(),
                                                                   RequestFormatType.VOLDEMORT_V1),
                                    metadata,
                                    server0.getStoreRepository());
    }

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

        // for normal state server0 should be empty
        checkGetEntries(entryMap,
                        server0,
                        server0.getStoreRepository().getStorageEngine(testStoreName),
                        Arrays.asList(0, 1),
                        Arrays.asList(-1));

        // set cluster.xml for invalidMetadata sake
        server0.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        server1.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);

        // set rebalancing 0 <-- 1 for partitions 2 only.
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.SERVER_STATE_KEY,
                               MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.REBALANCING_STEAL_INFO,
                               new RebalancePartitionsInfo(0,
                                                      1,
                                                      Arrays.asList(1),
                                                      Arrays.asList(testStoreName),
                                                      0));

        // for Rebalancing State we should see proxyGet()
        checkGetEntries(entryMap,
                        server0,
                        getRedirectingStore(server0.getMetadataStore(), testStoreName),
                        Arrays.asList(0),
                        Arrays.asList(1));
    }

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

        // for normal state server0 should not
        checkPutEntries(entryMap, server0, testStoreName, Arrays.asList(0, 1), Arrays.asList(-1));

        // set cluster.xml for invalidMetadata sake
        server0.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        server1.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);

        // set rebalancing 0 <-- 1 for partitions 2 only.
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.SERVER_STATE_KEY,
                               MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
        incrementVersionAndPut(server0.getMetadataStore(),
                               MetadataStore.REBALANCING_STEAL_INFO,
                               new RebalancePartitionsInfo(0,
                                                      1,
                                                      Arrays.asList(1),
                                                      Arrays.asList(testStoreName),
                                                      0));

        // for Rebalancing State we should see proxyPut()
        checkPutEntries(entryMap, server0, testStoreName, Arrays.asList(0), Arrays.asList(1));
    }

    private void checkGetEntries(HashMap<ByteArray, byte[]> entryMap,
                                 VoldemortServer server,
                                 Store<ByteArray, byte[]> store,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = server.getMetadataStore().getRoutingStrategy(store.getName());

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routing.getPartitionList(entry.getKey().get());
            if(unavailablePartitions.containsAll(partitions)) {
                try {
                    assertEquals("Keys for partition:" + partitions + " should not be present.",
                                 0,
                                 store.get(entry.getKey()).size());
                } catch(InvalidMetadataException e) {
                    // ignore
                }
            } else if(availablePartitions.containsAll(partitions)) {
                assertEquals("Keys for partition:" + partitions + " should be present.",
                             1,
                             store.get(entry.getKey()).size());
                assertEquals("Values should match.",
                             new String(entry.getValue()),
                             new String(store.get(entry.getKey()).get(0).getValue()));
            } else {
                fail("This case should not come for this test partitions:" + partitions);
            }
        }
    }

    private void checkPutEntries(HashMap<ByteArray, byte[]> entryMap,
                                 VoldemortServer server,
                                 String storeName,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        RoutingStrategy routing = server.getMetadataStore().getRoutingStrategy(storeName);
        RedirectingStore redirectingStore = getRedirectingStore(server0.getMetadataStore(),
                                                                storeName);

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routing.getPartitionList(entry.getKey().get());
            if(unavailablePartitions.containsAll(partitions)) {
                try {
                    // should NOT see obsoleteVersionException
                    redirectingStore.put(entry.getKey(),
                                         Versioned.value(entry.getValue(),
                                                         new VectorClock().incremented(0,
                                                                                       System.currentTimeMillis())));
                } catch(ObsoleteVersionException e) {
                    fail("should NOT see obsoleteVersionException for unavailablePartitions.");
                } catch(InvalidMetadataException e) {
                    // ignore
                }
            } else if(availablePartitions.containsAll(partitions)) {
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
            } else {
                fail("This case should not come for this test.");
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
