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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;

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
import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.rebalance.RebalanceClusterPlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.NoopFailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.DaemonThreadFactory;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class RedirectingStoreTest extends TestCase {

    private VoldemortServer[] servers;
    private Cluster targetCluster;
    private Cluster currentCluster;
    private List<Integer> primaryPartitionsMoved;
    private List<Integer> secondaryPartitionsMoved;
    private HashMap<ByteArray, byte[]> primaryEntriesMoved;
    private HashMap<ByteArray, byte[]> secondaryEntriesMoved;
    private HashMap<ByteArray, byte[]> proxyPutTestPrimaryEntries;
    private HashMap<ByteArray, byte[]> proxyPutTestSecondaryEntries;
    private final boolean useNio;
    private StoreDefinition storeDef;
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
    public void setUp() throws IOException, InterruptedException {
        currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1 }, { 2, 3 }, {} });
        targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster, 2, Arrays.asList(0));
        this.primaryPartitionsMoved = Lists.newArrayList(0);
        this.secondaryPartitionsMoved = Lists.newArrayList(2, 3);
        this.storeDef = new StoreDefinitionBuilder().setName("test")
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

        File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
        FileUtils.writeStringToFile(tempStoreXml,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef)));

        this.servers = new VoldemortServer[3];
        for(int nodeId = 0; nodeId < 3; nodeId++) {
            this.servers[nodeId] = startServer(nodeId,
                                               tempStoreXml.getAbsolutePath(),
                                               currentCluster);
        }

        // Start another node for only this unit test
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(100);

        SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(Lists.newArrayList("tcp://"
                                                                                                                               + currentCluster.getNodeById(0)
                                                                                                                                               .getHost()
                                                                                                                               + ":"
                                                                                                                               + currentCluster.getNodeById(0)
                                                                                                                                               .getSocketPort())));
        StoreClient<Object, Object> storeClient = factory.getStoreClient("test");

        this.primaryEntriesMoved = Maps.newHashMap();
        this.secondaryEntriesMoved = Maps.newHashMap();
        this.proxyPutTestPrimaryEntries = Maps.newHashMap();
        this.proxyPutTestSecondaryEntries = Maps.newHashMap();

        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      currentCluster);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            storeClient.put(new String(entry.getKey().get()), new String(entry.getValue()));
            List<Integer> pList = strategy.getPartitionList(entry.getKey().get());
            if(primaryPartitionsMoved.contains(pList.get(0))) {
                primaryEntriesMoved.put(entry.getKey(), entry.getValue());
            } else if(secondaryPartitionsMoved.contains(pList.get(0))) {
                secondaryEntriesMoved.put(entry.getKey(), entry.getValue());
            }
        }

        // Sleep a while for the queries to go through...
        // Hope the 'God of perfect timing' is on our side
        Thread.sleep(500);

        // steal a few primary key-value pairs for testing proxy put logic
        int cnt = 0;
        for(Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {
            if(cnt > 3)
                break;
            this.proxyPutTestPrimaryEntries.put(entry.getKey(), entry.getValue());
            cnt++;
        }
        for(ByteArray key: this.proxyPutTestPrimaryEntries.keySet()) {
            this.primaryEntriesMoved.remove(key);
        }
        assertTrue("Not enough primary entries", primaryEntriesMoved.size() > 1);

        // steal a few secondary key-value pairs for testing proxy put logic
        cnt = 0;
        for(Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {
            if(cnt > 3)
                break;
            this.proxyPutTestSecondaryEntries.put(entry.getKey(), entry.getValue());
            cnt++;
        }
        for(ByteArray key: this.proxyPutTestSecondaryEntries.keySet()) {
            this.secondaryEntriesMoved.remove(key);
        }
        assertTrue("Not enough secondary entries", primaryEntriesMoved.size() > 1);

        RebalanceClusterPlan plan = new RebalanceClusterPlan(currentCluster,
                                                             targetCluster,
                                                             Lists.newArrayList(storeDef),
                                                             true);
        List<RebalancePartitionsInfo> plans = RebalanceUtils.flattenNodePlans(Lists.newArrayList(plan.getRebalancingTaskQueue()));

        // Set into rebalancing state
        for(RebalancePartitionsInfo partitionPlan: plans) {
            servers[partitionPlan.getStealerId()].getMetadataStore()
                                                 .put(MetadataStore.SERVER_STATE_KEY,
                                                      MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);
            servers[partitionPlan.getStealerId()].getMetadataStore()
                                                 .put(MetadataStore.REBALANCING_STEAL_INFO,
                                                      new RebalancerState(Lists.newArrayList(partitionPlan)));
            servers[partitionPlan.getStealerId()].getMetadataStore()
                                                 .put(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML,
                                                      currentCluster);
        }

        // Update the cluster metadata on all three nodes
        for(VoldemortServer server: servers) {
            server.getMetadataStore().put(MetadataStore.CLUSTER_KEY, targetCluster);
        }

    }

    @Override
    @After
    public void tearDown() {
        for(VoldemortServer server: servers) {
            if(server != null)
                try {
                    ServerTestUtils.stopVoldemortServer(server);
                } catch(IOException e) {
                    e.printStackTrace();
                }
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

    private RedirectingStore getRedirectingStore(int nodeId,
                                                 MetadataStore metadata,
                                                 String storeName) {
        return new RedirectingStore(ServerTestUtils.getSocketStore(storeFactory,
                                                                   storeName,
                                                                   servers[nodeId].getIdentityNode()
                                                                                  .getSocketPort(),
                                                                   RequestFormatType.VOLDEMORT_V1),
                                    metadata,
                                    servers[nodeId].getStoreRepository(),
                                    new NoopFailureDetector(),
                                    storeFactory,
                                    true,
                                    Executors.newFixedThreadPool(1,
                                                                 new DaemonThreadFactory("voldemort-proxy-put-thread")),
                                    new ProxyPutStats(null));
    }

    @Test
    public void testProxyGet() {

        final RedirectingStore storeNode2 = getRedirectingStore(2,
                                                                servers[2].getMetadataStore(),
                                                                "test");
        final RedirectingStore storeNode0 = getRedirectingStore(0,
                                                                servers[0].getMetadataStore(),
                                                                "test");
        // Check primary
        for(final Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {
            assertEquals("Keys should be present.", 1, storeNode2.get(entry.getKey(), null).size());
            assertEquals("Values should match.",
                         new String(entry.getValue()),
                         new String(storeNode2.get(entry.getKey(), null).get(0).getValue()));
            assertEquals("Keys should be present.", 1, storeNode0.get(entry.getKey(), null).size());
            assertEquals("Values should match.",
                         new String(entry.getValue()),
                         new String(storeNode0.get(entry.getKey(), null).get(0).getValue()));
        }
        // Check secondary
        for(final Entry<ByteArray, byte[]> entry: secondaryEntriesMoved.entrySet()) {
            assertEquals("Keys should be present.", 1, storeNode2.get(entry.getKey(), null).size());
            assertEquals("Values should match.",
                         new String(entry.getValue()),
                         new String(storeNode2.get(entry.getKey(), null).get(0).getValue()));
        }
    }

    @Test
    public void testProxyGetAll() {
        final RedirectingStore storeNode2 = getRedirectingStore(2,
                                                                servers[2].getMetadataStore(),
                                                                "test");
        final RedirectingStore storeNode0 = getRedirectingStore(0,
                                                                servers[0].getMetadataStore(),
                                                                "test");
        // Check primary
        Set<ByteArray> primaryKeySet = primaryEntriesMoved.keySet();
        Iterator<ByteArray> iter = primaryKeySet.iterator();
        while(iter.hasNext()) {

            List<ByteArray> keys = Lists.newArrayList();
            for(int keyBatch = 0; keyBatch < 10 && iter.hasNext(); keyBatch++) {
                keys.add(iter.next());
            }
            assertEquals("Keys should be present.", keys.size(), storeNode2.getAll(keys, null)
                                                                           .size());

            for(Entry<ByteArray, List<Versioned<byte[]>>> entry: storeNode2.getAll(keys, null)
                                                                           .entrySet()) {
                assertEquals("Values should match.",
                             new String(entry.getValue().get(0).getValue()),
                             new String(storeNode2.get(entry.getKey(), null).get(0).getValue()));
            }

            assertEquals("Keys should be present.", keys.size(), storeNode0.getAll(keys, null)
                                                                           .size());

            for(Entry<ByteArray, List<Versioned<byte[]>>> entry: storeNode0.getAll(keys, null)
                                                                           .entrySet()) {
                assertEquals("Values should match.",
                             new String(entry.getValue().get(0).getValue()),
                             new String(storeNode0.get(entry.getKey(), null).get(0).getValue()));
            }
        }

        // Check secondary
        Set<ByteArray> secondaryKeySet = secondaryEntriesMoved.keySet();
        iter = secondaryKeySet.iterator();
        while(iter.hasNext()) {
            List<ByteArray> keys = Lists.newArrayList();
            for(int keyBatch = 0; keyBatch < 10 && iter.hasNext(); keyBatch++) {
                keys.add(iter.next());
            }
            assertEquals("Keys should be present.", keys.size(), storeNode2.getAll(keys, null)
                                                                           .size());

            for(Entry<ByteArray, List<Versioned<byte[]>>> entry: storeNode2.getAll(keys, null)
                                                                           .entrySet()) {
                assertEquals("Values should match.",
                             new String(entry.getValue().get(0).getValue()),
                             new String(storeNode2.get(entry.getKey(), null).get(0).getValue()));
            }
        }
    }

    @Test
    public void testProxyGetDuringPut() {

        final RedirectingStore storeNode2 = getRedirectingStore(2,
                                                                servers[2].getMetadataStore(),
                                                                "test");
        final RedirectingStore storeNode0 = getRedirectingStore(0,
                                                                servers[0].getMetadataStore(),
                                                                "test");
        // Check primary
        for(final Entry<ByteArray, byte[]> entry: primaryEntriesMoved.entrySet()) {

            try {
                // should see obsoleteVersionException for same vectorClock
                storeNode2.put(entry.getKey(),
                               Versioned.value(entry.getValue(),
                                               new VectorClock().incremented(0,
                                                                             System.currentTimeMillis())),
                               null);
                fail("Should see obsoleteVersionException here.");
            } catch(ObsoleteVersionException e) {
                // ignore
            }

            try {
                // should see obsoleteVersionException for same vectorClock
                storeNode0.put(entry.getKey(),
                               Versioned.value(entry.getValue(),
                                               new VectorClock().incremented(0,
                                                                             System.currentTimeMillis())),
                               null);
                fail("Should see obsoleteVersionException here.");
            } catch(ObsoleteVersionException e) {
                // ignore
            } catch(InvalidMetadataException e) {

            }

        }
    }

    /**
     * This exits out immediately if the node is not proxy putting.
     * 
     * @param store
     */
    private void waitForProxyPutsToDrain(RedirectingStore store) {
        // wait for the proxy write to complete
        while(store.getProxyPutStats().getNumPendingProxyPuts() > 0) {
            try {
                Thread.sleep(50);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testProxyPuts() {

        List<ByteArray> testPrimaryKeys = new ArrayList<ByteArray>(this.proxyPutTestPrimaryEntries.keySet());
        List<ByteArray> testSecondaryKeys = new ArrayList<ByteArray>(this.proxyPutTestSecondaryEntries.keySet());

        final RedirectingStore redirectingStoreNode2 = getRedirectingStore(2,
                                                                           servers[2].getMetadataStore(),
                                                                           "test");
        final RedirectingStore redirectingStoreNode0 = getRedirectingStore(0,
                                                                           servers[0].getMetadataStore(),
                                                                           "test");
        final Store<ByteArray, byte[], byte[]> socketStoreNode2 = redirectingStoreNode2.getRedirectingSocketStore("test",
                                                                                                                  2);
        final Store<ByteArray, byte[], byte[]> socketStoreNode0 = redirectingStoreNode0.getRedirectingSocketStore("test",
                                                                                                                  0);

        // 1. Make sure the vector clocks make sense.. Read through Node 2 and
        // proxy getting from Node 0 and issue a write based off that,
        // incrementing the clock for Node 2 and make sure there is no
        // ObsoleteVersionException at both Node 0 and
        // Node 2.
        ByteArray key1 = testSecondaryKeys.get(0);
        VectorClock clock1 = ((VectorClock) redirectingStoreNode2.getVersions(key1).get(0)).incremented(2,
                                                                                                        System.currentTimeMillis());
        try {
            redirectingStoreNode2.put(key1,
                                      Versioned.value("write-through".getBytes("UTF-8"), clock1),
                                      null);
        } catch(Exception e) {
            fail("Unexpected error in testing write through proxy put");
            e.printStackTrace();
        }
        waitForProxyPutsToDrain(redirectingStoreNode2);

        assertTrue("Unexpected failures in proxy put",
                   redirectingStoreNode2.getProxyPutStats().getNumProxyPutFailures() == 0);
        assertEquals("Unexpected value in Node 2",
                     "write-through",
                     new String(socketStoreNode2.get(key1, null).get(0).getValue()));
        assertTrue("Proxy write not seen on proxy node 0",
                   "write-through".equals(new String(socketStoreNode0.get(key1, null)
                                                                     .get(0)
                                                                     .getValue())));

        // Also test that if put fails locally, proxy put is not attempted.
        try {
            redirectingStoreNode2.put(key1,
                                      Versioned.value("write-through-updated".getBytes("UTF-8"),
                                                      clock1),
                                      null);
            fail("Should have thrown OVE");
        } catch(ObsoleteVersionException ove) {
            // Expected
        } catch(Exception e) {
            fail("Unexpected error in testing write through proxy put");
            e.printStackTrace();
        }
        waitForProxyPutsToDrain(redirectingStoreNode2);
        assertFalse("Proxy write not seen on proxy node 0",
                    "write-through-updated".equals(new String(socketStoreNode0.get(key1, null)
                                                                              .get(0)
                                                                              .getValue())));

        // 2. Make sure if the proxy node is still a replica, we don't issue
        // proxy puts. Node 2 -> Node 0 on partition 0, for which Node 0 is
        // still a replica
        ByteArray key2 = testPrimaryKeys.get(0);
        VectorClock clock2 = ((VectorClock) redirectingStoreNode2.getVersions(key2).get(0)).incremented(2,
                                                                                                        System.currentTimeMillis());
        try {
            redirectingStoreNode2.put(key2,
                                      Versioned.value("write-through".getBytes("UTF-8"), clock2),
                                      null);
        } catch(Exception e) {
            fail("Unexpected error in testing write through proxy put");
            e.printStackTrace();
        }
        waitForProxyPutsToDrain(redirectingStoreNode2);
        assertEquals("Unexpected value in Node 2",
                     "write-through",
                     new String(socketStoreNode2.get(key2, null).get(0).getValue()));
        assertFalse("Proxy write seen on proxy node which is a replica",
                    "write-through".equals(new String(socketStoreNode0.get(key2, null)
                                                                      .get(0)
                                                                      .getValue())));

        // 3. If the same entry reaches Node 2 again from Node 0, via partition
        // fetch, it will
        // generate OVE.
        try {
            redirectingStoreNode2.put(key2,
                                      Versioned.value("write-through".getBytes("UTF-8"), clock2),
                                      null);
            fail("Should have thrown OVE");
        } catch(ObsoleteVersionException ove) {
            // Expected
        } catch(Exception e) {
            fail("Unexpected error in testing write through proxy put");
            e.printStackTrace();
        }
    }
}
