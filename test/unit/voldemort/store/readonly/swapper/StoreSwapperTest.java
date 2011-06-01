/*
 * Copyright 2011 LinkedIn, Inc
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

package voldemort.store.readonly.swapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class StoreSwapperTest extends TestCase {

    private static int NUM_NODES = 3;
    private static String STORE_NAME = "test";
    private static SerializerDefinition serializerDef = new SerializerDefinition("json", "'string'");
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClient adminClient;
    private StoreDefinition storeDef;
    private File baseDirs[];

    @Override
    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(NUM_NODES);
        servers = new VoldemortServer[NUM_NODES];
        baseDirs = new File[NUM_NODES];
        storeDef = new StoreDefinitionBuilder().setName(STORE_NAME)
                                               .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                               .setKeySerializer(serializerDef)
                                               .setValueSerializer(serializerDef)
                                               .setRoutingPolicy(RoutingTier.SERVER)
                                               .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                               .setReplicationFactor(2)
                                               .setPreferredReads(1)
                                               .setRequiredReads(1)
                                               .setPreferredWrites(1)
                                               .setRequiredWrites(1)
                                               .build();

        File storesXml = new File(TestUtils.createTempDir(), "stores.xml");

        StoreDefinitionsMapper storeDefMapper = new StoreDefinitionsMapper();
        FileWriter writer = new FileWriter(storesXml);
        writer.write(storeDefMapper.writeStoreList(Lists.newArrayList(storeDef)));
        writer.close();

        File baseTempDir = TestUtils.createTempDir();

        Properties props = new Properties();
        props.put("readonly.backups", "1");
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            servers[nodeId] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                   ServerTestUtils.createServerConfig(false,
                                                                                                      nodeId,
                                                                                                      baseTempDir.getAbsolutePath(),
                                                                                                      null,
                                                                                                      storesXml.getAbsolutePath(),
                                                                                                      props),
                                                                   cluster);
            baseDirs[nodeId] = new File(baseTempDir + "/node-" + nodeId + "/data/read-only/"
                                        + STORE_NAME);
        }

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

    @Test
    public void testAdminStoreSwapper() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        try {
            // Use the admin store swapper
            StoreSwapper swapper = new AdminStoreSwapper(cluster,
                                                         executor,
                                                         adminClient,
                                                         1000000,
                                                         true,
                                                         true);
            testFetchSwap(swapper);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testHttpStoreSwapper() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        try {
            // Use the http store swapper
            HttpConnectionManager manager = new MultiThreadedHttpConnectionManager();
            manager.getParams().setMaxTotalConnections(10);
            manager.getParams().setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION,
                                                         10);
            HttpClient client = new HttpClient(manager);
            StoreSwapper swapper = new HttpStoreSwapper(cluster,
                                                        executor,
                                                        client,
                                                        "read-only/mgmt",
                                                        true,
                                                        true);
            testFetchSwap(swapper);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testAdminStoreSwapperWithoutRollback() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        try {
            // Use the admin store swapper
            StoreSwapper swapper = new AdminStoreSwapper(cluster,
                                                         executor,
                                                         adminClient,
                                                         1000000,
                                                         false,
                                                         false);
            testFetchSwapWithoutRollback(swapper);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testHttpStoreSwapperWithoutRollback() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        try {
            // Use the http store swapper
            HttpConnectionManager manager = new MultiThreadedHttpConnectionManager();
            manager.getParams().setMaxTotalConnections(10);
            manager.getParams().setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION,
                                                         10);
            HttpClient client = new HttpClient(manager);
            StoreSwapper swapper = new HttpStoreSwapper(cluster,
                                                        executor,
                                                        client,
                                                        "read-only/mgmt",
                                                        false,
                                                        false);
            testFetchSwapWithoutRollback(swapper);
        } finally {
            executor.shutdown();
        }
    }

    public File createTempROFolder() {
        File tempFolder = TestUtils.createTempDir();
        for(int i = 0; i < NUM_NODES; i++) {
            Utils.mkdirs(new File(tempFolder, "node-" + Integer.toString(i)));
        }
        return tempFolder;
    }

    public void testFetchSwapWithoutRollback(StoreSwapper swapper) throws Exception {

        // 1) Fetch for all nodes are successful
        File temporaryDir = createTempROFolder();

        // Retrieve all the current versions
        long currentVersion = adminClient.getROCurrentVersion(0, Lists.newArrayList(STORE_NAME))
                                         .get(STORE_NAME);
        for(int nodeId = 1; nodeId < NUM_NODES; nodeId++) {
            long newVersion = adminClient.getROCurrentVersion(nodeId,
                                                              Lists.newArrayList(STORE_NAME))
                                         .get(STORE_NAME);
            if(newVersion != currentVersion)
                fail("Current version (on " + nodeId + ") = " + newVersion
                     + " is not equal to others");
        }

        swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 1);

        // Check the directories and entries
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            File[] versionDirs = ReadOnlyUtils.getVersionDirs(baseDirs[nodeId]);
            for(File versionDir: versionDirs) {
                assertTrue(Lists.newArrayList(currentVersion + 1, currentVersion)
                                .contains(ReadOnlyUtils.getVersionId(versionDir)));
            }
        }

        // 2) Fetch fails on some nodes - Do this by creating a folder with
        // version directory which exists
        temporaryDir = createTempROFolder();

        // Add version "currentVersion + 3" on node-1 ...
        Utils.mkdirs(new File(baseDirs[1], "version-" + Long.toString(currentVersion + 3)));

        try {
            swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 3);
            fail("Should throw a VoldemortException during pushing to node 0");
        } catch(VoldemortException e) {}

        // ... check if "currentVersion + 3 " is NOT deleted
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            long maxVersion = adminClient.getROMaxVersion(nodeId, Lists.newArrayList(STORE_NAME))
                                         .get(STORE_NAME);

            assertTrue(maxVersion == (currentVersion + 3));
        }

    }

    public void testFetchSwap(StoreSwapper swapper) throws Exception {

        // 1) Fetch for all nodes are successful
        File temporaryDir = createTempROFolder();

        // Retrieve all the current versions
        long currentVersion = adminClient.getROCurrentVersion(0, Lists.newArrayList(STORE_NAME))
                                         .get(STORE_NAME);
        for(int nodeId = 1; nodeId < NUM_NODES; nodeId++) {
            long newVersion = adminClient.getROCurrentVersion(nodeId,
                                                              Lists.newArrayList(STORE_NAME))
                                         .get(STORE_NAME);
            if(newVersion != currentVersion)
                fail("Current version (on " + nodeId + ") = " + newVersion
                     + " is not equal to others");
        }

        swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 1);

        // Check the directories and entries
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            File[] versionDirs = ReadOnlyUtils.getVersionDirs(baseDirs[nodeId]);
            for(File versionDir: versionDirs) {
                assertTrue(Lists.newArrayList(currentVersion + 1, currentVersion)
                                .contains(ReadOnlyUtils.getVersionId(versionDir)));
            }
        }

        // 2) Fetch fails on some nodes - Do this by creating a folder with
        // version directory which exists
        temporaryDir = createTempROFolder();

        // Add version "currentVersion + 3" on node-1 ...
        Utils.mkdirs(new File(baseDirs[1], "version-" + Long.toString(currentVersion + 3)));

        try {
            swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 3);
            fail("Should throw a VoldemortException during pushing to node 0");
        } catch(VoldemortException e) {}

        // ... check if "currentVersion + 3 " is deleted on other nodes
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            if(nodeId != 1) {
                File[] versionDirs = ReadOnlyUtils.getVersionDirs(baseDirs[nodeId]);

                for(File versionDir: versionDirs) {
                    assertTrue(ReadOnlyUtils.getVersionId(versionDir) != (currentVersion + 3));
                }
            }
        }

        // 3) Have a folder with a version number very high while others are
        // still stuck at small number
        temporaryDir = createTempROFolder();

        // Create "currentVersion + 2" for all other nodes
        // i.e. N0 [ latest -> v3 ], N<others> [ latest -> v2 ]
        TreeMap<Integer, String> toSwap = Maps.newTreeMap();
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            if(nodeId != 1) {
                File newVersion = new File(baseDirs[nodeId], "version-"
                                                             + Long.toString(currentVersion + 2));
                Utils.mkdirs(newVersion);
                toSwap.put(nodeId, newVersion.getAbsolutePath());
            }
        }
        toSwap.put(1,
                   new File(baseDirs[1], "version-" + Long.toString(currentVersion + 3)).getAbsolutePath());

        swapper.invokeSwap(STORE_NAME, Lists.newArrayList(toSwap.values()));

        // Try to fetch in v2, which should fail on all
        try {
            swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 2);
            fail("Should throw a VoldemortException during pushing to node 0, 1");
        } catch(VoldemortException e) {}

        // 4) Move one node into rebalancing state and try swapping
        temporaryDir = createTempROFolder();
        // Current version now should be same afterwards as well
        Map<Integer, Long> versionToNode = Maps.newHashMap();

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            versionToNode.put(nodeId,
                              adminClient.getROCurrentVersion(nodeId,
                                                              Lists.newArrayList(STORE_NAME))
                                         .get(STORE_NAME));
        }

        servers[1].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                          MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);

        try {
            swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 4);
            fail("Should have thrown exception during swapping");
        } catch(VoldemortException e) {}

        // Check that latest is not currentVersion + 4
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            long currentNodeVersion = adminClient.getROCurrentVersion(nodeId,
                                                                      Lists.newArrayList(STORE_NAME))
                                                 .get(STORE_NAME);
            assertTrue(currentNodeVersion != (currentVersion + 4));
            assertEquals(currentNodeVersion, (long) versionToNode.get(nodeId));
        }

        // 5) All swaps work correctly
        temporaryDir = createTempROFolder();
        servers[1].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                          MetadataStore.VoldemortState.NORMAL_SERVER);

        swapper.swapStoreData(STORE_NAME, temporaryDir.getAbsolutePath(), currentVersion + 5);

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            long currentNodeVersion = adminClient.getROCurrentVersion(nodeId,
                                                                      Lists.newArrayList(STORE_NAME))
                                                 .get(STORE_NAME);
            assertTrue(currentNodeVersion == (currentVersion + 5));
        }
    }
}
