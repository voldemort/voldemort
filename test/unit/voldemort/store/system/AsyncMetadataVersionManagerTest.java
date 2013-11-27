/*
 * Copyright 2012-2013 LinkedIn, Inc
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

package voldemort.store.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SystemStoreClient;
import voldemort.client.SystemStoreClientFactory;
import voldemort.client.SystemStoreRepository;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.cluster.Cluster;
import voldemort.common.service.SchedulerService;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.utils.SystemTime;

/**
 * Test class to verify the AsyncMetadataVersionManager
 * 
 * @author csoman
 * 
 */
@RunWith(Parameterized.class)
public class AsyncMetadataVersionManagerTest {

    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    String[] bootStrapUrls = null;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private VoldemortServer[] servers;
    private Cluster cluster;
    public static String socketUrl = "";
    protected int clientZoneId;
    private long newVersion = 0;

    private SystemStoreClient<String, String> sysVersionStore;
    private SystemStoreRepository repository;
    private SchedulerService scheduler;
    private AsyncMetadataVersionManager asyncCheckMetadata;
    private boolean callbackDone = false;
    private long updatedClusterVersion;
    private long updatedStoreVersion;
    private List<StoreDefinition> storeDefs;

    // Multi store version tracking test
    List<AsyncMetadataVersionManager> asyncVersionManagers = new ArrayList<AsyncMetadataVersionManager>();
    long[] updatedVersions = new long[3];
    boolean[] callbacksDone = new boolean[3];

    public AsyncMetadataVersionManagerTest(Cluster cluster,
                                           List<StoreDefinition> storeDefs,
                                           int clientZoneId) {
        this.cluster = cluster;
        this.storeDefs = storeDefs;
        this.clientZoneId = clientZoneId;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIds(),
                        ClusterTestUtils.getZ1Z3Z5StoreDefsBDB(), 3 },
                { ClusterTestUtils.getZZZCluster(), ClusterTestUtils.getZZZ322StoreDefs("bdb"), 0 } });
    }

    @Before
    public void setUp() throws Exception {
        servers = new VoldemortServer[cluster.getNodeIds().size()];
        int i = 0;
        for(Integer nodeId: cluster.getNodeIds()) {
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                                nodeId,
                                                                                TestUtils.createTempDir()
                                                                                         .getAbsolutePath(),
                                                                                cluster,
                                                                                storeDefs,
                                                                                new Properties());
            VoldemortServer server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                          config);
            servers[i++] = server;
        }

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();

        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(bootStrapUrls).setClientZoneId(clientZoneId);
        SystemStoreClientFactory<String, String> systemStoreFactory = new SystemStoreClientFactory<String, String>(clientConfig);
        sysVersionStore = systemStoreFactory.createSystemStore(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        repository = new SystemStoreRepository(clientConfig);

        repository.addSystemStore(sysVersionStore,
                                  SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        this.scheduler = new SchedulerService(2, SystemTime.INSTANCE, true);
    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    /*
     * Validates that the AsyncMetadataVersionManager correctly identifies the
     * version update. This is done by initializing the base metadata version
     * (for cluster.xml), starting the AsyncMetadataVersionManager and then
     * updating the version to a new value. For the test to succeed the callback
     * has to be invoked correctly by the asynchronous manager.
     */
    @Test(timeout = 60000)
    public void testBasicAsyncBehaviour() {
        String storeVersionKey = "cluster.xml";
        try {
            Callable<Void> rebootstrapCallback = new Callable<Void>() {

                public Void call() throws Exception {
                    callbackForClusterChange();
                    return null;
                }
            };

            // Write a base version of 100
            Properties versionProps = MetadataVersionStoreUtils.getProperties(this.sysVersionStore);
            versionProps.setProperty(storeVersionKey, Long.toString(100));
            MetadataVersionStoreUtils.setProperties(this.sysVersionStore, versionProps);

            // Giving enough time to complete the above put.
            Thread.sleep(500);

            // Starting the Version Metadata Manager
            this.asyncCheckMetadata = new AsyncMetadataVersionManager(this.repository,
                                                                      rebootstrapCallback,
                                                                      null);
            scheduler.schedule(asyncCheckMetadata.getClass().getName(),
                               asyncCheckMetadata,
                               new Date(),
                               500);

            // Wait until the Version Manager is active
            while(!asyncCheckMetadata.isActive) {
                Thread.sleep(500);
            }

            // Updating the version metadata here for the Version Metadata
            // Manager to detect
            this.newVersion = 101;
            System.err.println("Incrementing the version for : " + storeVersionKey);
            versionProps.setProperty(storeVersionKey, Long.toString(this.newVersion));
            MetadataVersionStoreUtils.setProperties(this.sysVersionStore, versionProps);

            while(!callbackDone) {
                Thread.sleep(2000);
            }

            assertEquals(this.updatedClusterVersion, this.newVersion);
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failed to start the Metadata Version Manager : " + e.getMessage());
        }
    }

    /*
     * Validates that the AsyncMetadataVersionManager correctly identifies the
     * store specific version update. This is done by initializing the base
     * metadata version (for a particular store), starting the
     * AsyncMetadataVersionManager and then updating the version to a new value.
     * For the test to succeed the callback has to be invoked correctly by the
     * asynchronous manager.
     */
    @Test(timeout = 60000)
    public void testStoreDefinitionChangeTracker() {
        String storeVersionKey = "users";
        Callable<Void> rebootstrapCallback = new Callable<Void>() {

            public Void call() throws Exception {
                callbackForStoreChange();
                return null;
            }
        };

        try {
            // Write a base version of 100
            Properties versionProps = MetadataVersionStoreUtils.getProperties(this.sysVersionStore);
            versionProps.setProperty(storeVersionKey, Long.toString(100));
            MetadataVersionStoreUtils.setProperties(this.sysVersionStore, versionProps);

            // Giving enough time to complete the above put.
            Thread.sleep(500);

            // Starting the Version Metadata Manager
            this.asyncCheckMetadata = new AsyncMetadataVersionManager(this.repository,
                                                                      rebootstrapCallback,
                                                                      storeVersionKey);
            scheduler.schedule(asyncCheckMetadata.getClass().getName(),
                               asyncCheckMetadata,
                               new Date(),
                               500);

            // Wait until the Version Manager is active
            while(!asyncCheckMetadata.isActive) {
                Thread.sleep(500);
            }

            // Updating the version metadata here for the Version Metadata
            // Manager to detect
            this.newVersion = 101;
            System.err.println("Incrementing the version for : " + storeVersionKey);
            versionProps.setProperty(storeVersionKey, Long.toString(this.newVersion));
            MetadataVersionStoreUtils.setProperties(this.sysVersionStore, versionProps);

            while(!callbackDone) {
                Thread.sleep(2000);
            }

            assertEquals(false, (this.updatedStoreVersion == 0));
            assertEquals(this.updatedStoreVersion, this.newVersion);
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failed to start the Metadata Version Manager : " + e.getMessage());
        }
    }

    /*
     * Validates that the AsyncMetadataVersionManager correctly identifies the
     * store specific version update in the presence of multiple stores. This is
     * done by initializing the base metadata version (for a particular store),
     * starting the AsyncMetadataVersionManager and then updating the version to
     * a new value. For the test to succeed the callback has to be invoked
     * correctly by the asynchronous manager.
     * 
     * This test also checks that callback is invoked only for the corresponding
     * store that was updated. Other callbacks should not be invoked.
     */
    @Test(timeout = 60000)
    public void testMultipleStoreDefinitionsChangeTracker() {
        String storeVersionKeys[] = new String[3];
        for(int i = 0; i < 3; i++) {
            storeVersionKeys[i] = "users" + i;
        }

        List<StoresVersionCallback> rebootstrapCallbacks = new ArrayList<AsyncMetadataVersionManagerTest.StoresVersionCallback>();
        for(int i = 0; i < 3; i++) {
            StoresVersionCallback callback = new StoresVersionCallback(i);
            rebootstrapCallbacks.add(callback);
        }

        try {
            // Write a base version of 100
            Properties versionProps = MetadataVersionStoreUtils.getProperties(this.sysVersionStore);
            for(int i = 0; i < 3; i++) {
                versionProps.setProperty(storeVersionKeys[i], Long.toString(100));
            }
            MetadataVersionStoreUtils.setProperties(this.sysVersionStore, versionProps);

            // Giving enough time to complete the above put.
            Thread.sleep(500);

            // Starting the Version Metadata Managers
            for(int i = 0; i < 3; i++) {
                AsyncMetadataVersionManager versionManager = new AsyncMetadataVersionManager(this.repository,
                                                                                             rebootstrapCallbacks.get(i),
                                                                                             storeVersionKeys[i]);
                asyncVersionManagers.add(versionManager);

                scheduler.schedule(versionManager.getClass().getName(),
                                   versionManager,
                                   new Date(),
                                   500);
            }

            // Wait until the Version Manager is active
            while(!asyncVersionManagers.get(0).isActive && !asyncVersionManagers.get(1).isActive
                  && !asyncVersionManagers.get(2).isActive) {
                Thread.sleep(500);
            }

            // Updating the version metadata here for the Version Metadata
            // Manager to detect
            this.newVersion = 101;

            // Set this new version on the store: 'users1'
            System.out.println("Incrementing the version for : " + storeVersionKeys[1]);
            versionProps.setProperty(storeVersionKeys[1], Long.toString(this.newVersion));
            MetadataVersionStoreUtils.setProperties(this.sysVersionStore, versionProps);

            while(!callbacksDone[0] && !callbacksDone[1] && !callbacksDone[2]) {
                Thread.sleep(2000);
            }

            // Check that version for index 1 has been udpated correctly.
            assertEquals(false, (this.updatedVersions[1] == 0));
            long updatedVersion = this.updatedVersions[1];
            assertEquals(updatedVersion, this.newVersion);

            // Check that versions for other indices have not changed
            assertEquals(true, (this.updatedVersions[0] == 0));
            assertEquals(true, (this.updatedVersions[2] == 0));
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failed to start the Metadata Version Manager : " + e.getMessage());
        }
    }

    private void callbackForClusterChange() {
        try {
            Long clusterVersion = this.asyncCheckMetadata.getClusterMetadataVersion();
            if(clusterVersion != null) {
                this.updatedClusterVersion = clusterVersion;
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in updating cluster.xml version: " + e.getMessage());
        } finally {
            this.callbackDone = true;
        }
    }

    private void callbackForStoreChange() {
        try {
            Long storeVersion = this.asyncCheckMetadata.getStoreMetadataVersion();
            if(storeVersion != null) {
                this.updatedStoreVersion = storeVersion;
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in updating store version: " + e.getMessage());
        } finally {
            this.callbackDone = true;
        }
    }

    private void callbacksForStoreChange(int index) {
        try {
            Long storeVersion = this.asyncVersionManagers.get(index).getStoreMetadataVersion();
            if(storeVersion != null) {
                this.updatedVersions[index] = storeVersion;
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("Error in updating store version: " + e.getMessage());
        } finally {
            this.callbacksDone[index] = true;
        }
    }

    class StoresVersionCallback implements Callable<Void> {

        private int index;

        public StoresVersionCallback(int index) {
            this.index = index;
        }

        @Override
        public Void call() throws Exception {
            callbacksForStoreChange(this.index);
            return null;
        }

    }

}
