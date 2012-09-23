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

package voldemort.store.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.SystemStore;
import voldemort.client.SystemStoreRepository;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.cluster.Cluster;
import voldemort.common.service.SchedulerService;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.SystemTime;

/**
 * Test class to verify the AsyncMetadataVersionManager
 * 
 * @author csoman
 * 
 */
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
    protected final int CLIENT_ZONE_ID = 0;
    private long newVersion = 0;

    private SystemStore<String, String> sysVersionStore;
    private SystemStoreRepository repository;
    private SchedulerService scheduler;
    private AsyncMetadataVersionManager asyncCheckMetadata;
    private boolean callbackDone = false;
    private long updatedStoresVersion;

    @Before
    public void setUp() throws Exception {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        servers = new VoldemortServer[2];

        servers[0] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(true,
                                                                                             0,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);
        servers[1] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(true,
                                                                                             1,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();

        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;
        sysVersionStore = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                          bootStrapUrls,
                                                          this.CLIENT_ZONE_ID);
        repository = new SystemStoreRepository();
        repository.addSystemStore(sysVersionStore,
                                  SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        this.scheduler = new SchedulerService(2, SystemTime.INSTANCE, true);
    }

    @After
    public void tearDown() throws Exception {
        servers[0].stop();
        servers[1].stop();
    }

    /*
     * Validates that the AsyncMetadataVersionManager correctly identifies the
     * version update. This is done by initializing the base metadata version
     * (for cluster.xml), starting the AsyncMetadataVersionManager and then
     * updating the version to a new value. For the test to succeed the callback
     * has to be invoked correctly by the asynchronous manager.
     */
    @Test
    public void testBasicAsyncBehaviour() {
        String storeVersionKey = "cluster.xml";
        try {
            Callable<Void> rebootstrapCallback = new Callable<Void>() {

                public Void call() throws Exception {
                    callback();
                    return null;
                }
            };

            // Write a base version of 100
            String existingVersions = this.sysVersionStore.getSysStore(AsyncMetadataVersionManager.VERSIONS_METADATA_STORE)
                                                          .getValue();
            existingVersions += storeVersionKey + "=100";
            this.sysVersionStore.putSysStore(AsyncMetadataVersionManager.VERSIONS_METADATA_STORE,
                                             existingVersions);

            // Giving enough time to complete the above put.
            Thread.sleep(500);

            // Starting the Version Metadata Manager
            this.asyncCheckMetadata = new AsyncMetadataVersionManager(this.repository,
                                                                      rebootstrapCallback);
            scheduler.schedule(asyncCheckMetadata.getClass().getName(),
                               asyncCheckMetadata,
                               new Date(),
                               500);

            // Wait until the Version Manager is active
            int maxRetries = 0;
            while(maxRetries < 3 && !asyncCheckMetadata.isActive) {
                Thread.sleep(500);
                maxRetries++;
            }

            // Updating the version metadata here for the Version Metadata
            // Manager to detect
            this.newVersion = 101;
            System.err.println("Incrementing the version for : " + storeVersionKey);
            existingVersions = this.sysVersionStore.getSysStore(AsyncMetadataVersionManager.VERSIONS_METADATA_STORE)
                                                   .getValue();
            existingVersions = existingVersions.replaceAll(storeVersionKey + "=100",
                                                           storeVersionKey + "=101");
            this.sysVersionStore.putSysStore(AsyncMetadataVersionManager.VERSIONS_METADATA_STORE,
                                             existingVersions);

            maxRetries = 0;
            while(maxRetries < 3 && !callbackDone) {
                Thread.sleep(2000);
                maxRetries++;
            }

            assertEquals(this.updatedStoresVersion, this.newVersion);
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failed to start the Metadata Version Manager : " + e.getMessage());
        }
    }

    private void callback() {
        try {
            Long clusterVersion = this.asyncCheckMetadata.getClusterMetadataVersion();
            if(clusterVersion != null) {
                this.updatedStoresVersion = clusterVersion;
            }
        } catch(Exception e) {
            fail("Error in updating stores.xml version: " + e.getMessage());
        } finally {
            this.callbackDone = true;
        }
    }
}
