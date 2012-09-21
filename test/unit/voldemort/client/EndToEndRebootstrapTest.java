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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortAdminTool;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.SchedulerService;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.SystemTime;
import voldemort.xml.ClusterMapper;

/**
 * Test class to verify that the Zenstore client rebootstraps when needed (on
 * change of cluster.xml)
 * 
 * @author csoman
 * 
 */
public class EndToEndRebootstrapTest {

    private static final String STORE_NAME = "test-replication-persistent";
    private static final String CLUSTER_KEY = "cluster.xml";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    String[] bootStrapUrls = null;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private VoldemortServer[] servers;
    private ZenStoreClient<String, String> storeClient;
    SystemStore<String, String> sysStoreVersion;
    SystemStore<String, String> clientRegistryStore;
    private Cluster cluster;
    public static String socketUrl = "";
    protected final int CLIENT_ZONE_ID = 0;

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

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClientRegistryUpdateIntervalInSecs(5);
        clientConfig.setAsyncMetadataRefreshInMs(5000);
        clientConfig.setBootstrapUrls(bootstrapUrl);
        SocketStoreClientFactory storeClientFactory = new SocketStoreClientFactory(clientConfig);

        SchedulerService service = new SchedulerService(clientConfig.getAsyncJobThreadPoolSize(),
                                                        SystemTime.INSTANCE,
                                                        true);
        storeClient = new ZenStoreClient<String, String>(STORE_NAME,
                                                         null,
                                                         storeClientFactory,
                                                         3,
                                                         clientConfig.getClientContextName(),
                                                         0,
                                                         clientConfig,
                                                         service);
        sysStoreVersion = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                          bootStrapUrls,
                                                          0);
        clientRegistryStore = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                              bootStrapUrls,
                                                              0);

    }

    @After
    public void tearDown() throws Exception {
        servers[0].stop();
        servers[1].stop();
    }

    /*
     * Test to validate that the client bootstraps on metadata change. First do
     * some operations to validate that the client is correctly initialized.
     * Then update the cluster.xml using the Admin Tool (which should update the
     * metadata version as well). Verify that the client bootstraps after this
     * update.
     * 
     * Whether the client has automatically bootstrapped is verified by checking
     * the new bootstrap time in the client registry.
     */
    @Test
    public void testEndToEndRebootstrap() {
        try {
            // Do a sample get, put to check client is correctly initialized.
            String key = "city";
            String value = "SF";
            String bootstrapTime = "";
            String newBootstrapTime = "";
            AdminClient adminClient = new AdminClient(bootStrapUrls[0],
                                                      new AdminClientConfig(),
                                                      CLIENT_ZONE_ID);

            try {
                storeClient.put(key, value);
                String received = storeClient.getValue(key);
                assertEquals(value, received);
            } catch(VoldemortException ve) {
                fail("Error in doing basic get, put");
            }

            String originalClientInfo = null;

            try {
                originalClientInfo = clientRegistryStore.getSysStore(storeClient.getClientId())
                                                        .getValue();

                Properties props = new Properties();
                props.load(new ByteArrayInputStream(originalClientInfo.getBytes()));

                bootstrapTime = props.getProperty("bootstrapTime");
                assertNotNull(bootstrapTime);
            } catch(Exception e) {
                fail("Error in retrieving bootstrap time: " + e);
            }

            // Update cluster.xml metadata
            VoldemortAdminTool adminTool = new VoldemortAdminTool();
            ClusterMapper mapper = new ClusterMapper();
            for(Node node: cluster.getNodes()) {
                VoldemortAdminTool.executeSetMetadata(node.getId(),
                                                      adminClient,
                                                      CLUSTER_KEY,
                                                      mapper.writeCluster(cluster));
            }

            // Wait for about 15 seconds to be sure
            try {
                Thread.sleep(15000);
            } catch(Exception e) {
                fail("Interrupted .");
            }

            // // Retrieve the new client bootstrap timestamp
            String newClientInfo = null;

            try {
                newClientInfo = clientRegistryStore.getSysStore(storeClient.getClientId())
                                                   .getValue();
                Properties newProps = new Properties();
                newProps.load(new ByteArrayInputStream(newClientInfo.getBytes()));
                newBootstrapTime = newProps.getProperty("bootstrapTime");
                assertNotNull(newBootstrapTime);
            } catch(Exception e) {
                fail("Error in retrieving bootstrap time: " + e);
            }

            assertFalse(bootstrapTime.equals(newBootstrapTime));
            long origTime = Long.parseLong(bootstrapTime);
            long newTime = Long.parseLong(newBootstrapTime);
            assertTrue(newTime > origTime);

        } catch(Exception e) {
            fail("Error in validating end to end client rebootstrap : " + e);
        }
    }
}
