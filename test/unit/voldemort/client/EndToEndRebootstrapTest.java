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

package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortAdminTool;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.SchedulerService;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.SystemTime;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

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
    private static final String STORES_KEY = "stores.xml";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    String[] bootStrapUrls = null;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private VoldemortServer[] servers;
    private ZenStoreClient<String, String> storeClient;
    SystemStoreClient<String, String> sysStoreVersion;
    SystemStoreClient<String, String> clientRegistryStore;
    private Cluster cluster;
    public static String socketUrl = "";
    protected final int CLIENT_ZONE_ID = 0;

    private SystemStoreRepository sysRepository;
    private SchedulerService scheduler;

    
    
    @Before
    public void setUp() throws Exception {
        final int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        true, // useNio
                                                        null,
                                                        storesXmlfile,
                                                        new Properties());

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

        sysRepository = new SystemStoreRepository(clientConfig);
        // Start up the scheduler
        scheduler = new SchedulerService(clientConfig.getAsyncJobThreadPoolSize(),
                                         SystemTime.INSTANCE,
                                         true);

        storeClient = new ZenStoreClient<String, String>(STORE_NAME,
                                                         null,
                                                         storeClientFactory,
                                                         3,
                                                         clientConfig.getClientContextName(),
                                                         0,
                                                         clientConfig,
                                                         scheduler,
                                                         sysRepository);

        SystemStoreClientFactory<String, String> systemStoreFactory = new SystemStoreClientFactory<String, String>(clientConfig);

        sysStoreVersion = systemStoreFactory.createSystemStore(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        clientRegistryStore = systemStoreFactory.createSystemStore(SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name());

    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }

        scheduler.stop();
        sysRepository.close();
    }
    
    private void sanityTestClientOps() {
        // Do a sample get, put to check client is correctly initialized.
        String key = "city";
        String value = "SF";
        try {
            storeClient.put(key, value);
            String received = storeClient.getValue(key);
            assertEquals(value, received);
        } catch(VoldemortException ve) {
            fail("Error in doing basic get, put");
        }
    }
    
    private String getPropertyFromClientInfo(String propertyName) {
        String bootstrapTime = "";
        try {
            String clientInfo = clientRegistryStore.getSysStore(storeClient.getClientId())
                                                    .getValue();

            Properties props = new Properties();
            props.load(new ByteArrayInputStream(clientInfo.getBytes()));
            bootstrapTime = props.getProperty(propertyName);
            assertNotNull(bootstrapTime);
        } catch(Exception e) {
            fail("Error in retrieving bootstrap time: " + e);
        }
        return bootstrapTime;
    }

    /*
     * Test to validate that the client bootstraps on metadata change. 
     * 1. Do some operations to validate that the client is correctly initialized.
     * 2. Update the cluster.xml using the Admin Tool (which should update the
     *    metadata version as well). 
     * 3. Verify that the client bootstraps after this update.
     * 4. Whether the client has automatically bootstrapped is verified by checking
     * the new bootstrap time in the client registry.
     */
    @Test
    public void testEndToEndRebootstrap() {
        try {
            sanityTestClientOps();
            // Get bootstraptime at start
            String bootstrapTime = getPropertyFromClientInfo("bootstrapTime");
            // Update cluster.xml metadata
            AdminClient adminClient = new AdminClient(bootStrapUrls[0],
                                                      new AdminClientConfig(),
                                                      new ClientConfig(),
                                                      CLIENT_ZONE_ID);
            for(Node node: cluster.getNodes()) {
                VoldemortAdminTool.executeSetMetadata(node.getId(),
                                                      adminClient,
                                                      CLUSTER_KEY,
                                                      new ClusterMapper().writeCluster(cluster));
            }
            // Wait for about 15 seconds to be sure
            try {
                Thread.sleep(15000);
            } catch(Exception e) {
                fail("Interrupted .");
            }
            // Get bootstraptime again
            String newBootstrapTime = getPropertyFromClientInfo("bootstrapTime");
            assertFalse(bootstrapTime.equals(newBootstrapTime));
            
            long origTime = Long.parseLong(bootstrapTime);
            long newTime = Long.parseLong(newBootstrapTime);
            assertTrue(newTime > origTime);

        } catch(Exception e) {
            fail("Error in validating end to end client rebootstrap : " + e);
        }
    }
    
    /*
     * Test to validate that the client bootstraps on metadata change. 
     * 1. Do some operations to validate that the client is correctly initialized.
     * 2. Update the <cluster.xml, stores.xml> pair using the Admin Tool. 
     * 3. Verify that the client bootstraps after this update.
     * 4. Whether the client has automatically bootstrapped is verified by checking
     *    the new bootstrap time in the client registry.
     */
    @Test
    public void testEndToEndRebootstrapWithSetMetadataPair() {
        try {
            sanityTestClientOps();
            // Get bootstraptime at start
            String bootstrapTime = getPropertyFromClientInfo("bootstrapTime");
            // Update cluster.xml metadata
            AdminClient adminClient = new AdminClient(bootStrapUrls[0],
                                                      new AdminClientConfig(),
                                                      new ClientConfig(),
                                                      CLIENT_ZONE_ID);
            StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefs = storeDefsMapper.readStoreList(new File(storesXmlfile));
            for (Node node: cluster.getNodes()) {
                VoldemortAdminTool.executeSetMetadataPair(node.getId(),
                                                          adminClient,
                                                          CLUSTER_KEY,
                                                          new ClusterMapper().writeCluster(cluster),
                                                          STORES_KEY,
                                                          storeDefsMapper.writeStoreList(storeDefs));
            }
            // Wait for about 15 seconds to be sure
            try {
                Thread.sleep(15000);
            } catch(Exception e) {
                fail("Interrupted .");
            }
            // Get bootstraptime again
            String newBootstrapTime = getPropertyFromClientInfo("bootstrapTime");
            assertFalse(bootstrapTime.equals(newBootstrapTime));
            
            long origTime = Long.parseLong(bootstrapTime);
            long newTime = Long.parseLong(newBootstrapTime);
            assertTrue(newTime > origTime);

        } catch(Exception e) {
            fail("Error in validating end to end client rebootstrap : " + e);
        }
    }
}
