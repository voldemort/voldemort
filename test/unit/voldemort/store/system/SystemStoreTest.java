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

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.SystemStore;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

/**
 * Test class to verify the SystemStore (used to interact with the system
 * metadata stores managed by the cluster).
 * 
 * @author csoman
 * 
 */
public class SystemStoreTest {

    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    String[] bootStrapUrls = null;
    private String clusterXml;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private VoldemortServer[] servers;
    private Cluster cluster;
    public static String socketUrl = "";
    protected final int CLIENT_ZONE_ID = 0;

    @Before
    public void setUp() throws Exception {
        final int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };

        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        servers = new VoldemortServer[2];
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        true, // useNio
                                                        null,
                                                        storesXmlfile,
                                                        new Properties());

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();

        ClientConfig clientConfig = new ClientConfig().setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(socketUrl);

        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;
        clusterXml = ((AbstractStoreClientFactory) socketFactory).bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);
    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    @Test
    public void testBasicStore() {
        try {
            SystemStore<String, String> sysVersionStore = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                                          bootStrapUrls,
                                                                                          this.CLIENT_ZONE_ID);
            long storesVersion = 1;
            sysVersionStore.putSysStore("stores.xml", Long.toString(storesVersion));
            long version = Long.parseLong(sysVersionStore.getValueSysStore("stores.xml"));
            assertEquals("Received incorrect version from the voldsys$_metadata_version system store",
                         storesVersion,
                         version);
        } catch(Exception e) {
            fail("Failed to create the default System Store : " + e.getMessage());
        }
    }

    @Test
    public void testCustomClusterXmlStore() {
        try {
            SystemStore<String, String> sysVersionStore = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                                          bootStrapUrls,
                                                                                          this.CLIENT_ZONE_ID,
                                                                                          this.clusterXml,
                                                                                          null);
            long storesVersion = 1;
            sysVersionStore.putSysStore("stores.xml", Long.toString(storesVersion));
            long version = Long.parseLong(sysVersionStore.getValueSysStore("stores.xml"));
            assertEquals("Received incorrect version from the voldsys$_metadata_version system store",
                         storesVersion,
                         version);
        } catch(Exception e) {
            fail("Failed to create System Store with custom cluster Xml: " + e.getMessage());
        }
    }

    @Test
    public void testIllegalSystemStore() {
        try {
            @SuppressWarnings("unused")
            SystemStore<String, Long> sysVersionStore = new SystemStore<String, Long>("test-store",
                                                                                      bootStrapUrls,
                                                                                      this.CLIENT_ZONE_ID,
                                                                                      this.clusterXml,
                                                                                      null);
            fail("Should not execute this. We can only connect to system store with a 'voldsys$' prefix.");
        } catch(Exception e) {
            // This is fine.
        }
    }
}
