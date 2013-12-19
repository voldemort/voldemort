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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortAdminTool;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;


public class AtomicSetMetadataPairTest {

    private static final String CLUSTER_KEY = "cluster.xml";
    private static final String STORES_KEY = "stores.xml";
    
    private static String oldStoresXmlfile = "test/common/voldemort/config/stores.xml";
    private static String newStoresXmlfile = "test/common/voldemort/config/two-stores.xml";
    
    String[] bootStrapUrls = null;
    public static String socketUrl = "";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private VoldemortServer[] servers;
    private Cluster oldCluster;
 
    @Before
    public void setUp() throws Exception {
        final int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };
        oldCluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        true, // useNio
                                                        null,
                                                        oldStoresXmlfile,
                                                        new Properties());
        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;

    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }
   
    @Test
    public void testClusterAndStoresAreSetAtomically() {
        try {
        
            // Update cluster.xml metadata
            AdminClient adminClient = new AdminClient(bootStrapUrls[0],
                                                      new AdminClientConfig(),
                                                      new ClientConfig());
            StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefs = storeDefsMapper.readStoreList(new File(newStoresXmlfile));
            
            
            final int numServers = 2;
            int partitionMap[][] = { { 0, 1, 6, 7 }, { 4, 5, 2, 3 } };
            Cluster newCluster = ServerTestUtils.getLocalCluster(numServers, partitionMap);
            
            ClusterMapper clusterMapper = new ClusterMapper();
            
            for (Node node: oldCluster.getNodes()) {
                VoldemortAdminTool.executeSetMetadataPair(node.getId(),
                                                          adminClient,
                                                          CLUSTER_KEY,
                                                          clusterMapper.writeCluster(newCluster),
                                                          STORES_KEY,
                                                          storeDefsMapper.writeStoreList(storeDefs));
            }
            String dirPath = TestUtils.createTempDir().getAbsolutePath();
            
            for (Node node: oldCluster.getNodes()) {
                VoldemortAdminTool.executeGetMetadata(node.getId(), adminClient, CLUSTER_KEY, dirPath);
                // Make sure cluster metadata was updated                
                Cluster newClusterFromMetadataRepo = clusterMapper.readCluster(new File(dirPath, CLUSTER_KEY + "_" + node.getId()));
                assertTrue(newClusterFromMetadataRepo.getNodeById(0).getPartitionIds().equals(Lists.newArrayList(0, 1, 6, 7 )));
                // Make sure store metadata was updated
                VoldemortAdminTool.executeGetMetadata(node.getId(), adminClient, STORES_KEY, dirPath);
                List<StoreDefinition> newStoreDefsFromMetadatRepo = storeDefsMapper.readStoreList(new File(dirPath, STORES_KEY + "_" + node.getId()));
                assertTrue(newStoreDefsFromMetadatRepo.size() == 2); 
                assertTrue(newStoreDefsFromMetadatRepo.get(1).getName().equals("best"));
            }
        } catch(Exception e) {
            fail("Error in validating end to end client rebootstrap : " + e);
        }
    }
}
