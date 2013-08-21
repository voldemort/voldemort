/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.server.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.SchedulerService;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

/*
 * This class tests the repair job tool. The basic workflow is as follows :
 * 1. Start a 9 node cluster with a single322 storedef.
 * 2. Generate 1024 random <key,value> pairs and do puts of all these 1024 on all the servers.
 *    At this time every server will have all the data. 
 * 3. Run the repair job on all 9 nodes.
 * 4. Now, for each key check that the repair job deleted that key from nodes that do not belong 
 *    to the preferenceList for that key.
 * 5. Verify that the key exists on nodes that belong to preferenceList.
 */

public class RepairJobTest {

    private Cluster cluster;
    private StoreRepository storeRepository;
    private StorageService storage;
    private SchedulerService scheduler;
    private List<StoreDefinition> storeDefs;
    private MetadataStore metadataStore;
    private ScanPermitWrapper scanPermitWrapper;
    private SocketStoreFactory socketStoreFactory;
    private Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap;

    public void setUp() {

        File temp = TestUtils.createTempDir();
        VoldemortConfig config = new VoldemortConfig(0, temp.getAbsolutePath());
        new File(config.getMetadataDirectory()).mkdir();
        this.scheduler = new SchedulerService(1, new MockTime());
        this.cluster = VoldemortTestConstants.getNineNodeCluster();
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        this.storeDefs = mapper.readStoreList(new StringReader((VoldemortTestConstants.getSingleStore322Xml())));
        this.storeRepository = new StoreRepository();
        this.metadataStore = ServerTestUtils.createMetadataStore(cluster, storeDefs);
        storage = new StorageService(storeRepository, metadataStore, scheduler, config);
        // Start the storage service
        storage.start();
        this.scanPermitWrapper = new ScanPermitWrapper(1);
        this.socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
        String storeDefsString = mapper.writeStoreList(storeDefs);
        File file = null;
        try {
            file = File.createTempFile("single-store-", ".xml");
            FileUtils.writeStringToFile(file, storeDefsString);
            String storeDefFile = file.getAbsolutePath();
            List<Integer> nodesToStart = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8);
            // Start the servers
            startServers(cluster, storeDefFile, nodesToStart, null);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private Cluster startServers(Cluster cluster,
                                 String storeXmlFile,
                                 List<Integer> nodeToStart,
                                 Map<String, String> configProps) throws Exception {
        for (int node: nodeToStart) {
            Properties properties = new Properties();
            if (null != configProps) {
                for (Entry<String, String> property: configProps.entrySet()) {
                    properties.put(property.getKey(), property.getValue());
                }
            }
            VoldemortConfig config = ServerTestUtils.createServerConfig(true,
                                                                        node,
                                                                        TestUtils.createTempDir().getAbsolutePath(),
                                                                        null,
                                                                        storeXmlFile,
                                                                        properties);
            ServerTestUtils.startVoldemortServer(socketStoreFactory, config, cluster);
        }
        return cluster;
    }

    private Store<ByteArray, byte[], byte[]> getSocketStore(String storeName,
                                                              String host,
                                                              int port) {
        return ServerTestUtils.getSocketStore(socketStoreFactory,
                                              storeName,
                                              host,
                                              port,
                                              RequestFormatType.VOLDEMORT_V1,
                                              false);
    }

    private Map<Integer, Store<ByteArray, byte[], byte[]>> createSocketStore(StoreDefinition storeDef) {
        storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        for (Node node: cluster.getNodes()) {
            storeMap.put(node.getId(), getSocketStore(storeDef.getName(), node.getHost(), node.getSocketPort()));
        }
        return storeMap;
    }
    
    private HashMap<String, String> populateData(HashMap<String, String> testEntries) {
        for (Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
            List<Integer> allNodes = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8);
            for (int nodeId: allNodes) {
                try {
                    storeMap.get(nodeId)
                            .put(keyBytes,
                                 new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(), "UTF-8")),
                                 null);
                } catch (Exception e) {
                 // Don't do anything with the exception. Exception are expected here as we are
                 // putting all keys on all nodes.
                }
            }
        }
        for (Store<ByteArray, byte[], byte[]> store: storeMap.values()) {
            store.close();
        }
        return testEntries;
    }

    @Test
    public void testRepairJob() {
        // start the servers
        setUp();
        // Create socket store
        storeMap = createSocketStore(storeDefs.get(0));
        // Generate random data, populate cluster with it.
        HashMap<String, String> testEntries = ServerTestUtils.createRandomKeyValueString(1024);
        populateData(testEntries);
        
        // create admin client and run repair on all nodes
        AdminClient admin = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
        for(int i = 0; i < 9; i++) {
            admin.storeMntOps.repairJob(i);
        }
        
        BaseStoreRoutingPlan storeInstance = new BaseStoreRoutingPlan(cluster, storeDefs.get(0));
        for (Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
            List<Integer> preferenceNodes = storeInstance.getReplicationNodeList(keyBytes.get());
            List<Integer> allNodes = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8));
            
            // Repair job should have deleted the keys on the nodes that shouldn't have been 
            // hosting the key. Go over all these remaining nodes to make sure that it's true.
            allNodes.removeAll(preferenceNodes);
            for (int nodeId: allNodes) {
                try {
                    List<Versioned<byte[]>> retVal = storeMap.get(nodeId).get(keyBytes, null);
                    assertEquals("Repair did not run properly as it left the key it should have" 
                                 + " deleted", retVal.isEmpty(), true);
                } catch (Exception e) {
                    // We expect a bunch of invalidmetadata exceptions as we are asking for key
                    // that doesn't belong to the nodes. Hence leaving the catch empty.
                }
            }
            
          // The repair job should not have deleted the keys from nodes on the pref list.
            for (int nodeId: preferenceNodes) {
                try {
                    List<Versioned<byte[]>> retVal = storeMap.get(nodeId).get(keyBytes, null);
                    assertEquals("Repair job has deleted keys that it should not have", 
                                 retVal.isEmpty(), false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        for (Store<ByteArray, byte[], byte[]> store: storeMap.values()) {
            store.close();
        }
    }
}
