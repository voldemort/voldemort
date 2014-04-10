/**
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

package voldemort.tools.admin;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.tools.admin.command.AdminCommand;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Sets;

/*
 * This class tests that the metadata operations work properly.
 * For testMetaGet(), it starts with a old cluster, fetches the stores.xml and cluster.xml
 * from the cluster, loads the metadata files, and compares the loaded metadata with those
 * used to start the cluster.
 * For testMetaSet(), it firstly tests whether all nodes have the same old metadata, create
 * new metadata file locally, set new metadata to the cluster, and checks whether the metadata
 * on cluster is different with the old metadata and same with new metadata. It also checks
 * whether all nodes have same new metadata at the end.
 */

public class MetaOperationsTest {

    HashMap<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    HashMap<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    String bsURL;
    Cluster cluster;
    List<StoreDefinition> stores;
    AdminClient adminClient;

    @Before
    public void setup() throws IOException {
        // setup cluster
        cluster = ServerTestUtils.getLocalCluster(2);
        stores = ServerTestUtils.getStoreDefs(2);
        bsURL = cluster.getNodes().iterator().next().getSocketUrl().toString();

        for(Node node: cluster.getNodes()) {
            SocketStoreFactory ssf = new ClientRequestExecutorPool(2, 10000, 100000, 1024);
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                                node.getId(),
                                                                                TestUtils.createTempDir()
                                                                                         .getAbsolutePath(),
                                                                                cluster,
                                                                                stores,
                                                                                new Properties());
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(ssf, config, cluster);
            vservers.put(node.getId(), vs);
            socketStoreFactories.put(node.getId(), ssf);
        }
        adminClient = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
    }

    @Test
    public void testMetaGet() throws Exception {
        Integer nodeId = 0;
        String tempDir = System.getProperty("java.io.tmpdir") + "temp" + System.currentTimeMillis();

        // get metadata and write to files
        AdminCommand.executeCommand(new String[] { "meta", "get",
                MetadataStore.CLUSTER_KEY + "," + MetadataStore.STORES_KEY, "-u", bsURL, "-n",
                nodeId.toString(), "-d", tempDir });

        // load metadata files
        String clusterFile = tempDir + "/" + MetadataStore.CLUSTER_KEY + "_" + nodeId;
        String storesFile = tempDir + "/" + MetadataStore.STORES_KEY + "_" + nodeId;

        assertTrue(Utils.isReadableFile(clusterFile));
        assertTrue(Utils.isReadableFile(storesFile));

        ClusterMapper clusterMapper = new ClusterMapper();
        StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();

        Cluster newCluster = clusterMapper.readCluster(new File(clusterFile));
        List<StoreDefinition> newStores = storeDefsMapper.readStoreList(new File(storesFile));

        // compare metadata objects
        assertTrue(newCluster.equals(cluster));
        assertTrue(newStores.size() == stores.size());
        for(StoreDefinition store: stores) {
            assertTrue(newStores.contains(store));
        }
    }

    @Test
    public void testMetaSet() throws Exception {
        // check if all old metadata are the same
        Set<Object> storesValues = Sets.newHashSet();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                        MetadataStore.STORES_KEY);
            assertTrue(versioned != null && versioned.getValue() != null);
            storesValues.add(new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue())));
        }
        assertTrue(storesValues.size() == 1);

        // create new metadata object
        final String newStoreXMLFilePrefix = "updated.stores";
        final String newStoreXMLFileSuffix = "xml";

        List<StoreDefinition> newStoresToUpload = new ArrayList<StoreDefinition>();
        for(StoreDefinition storeDef: stores) {
            StoreDefinitionBuilder sb = AdminToolTestUtils.storeDefToBuilder(storeDef);
            sb.setName(sb.getName() + "_new");
            newStoresToUpload.add(sb.build());
        }

        // create file of new metadata
        File newStoresXMLFolder = TestUtils.createTempDir();
        File newStoreXMLFile = File.createTempFile(newStoreXMLFilePrefix,
                                                   newStoreXMLFileSuffix,
                                                   newStoresXMLFolder);
        FileWriter fwriter = new FileWriter(newStoreXMLFile);
        fwriter.write(new StoreDefinitionsMapper().writeStoreList(newStoresToUpload));
        fwriter.close();

        // set new metadata from file
        AdminCommand.executeCommand(new String[] { "meta", "set",
                MetadataStore.STORES_KEY + "=" + newStoreXMLFile.getAbsolutePath(), "-u", bsURL,
                "--confirm" });

        // fetch new metadata from node
        List<StoreDefinition> newStoresToVerify = adminClient.metadataMgmtOps.getRemoteStoreDefList(adminClient.getAdminClientCluster()
                                                                                                               .getNodes()
                                                                                                               .iterator()
                                                                                                               .next()
                                                                                                               .getId())
                                                                             .getValue();

        // check if new metadata is the same as the source
        assertTrue(!newStoresToVerify.equals(stores));
        assertTrue(newStoresToVerify.equals(newStoresToUpload));

        // check if all new metadata are the same
        storesValues.clear();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                        MetadataStore.STORES_KEY);
            assertTrue(versioned != null && versioned.getValue() != null);
            storesValues.add(new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue())));
        }
        System.out.println(storesValues.size());
        assertTrue(storesValues.size() == 1);
    }

    @After
    public void teardown() {
        // shutdown
        for(VoldemortServer vs: vservers.values()) {
            vs.stop();
        }
        for(SocketStoreFactory ssf: socketStoreFactories.values()) {
            ssf.close();
        }
    }
}
