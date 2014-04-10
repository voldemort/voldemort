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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

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
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.tools.admin.command.AdminCommand;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/*
 * This class tests that the quota operations work properly.
 * It starts with a cluster that has several stores, generates new stores by copy-and-modify
 * the store names, adds the new stores to the cluster and checks if all new stores are added.
 * Then it deletes the new stores and checks if the new stores are deleted correctly.
 */

public class StoreOperationsTest {

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
    public void testStoreAddAndDelete() throws Exception {
        // create new stores_key object
        final String newStoreXMLFilePrefix = "updated.stores";
        final String newStoreXMLFileSuffix = "xml";

        List<StoreDefinition> newStores = new ArrayList<StoreDefinition>();
        List<String> newStoreNames = Lists.newArrayList();
        for(StoreDefinition storeDef: stores) {
            StoreDefinitionBuilder sb = AdminToolTestUtils.storeDefToBuilder(storeDef);
            sb.setName(sb.getName() + "_new");
            newStores.add(sb.build());
            newStoreNames.add(sb.getName());
        }

        // create stores.xml
        File newStoresXMLFolder = TestUtils.createTempDir();
        File newStoreXMLFile = File.createTempFile(newStoreXMLFilePrefix,
                                                   newStoreXMLFileSuffix,
                                                   newStoresXMLFolder);
        FileWriter fwriter = new FileWriter(newStoreXMLFile);
        fwriter.write(new StoreDefinitionsMapper().writeStoreList(newStores));
        fwriter.close();

        // execute store-add command
        AdminCommand.executeCommand(new String[] { "store", "add", "-f",
                newStoreXMLFile.getAbsolutePath(), "-u", bsURL });

        // check if stores have been added
        Integer nodeId = adminClient.getAdminClientCluster().getNodes().iterator().next().getId();
        List<StoreDefinition> newStoresToVerify = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                             .getValue();
        for(StoreDefinition newStore: newStores) {
            assertTrue(newStoresToVerify.contains(newStore));
        }

        // execute store-delete command
        AdminCommand.executeCommand(new String[] { "store", "delete", "-s",
                Joiner.on(",").join(newStoreNames), "-u", bsURL, "--confirm" });

        // check if stores have been deleted
        newStoresToVerify = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId).getValue();
        for(StoreDefinition newStore: newStores) {
            assertTrue(!newStoresToVerify.contains(newStore));
        }
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
