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

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import voldemort.*;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStoreTest;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.xml.StoreDefinitionsMapper;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

public class AddStoreTest {
    private Logger logger = Logger.getLogger(AddStoreTest.class);

    HashMap<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    HashMap<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    String bsURL;
    Cluster cluster;
    List<StoreDefinition> oldStores;
    AdminClient adminClient;

    public static String storeXmlWithBackwardIncompatibleSchema = "  <store>\n" +
            "    <name>test</name>\n" +
            "    <persistence>bdb</persistence>\n" +
            "    <description>Test store</description>\n" +
            "    <owners>harry@hogwarts.edu, hermoine@hogwarts.edu</owners>\n" +
            "    <routing-strategy>consistent-routing</routing-strategy>\n" +
            "    <routing>client</routing>\n" +
            "    <replication-factor>1</replication-factor>\n" +
            "    <required-reads>1</required-reads>\n" +
            "    <required-writes>1</required-writes>\n" +
            "      <key-serializer>\n" +
            "          <type>avro-generic-versioned</type>\n" +
            "          <schema-info version=\"0\">\"int32\"</schema-info>\n" +
            "      </key-serializer>\n" +
            "      <value-serializer>\n" +
            "          <type>avro-generic-versioned</type>\n" +
            "          <schema-info version=\"0\">\"int\"</schema-info>\n" +
            "          <schema-info version=\"1\">\"string\"</schema-info>\n" +
            "      </value-serializer>\n" +
            "    <hinted-handoff-strategy>consistent-handoff</hinted-handoff-strategy>\n" +
            "  </store>\n";

    public static String storeXmlWithBackwardCompatibleSchema = "  <store>\n" +
            "    <name>test</name>\n" +
            "    <persistence>bdb</persistence>\n" +
            "    <description>Test store</description>\n" +
            "    <owners>harry@hogwarts.edu, hermoine@hogwarts.edu</owners>\n" +
            "    <routing-strategy>consistent-routing</routing-strategy>\n" +
            "    <routing>client</routing>\n" +
            "    <replication-factor>1</replication-factor>\n" +
            "    <required-reads>1</required-reads>\n" +
            "    <required-writes>1</required-writes>\n" +
            "      <key-serializer>\n" +
            "          <type>avro-generic-versioned</type>\n" +
            "          <schema-info version=\"0\">\"int32\"</schema-info>\n" +
            "      </key-serializer>\n" +
            "      <value-serializer>\n" +
            "          <type>avro-generic-versioned</type>\n" +
            "          <schema-info version=\"0\">\"int\"</schema-info>\n" +
            "          <schema-info version=\"1\">\"int\"</schema-info>\n" +
            "      </value-serializer>\n" +
            "    <hinted-handoff-strategy>consistent-handoff</hinted-handoff-strategy>\n" +
            "  </store>\n";


    /**
     * This test is to partially test the functionality of SetMetadata feature
     * of the VoldemortAdminTool
     */

    @Before
    public void setup() throws IOException {
        // setup cluster
        cluster = ClusterTestUtils.getZZZCluster();
        oldStores = ClusterTestUtils.getZZZStoreDefsInMemory();
        bsURL = cluster.getNodes().iterator().next().getSocketUrl().toString();

        for(Node node: cluster.getNodes()) {
            SocketStoreFactory ssf = new ClientRequestExecutorPool(2, 10000, 100000, 1024);
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                    node.getId(),
                    TestUtils.createTempDir()
                            .getAbsolutePath(),
                    cluster,
                    oldStores,
                    new Properties());
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(ssf, config, cluster);
            vservers.put(node.getId(), vs);
            socketStoreFactories.put(node.getId(), ssf);
        }
        adminClient = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
    }

    @Test
    public void testAddSchemaBackwardIncompatibleStore() throws Exception {
        try{
            logger.info("Now inserting stores with non backward compatible schema. Should see exception");
            adminClient.storeMgmtOps.addStore(new StoreDefinitionsMapper().readStore(new StringReader(storeXmlWithBackwardIncompatibleSchema)));
            Assert.fail("Did not throw exception");
        } catch(VoldemortException e) {

        }
        for (VoldemortServer vs: vservers.values()) {
            assertNull(vs.getStoreRepository().getLocalStore("test"));
        }
        logger.info("Now inserting stores with backward compatible schema. Should not see exception");
        adminClient.storeMgmtOps.addStore(new StoreDefinitionsMapper().readStore(new StringReader(storeXmlWithBackwardCompatibleSchema)));

        for (VoldemortServer vs: vservers.values()) {
            assertNotNull(vs.getStoreRepository().getLocalStore("test"));
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
