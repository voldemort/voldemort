/**
 * Copyright 2014 LinkedIn, Inc
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
package voldemort.tools;

import org.junit.Test;
import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.TestSocketStoreFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ZoneShrinkageCLITest {


    HashMap<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    HashMap<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    String bsURL;
    Cluster cluster;
    List<StoreDefinition> oldStores;

    public void setup() throws IOException {
        // setup cluster
        bsURL = cluster.getNodes().iterator().next().getSocketUrl().toString();

        for(Node node: cluster.getNodes()) {
            SocketStoreFactory ssf = new TestSocketStoreFactory();
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(
                    true,
                    node.getId(),
                    TestUtils.createTempDir().getAbsolutePath(),
                    cluster,
                    oldStores,
                    new Properties()
            );
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(ssf, config, cluster);
            vservers.put(node.getId(), vs);
            socketStoreFactories.put(node.getId(), ssf);
        }
    }

    @Test
    public void testZoneShrinkageCLI() throws Exception {

        cluster = ClusterTestUtils.getZZZCluster();
        oldStores = ClusterTestUtils.getZZZStoreDefsInMemory();

        setup();

        String[] argv = ("--url " + bsURL +" --drop-zoneid 0 --real-run").split(" ");
        ZoneShrinkageCLI.main(argv);

        AdminClient adminClient = new AdminClient(bsURL, new AdminClientConfig(), new ClientConfig());
        assertEquals(2, adminClient.getAdminClientCluster().getZoneIds().size());


        String bootstrapUrl = adminClient.getAdminClientCluster().getNodes().iterator().next().getSocketUrl().toString();
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        StoreClient<String, String> client = factory.getStoreClient(oldStores.get(0).getName());
        client.put("k1", "v1" );
        assertEquals("v1", client.get("k1").getValue());
    }

    @Test
    public void testZoneShrinkageCLIWithNonContigiousZone() throws Exception {

        cluster = ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds();
        oldStores = ClusterTestUtils.getZ1Z3111StoreDefs("memory");

        setup();

        String[] argv = ("--url " + bsURL +" --drop-zoneid 1 --real-run").split(" ");
        ZoneShrinkageCLI.main(argv);

        AdminClient adminClient = new AdminClient(bsURL, new AdminClientConfig(), new ClientConfig());
        assertEquals(1, adminClient.getAdminClientCluster().getZoneIds().size());
    }
}
