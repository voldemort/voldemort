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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.quota.QuotaType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.tools.admin.command.AdminCommand;
import voldemort.versioning.Versioned;

/*
 * This class tests that the quota operations work properly.
 * It sets a quota on a store in the cluster where the store does not have a quota before.
 * Then it checks if the store has the quota setting after the operation.
 * After that, it unsets the quota on the store, and checks if the quota is cancelled.
 */

public class QuotaOperationsTest {

    HashMap<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    HashMap<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    String bsURL;
    Cluster cluster;
    List<StoreDefinition> stores;
    AdminClient adminClient;

    String storeName;
    String quotaType;

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
        adminClient = new AdminClient(cluster);
        storeName = stores.iterator().next().getName();
    }

    @Test
    public void testQuotaSetAndUnset() throws Exception {
        for(QuotaType quotaType: QuotaType.values()) {
            String quotaValueToSet = "1000";

            // set quota value
            AdminCommand.executeCommand(new String[] { "quota", "set",
                    quotaType + "=" + quotaValueToSet, "-s", storeName, "-u", bsURL, "--confirm" });

            // get quota value
            String quotaValueToVerify = adminClient.quotaMgmtOps.getQuota(storeName, quotaType)
                                                                .getValue();
            assertTrue(quotaValueToVerify.equals(quotaValueToSet));

            // unset quota value
            AdminCommand.executeCommand(new String[] { "quota", "unset", quotaType.toString(),
                    "-s", storeName, "-u", bsURL, "--confirm" });

            // get quota value
            Versioned<String> versionedQuotaValueToVerify = adminClient.quotaMgmtOps.getQuota(storeName,
                                                                                              quotaType);
            assertNull("Value retrieved should be null" + versionedQuotaValueToVerify,
                       versionedQuotaValueToVerify);
        }
    }

    @After
    public void teardown() throws IOException {
        // shutdown
        for(VoldemortServer vs: vservers.values()) {
            ServerTestUtils.stopVoldemortServer(vs);
        }
        for(SocketStoreFactory ssf: socketStoreFactories.values()) {
            ssf.close();
        }
    }
}
