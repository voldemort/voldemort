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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * 
 * Check the Filter class parameter in AdminServer stream API's <br>
 * 
 * @author bbansal
 * 
 */
public class AdminServiceFilterTest extends AbstractAdminServiceFilterTest {

    private static int TEST_KEYS = 10000;
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private AdminClient adminClient;
    private VoldemortServer server;
    private Cluster cluster;

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2);
        VoldemortConfig config = ServerTestUtils.createServerConfig(0,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile);
        server = new VoldemortServer(config, cluster);
        server.start();

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        adminClient.stop();
        server.stop();
    }

    @Override
    protected Set<Pair<ByteArray, Versioned<byte[]>>> createEntries() {
        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = new HashSet<Pair<ByteArray, Versioned<byte[]>>>();
        for(Entry<ByteArray, byte[]> entry: ServerTestUtils.createRandomKeyValuePairs(TEST_KEYS)
                                                           .entrySet()) {
            entrySet.add(new Pair<ByteArray, Versioned<byte[]>>(entry.getKey(),
                                                                new Versioned<byte[]>(entry.getValue())));
        }

        return entrySet;
    }

    @Override
    protected AdminClient getAdminClient() {
        return adminClient;
    }

    @Override
    protected Store<ByteArray, byte[]> getStore(int nodeId, String storeName) {
        return server.getStoreRepository().getStorageEngine(storeName);
    }

}
