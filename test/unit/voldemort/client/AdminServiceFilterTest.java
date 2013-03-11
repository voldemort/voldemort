/*
 * Copyright 2008-2013 LinkedIn, Inc
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * 
 * Check the Filter class parameter in AdminServer stream API's <br>
 * 
 * 
 */
@RunWith(Parameterized.class)
public class AdminServiceFilterTest extends AbstractAdminServiceFilterTest {

    private static int TEST_KEYS = 10000;
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private AdminClient adminClient;
    private VoldemortServer server;
    protected Cluster cluster;
    private final boolean useNio;
    protected StoreDefinition storeDef;

    public AdminServiceFilterTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2);
        VoldemortConfig config = ServerTestUtils.createServerConfig(useNio,
                                                                    0,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile,
                                                                    new Properties());

        config.setEnableNetworkClassLoader(true);

        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile));
        storeDef = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs, testStoreName);

        server = new VoldemortServer(config, cluster);
        server.start();

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Override
    @After
    public void tearDown() throws IOException, InterruptedException {
        adminClient.close();
        server.stop();
        FileUtils.deleteDirectory(new File(server.getVoldemortConfig().getVoldemortHome()));
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
    protected Store<ByteArray, byte[], byte[]> getStore(int nodeId, String storeName) {
        return server.getStoreRepository().getStorageEngine(storeName);
    }

    @Override
    protected StoreDefinition getStoreDef() {
        return this.storeDef;
    }

    @Override
    protected Cluster getCluster() {
        return this.cluster;
    }

}
