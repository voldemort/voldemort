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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.ServerJVMTestUtils;
import voldemort.versioning.Versioned;

/**
 * Multiple JVM test for {@link AbstractAdminServiceFilterTest}
 * 
 */
@RunWith(Parameterized.class)
@Ignore
public class AdminServiceMultiJVMTest extends AbstractAdminServiceFilterTest {

    private static int TEST_KEYS = 10000;
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private Cluster cluster;
    private Process pid;
    private AdminClient adminClient;
    private String voldemortHome;
    private final boolean useNio;

    public AdminServiceMultiJVMTest(boolean useNio) {
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
        voldemortHome = ServerJVMTestUtils.createAndInitializeVoldemortHome(useNio,
                                                                            0,
                                                                            storesXmlfile,
                                                                            cluster);
        pid = ServerJVMTestUtils.startServerJVM(cluster.getNodeById(0), voldemortHome);
        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Override
    @After
    public void tearDown() throws IOException {
        System.out.println("teardown called");
        adminClient.stop();
        ServerJVMTestUtils.StopServerJVM(pid);
        FileUtils.deleteDirectory(new File(voldemortHome));
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
        Node node = cluster.getNodeById(nodeId);

        return ServerTestUtils.getSocketStore(storeName, node.getSocketPort());
    }
}
