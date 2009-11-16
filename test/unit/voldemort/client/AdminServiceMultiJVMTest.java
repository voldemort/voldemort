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
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * Multiple JVM test for {@link AbstractAdminServiceFilterTest}
 * 
 * @author bbansal
 */
public class AdminServiceMultiJVMTest extends AbstractAdminServiceFilterTest {

    private static int TEST_KEYS = 10000;
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private Cluster cluster;
    private Process pid;
    private AdminClient adminClient;
    private String voldemortHome;

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2);
        voldemortHome = createAndInitializeVoldemortHome(0, storesXmlfile, cluster);
        pid = startServerAsNewJVM(0, voldemortHome);
        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    private Process startServerAsNewJVM(int nodeId, String voldemortHome) throws IOException {
        String command = "java -cp " + System.getProperty("java.class.path")
                         + " voldemort.server.VoldemortServer " + voldemortHome;
        System.out.println("command:" + command);
        Process process = Runtime.getRuntime().exec(command);
        waitForServerStart(cluster.getNodeById(nodeId));
        return process;
    }

    private void waitForServerStart(Node node) {
        boolean success = false;
        int retries = 10;
        Store store = null;
        while(retries-- > 0) {
            store = ServerTestUtils.getSocketStore(MetadataStore.METADATA_STORE_NAME,
                                                   node.getSocketPort());
            try {
                store.get(new ByteArray(MetadataStore.CLUSTER_KEY.getBytes()));
                success = true;
            } catch(UnreachableStoreException e) {
                store.close();
                store = null;
                System.out.println("UnreachableSocketStore sleeping will try again " + retries
                                   + " times.");
                sleep(1000);
            }
        }

        store.close();
        if(!success)
            throw new RuntimeException("Failed to connect with server:" + node);
    }

    @Override
    public void tearDown() throws IOException {
        System.out.println("teardown called");
        adminClient.stop();
        StopServerJVM(pid);
        FileUtils.deleteDirectory(new File(voldemortHome));
    }

    private void StopServerJVM(Process server) throws IOException {
        System.out.println("killing process" + server);

        server.destroy();

        try {
            server.waitFor();
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String createAndInitializeVoldemortHome(int node, String storesXmlfile, Cluster cluster)
            throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfig(node,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile);

        // Initialize voldemort config dir with all required files.
        // cluster.xml
        File clusterXml = new File(config.getMetadataDirectory() + File.separator + "cluster.xml");
        FileUtils.writeStringToFile(clusterXml, new ClusterMapper().writeCluster(cluster));

        // stores.xml
        File storesXml = new File(config.getMetadataDirectory() + File.separator + "stores.xml");
        FileUtils.copyFile(new File(storesXmlfile), storesXml);

        // server.properties
        File serverProperties = new File(config.getMetadataDirectory() + File.separator
                                         + "server.properties");
        FileUtils.writeLines(serverProperties, Arrays.asList("node.id=" + node,
                                                             "bdb.cache.size=" + 1024 * 1024,
                                                             "enable.metadata.checking=" + false));

        return config.getVoldemortHome();
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
        Node node = cluster.getNodeById(nodeId);

        return ServerTestUtils.getSocketStore(storeName, node.getSocketPort());
    }

    private void sleep(int milisec) {
        try {
            Thread.sleep(milisec);
        } catch(InterruptedException e1) {
            // ignore
        }
    }
}
