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

package voldemort.store.invalidmetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * InvalidMetadata can cause trouble for server side routing by not allowing
 * requests from random partitions on a server.
 * 
 */
@RunWith(Parameterized.class)
public class ServerSideRoutingTest extends TestCase {

    private static int TEST_VALUES_SIZE = 1000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private final boolean useNio;
    private final boolean enableMetadataChecking;
    private VoldemortServer servers[];

    public ServerSideRoutingTest(boolean useNio, boolean enableMetadataChecking) {
        this.useNio = useNio;
        this.enableMetadataChecking = enableMetadataChecking;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false, false }, { false, true }, { true, false },
                { true, true } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1 }, { 2, 3 } });

        servers = new VoldemortServer[2];
        servers[0] = startServer(useNio, 0, storesXmlfile, cluster, enableMetadataChecking);
        servers[1] = startServer(useNio, 1, storesXmlfile, cluster, enableMetadataChecking);
    }

    @Override
    @After
    public void tearDown() {
        for(int i = 0; i < servers.length; i++) {
            try {
                ServerTestUtils.stopVoldemortServer(servers[i]);
            } catch(VoldemortException e) {
                // ignore these at stop time
            } catch(IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testServerSideRouting() {
        checkServerSideRouting(servers[0], servers[1]);
    }

    private void checkServerSideRouting(VoldemortServer server0, VoldemortServer server1) {
        // create bunch of key-value pairs
        HashMap<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        // populate all entries in server1
        Store<ByteArray, byte[], byte[]> store = server1.getStoreRepository()
                                                        .getRoutedStore(testStoreName);
        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            store.put(entry.getKey(),
                      Versioned.value(entry.getValue(),
                                      new VectorClock().incremented(0, System.currentTimeMillis())),
                      null);
        }

        // try fetching them from server0
        store = server0.getStoreRepository().getLocalStore(testStoreName);
        RoutingStrategy routing = server0.getMetadataStore().getRoutingStrategy(testStoreName);

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Node> nodes = routing.routeRequest(entry.getKey().get());
            if(hasNode(nodes, 0)) {
                assertTrue("ServerSideRouting should return keys from other nodes.",
                           ByteUtils.compare(entry.getValue(), store.get(entry.getKey(), null)
                                                                    .get(0)
                                                                    .getValue()) == 0);
            }
        }
    }

    /**
     * Checks if a list of nodes contains a node with the passed node id
     * 
     */
    private boolean hasNode(List<Node> nodes, int nodeId) {
        for(Node node: nodes) {
            if(node.getId() == nodeId) {
                return true;
            }
        }
        return false;
    }

    private VoldemortServer startServer(boolean useNio,
                                        int node,
                                        String storesXmlfile,
                                        Cluster cluster,
                                        boolean metadataChecking) throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfig(useNio,
                                                                    node,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile,
                                                                    new Properties());

        if(metadataChecking)
            config.setEnableMetadataChecking(true);
        else
            config.setEnableMetadataChecking(false);

        // set server side routing true.
        config.setEnableServerRouting(true);

        VoldemortServer server = new VoldemortServer(config, cluster);
        server.start();
        return server;
    }
}
