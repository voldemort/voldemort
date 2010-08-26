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
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * InvalidMetadata can cause trouble for server side routing by not allowing
 * requests from random partitions on a server.
 * 
 * 
 */
public class ServerSideRoutingTest extends TestCase {

    private static int TEST_VALUES_SIZE = 1000;
    private static String testStoreName = "test-replication-memory";

    /**
     * TODO : enable this test after serversideRouting is fixed.
     * 
     * @throws IOException
     */
    @Test
    public void testServerSideRouting() throws IOException {
    // Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1
    // }, { 2, 3 } });
    //
    // // check with InvalidMetadata disabled.
    // VoldemortServer server0 = startServer(0, storesXmlfile, cluster, false);
    // VoldemortServer server1 = startServer(1, storesXmlfile, cluster, false);
    // checkServerSideRouting(server0, server1);
    //
    // // check with InvalidMetadata enabled.
    // server0 = startServer(0, storesXmlfile, cluster, true);
    // server1 = startServer(1, storesXmlfile, cluster, true);
    // checkServerSideRouting(server0, server1);
    }

    @SuppressWarnings("unused")
    private void checkServerSideRouting(VoldemortServer server0, VoldemortServer server1) {
        // create bunch of key-value pairs
        HashMap<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        // populate all entries in server1
        Store<ByteArray, byte[], byte[]> store = server1.getStoreRepository()
                                                        .getStorageEngine(testStoreName);
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
            if(!routing.routeRequest(entry.getKey().get()).contains(0)) {
                assertEquals("ServerSideRouting should return keys from other nodes.",
                             entry.getValue(),
                             store.get(entry.getKey(), null).get(0).getValue());
            }
        }
    }

    @SuppressWarnings("unused")
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
