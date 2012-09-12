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

package voldemort.utils;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class ServerTestUtilsTest {

    // private static String storesXmlfile = "test/common/voldemort/config/single-store.xml";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    @Before
    public void setUp() throws IOException {
        /*-
        Cluster cluster = ServerTestUtils.getLocalCluster(1, new int[][] { { 0, 1, 2, 3, 4, 5, 6,
                7, 8 } });
        */
        Cluster cluster = ServerTestUtils.getLocalCluster(8, new int[][] { { 0 }, { 1 }, { 2 },
                { 3 }, { 4 }, { 5 }, { 6 }, { 7 } });

        VoldemortServer[] servers = new VoldemortServer[8];

        // for(int i = 0; i < 1; i++) {
        for(int i = 0; i < 8; i++) {
            servers[i] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                              ServerTestUtils.createServerConfig(true,
                                                                                                 i,
                                                                                                 TestUtils.createTempDir()
                                                                                                          .getAbsolutePath(),
                                                                                                 null,
                                                                                                 storesXmlfile,
                                                                                                 new Properties()),
                                                              cluster);
        }
    }

    @Test
    public void startMultipleVoldemortServers() {
        assertTrue(true);
    }
}
