/*
 * Copyright 2010-2012 LinkedIn, Inc.
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

package voldemort.cluster.failuredetector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortServer;
import voldemort.server.storage.StorageService;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

@RunWith(Parameterized.class)
public class ServerStoreVerifierTest {

    private final String storesXmlfile = "test/common/voldemort/config/single-store.xml";

    private final Map<Integer, VoldemortServer> serverMap = new HashMap<Integer, VoldemortServer>();

    private final boolean useNio;

    private Cluster cluster;

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    public ServerStoreVerifierTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        int numServers = 2;
        VoldemortServer[] servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0 }, { 1 } };
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        useNio,
                                                        null,
                                                        storesXmlfile,
                                                        new Properties());

        for(int i = 0; i < numServers; i++) {
            serverMap.put(i, servers[i]);
        }
    }

    @After
    public void tearDown() throws IOException {
        for(int i: serverMap.keySet()) {
            try {
                ServerTestUtils.stopVoldemortServer(serverMap.get(i));
            } catch(VoldemortException e) {
                // ignore these at stop time
            }
        }

        socketStoreFactory.close();
    }

    @Test
    public void testMetadataStore() throws Exception {
        for(Node node: cluster.getNodes()) {
            VoldemortServer voldemortServer = serverMap.get(node.getId());
            StorageService ss = (StorageService) voldemortServer.getService(ServiceType.STORAGE);
            ServerStoreVerifier ssv = new ServerStoreVerifier(ss.getSocketStoreFactory(),
                                                              voldemortServer.getMetadataStore(),
                                                              voldemortServer.getVoldemortConfig());

            for(Node siblingNodes: cluster.getNodes())
                ssv.verifyStore(siblingNodes);
        }
    }

}
