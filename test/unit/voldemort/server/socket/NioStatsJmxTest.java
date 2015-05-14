/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.server.socket;

import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.server.niosocket.NioSocketService;
import voldemort.store.Store;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
import voldemort.versioning.Versioned;

/**
 * Unit test for NIO selector connection stats
 * 
 */
public class NioStatsJmxTest {

    private VoldemortServer server;
    private Store<ByteArray, byte[], byte[]> socketStore;
    private static final int MAX_TRAFFIC_TIME_MS = 2000;

    @Before
    public void setUp() throws Exception {
        String storesXmlfile = "test/common/voldemort/config/single-store.xml";
        ClientConfig clientConfig = new ClientConfig().setMaxConnectionsPerNode(1).setMaxThreads(1);
        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(clientConfig.getSelectors(),
                                                                              clientConfig.getMaxConnectionsPerNode(),
                                                                              clientConfig.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                                              clientConfig.getSocketTimeout(TimeUnit.MILLISECONDS),
                                                                              clientConfig.getSocketBufferSize(),
                                                                              clientConfig.getSocketKeepAlive());

        Properties props = new Properties();
        props.put("jmx.enable", "true");

        int numServers = 1;
        VoldemortServer[] servers = new VoldemortServer[numServers];
        Cluster cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                                servers,
                                                                null,
                                                                socketStoreFactory,
                                                                true,
                                                                null,
                                                                storesXmlfile,
                                                                props);

        server = servers[0];

        for(Node node: cluster.getNodes()) {
            socketStore = ServerTestUtils.getSocketStore(socketStoreFactory,
                                                         "test",
                                                         node.getSocketPort(),
                                                         clientConfig.getRequestFormatType());
        }
    }

    @Test
    public void testActiveConnectionCount() throws Exception {
        // generate some traffic,
        Random dataGen = new Random();
        long start = System.currentTimeMillis();

        byte[] data = new byte[256];
        while(((System.currentTimeMillis()) - start) <= MAX_TRAFFIC_TIME_MS) {
            dataGen.nextBytes(data);
            ByteArray key = new ByteArray(data);
            socketStore.put(key, new Versioned<byte[]>(data), null);
        }

        // has to be 1, since we configure client with 1 connection and do
        // atleast one operation
        MBeanServer beanserver = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = JmxUtils.createObjectName(JmxUtils.getPackageName(NioSocketService.class),
                                                    "nio-socket-server");
        assertEquals(1, beanserver.getAttribute(name, "numActiveConnections"));
    }

    @After
    public void tearDown() {
        server.stop();
    }
}
