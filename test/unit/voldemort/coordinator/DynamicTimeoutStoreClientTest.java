/*
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

package voldemort.coordinator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.rest.coordinator.DynamicTimeoutStoreClient;
import voldemort.server.VoldemortServer;
import voldemort.store.CompositeGetVoldemortRequest;
import voldemort.store.CompositePutVoldemortRequest;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * Class to test the Fat Client wrapper
 */
public class DynamicTimeoutStoreClientTest {

    private VoldemortServer[] servers;
    private Cluster cluster;
    public static String socketUrl = "";
    private static final String STORE_NAME = "slow-store-test";
    private static final String STORES_XML = "test/common/voldemort/config/single-slow-store.xml";
    private static final String SLOW_STORE_DELAY = "500";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private DynamicTimeoutStoreClient<ByteArray, byte[]> dynamicTimeoutClient = null;

    /**
     * Setup a one node Voldemort cluster with a 'slow' store
     * (SlowStorageEngine) with a delay of 500 ms for get and put.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        int numServers = 1;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 2, 4, 6, 1, 3, 5, 7 } };
        Properties props = new Properties();
        props.setProperty("storage.configs",
                          "voldemort.store.bdb.BdbStorageConfiguration,voldemort.store.slow.SlowStorageConfiguration");
        props.setProperty("testing.slow.queueing.get.ms", SLOW_STORE_DELAY);
        props.setProperty("testing.slow.queueing.put.ms", SLOW_STORE_DELAY);

        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        true, // useNio
                                                        null,
                                                        STORES_XML,
                                                        props);

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        String bootstrapUrl = socketUrl;
        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                      .setEnableCompressionLayer(false)
                                                      .setEnableSerializationLayer(false)
                                                      .enableDefaultClient(true)
                                                      .setEnableLazy(false);

        String storesXml = FileUtils.readFileToString(new File(STORES_XML), "UTF-8");
        ClusterMapper mapper = new ClusterMapper();

        this.dynamicTimeoutClient = new DynamicTimeoutStoreClient<ByteArray, byte[]>(STORE_NAME,
                                                                                     new SocketStoreClientFactory(clientConfig),
                                                                                     1,
                                                                                     storesXml,
                                                                                     mapper.writeCluster(cluster));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        if(this.socketStoreFactory != null) {
            this.socketStoreFactory.close();
        }
    }

    /**
     * Test the dynamic per call timeout. We do a regular put with the default
     * configured timeout. We then do a put with a dynamic timeout of 200 ms
     * which is less than the delay at the server side. After this we do a get
     * with a dynamic timeout of 1500 ms which should succeed and return the
     * value from the first put.
     */
    @Test
    public void test() {
        long incorrectTimeout = 200;
        long correctTimeout = 1500;
        String key = "a";
        String value = "First";
        String newValue = "Second";

        try {
            this.dynamicTimeoutClient.put(new ByteArray(key.getBytes()), value.getBytes());
        } catch(Exception e) {
            fail("Error in regular put.");
        }

        long startTime = System.currentTimeMillis();
        try {
            this.dynamicTimeoutClient.putWithCustomTimeout(new CompositePutVoldemortRequest<ByteArray, byte[]>(new ByteArray(key.getBytes()),
                                                                                                               newValue.getBytes(),
                                                                                                               incorrectTimeout));
            fail("Should not reach this point. The small (incorrect) timeout did not work.");
        } catch(InsufficientOperationalNodesException ion) {
            System.out.println("This failed as Expected.");
        }

        try {
            List<Versioned<byte[]>> versionedValues = this.dynamicTimeoutClient.getWithCustomTimeout(new CompositeGetVoldemortRequest<ByteArray, byte[]>(new ByteArray(key.getBytes()),
                                                                                                                                                         correctTimeout,
                                                                                                                                                         true));
            // We only expect one value in the response since resolve conflicts
            // is set to true
            assertTrue(versionedValues.size() == 1);

            Versioned<byte[]> versionedValue = versionedValues.get(0);

            long endTime = System.currentTimeMillis();
            System.out.println("Total time taken = " + (endTime - startTime));
            String response = new String(versionedValue.getValue());
            if(!response.equals(value)) {
                fail("The returned value does not match. Expected: " + value + " but Received: "
                     + response);
            }
        } catch(Exception e) {
            e.printStackTrace();
            fail("The dynamic per call timeout did not work !");
        }
    }
}
