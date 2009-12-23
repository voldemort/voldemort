/*
 * Copyright 2009 LinkedIn, Inc.
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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;

/**
 * Ec2FailureDetectorTest creates nodes using EC2 and .
 * 
 * There are quite a few properties that are needed which are provided for these
 * tests to run. Please see {@link Ec2RemoteTestConfig} for details.
 * 
 * @author Kirk True
 */

public class Ec2FailureDetectorTest {

    private static Ec2RemoteTestConfig ec2RemoteTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private static final Logger logger = Logger.getLogger(Ec2FailureDetectorTest.class);

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2RemoteTestConfig = new Ec2RemoteTestConfig();

        if(ec2RemoteTestConfig.getInstanceCount() > 0) {
            hostNamePairs = createInstances(ec2RemoteTestConfig);

            if(logger.isInfoEnabled())
                logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

            Thread.sleep(30000);
        } else {
            hostNamePairs = Arrays.asList(new HostNamePair("localhost", "localhost"));
        }

        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2RemoteTestConfig);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(hostNames != null)
            destroyInstances(hostNames, ec2RemoteTestConfig);
    }

    @Before
    public void setUp() throws Exception {
        deploy(hostNames, ec2RemoteTestConfig);
        startClusterAsync(hostNames, ec2RemoteTestConfig, nodeIds);
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2RemoteTestConfig);
    }

    @Test
    public void testSingleNodeOffline() throws Exception {
        String offlineHostName = hostNames.get(0);
        Integer offlineNodeId = nodeIds.get(offlineHostName);

        StoreClientFactory scf = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://"
                                                                                                  + hostNamePairs.get(0)
                                                                                                                 .getInternalHostName()
                                                                                                  + ":6666"));
        FailureDetector failureDetector = scf.getFailureDetector();
        Node node = null;

        for(Node n: failureDetector.getConfig().getNodes()) {
            if(offlineNodeId.equals(n.getId())) {
                node = n;
                break;
            }
        }

        StoreClient<String, String> store1 = scf.getStoreClient("test");

        test(store1, true);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());
        assertTrue(failureDetector.isAvailable(node));

        stopClusterNode(offlineHostName, ec2RemoteTestConfig);

        test(store1, false);
        assertEquals(hostNamePairs.size() - 1, failureDetector.getAvailableNodeCount());
        assertFalse(failureDetector.isAvailable(node));

        startClusterNode(offlineHostName, ec2RemoteTestConfig, offlineNodeId);
        failureDetector.waitForAvailability(node);

        test(store1, true);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());
        assertTrue(failureDetector.isAvailable(node));
    }

    @Test
    public void testAllNodesOffline() throws Exception {
        StoreClientFactory scf = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://"
                                                                                                  + hostNamePairs.get(0)
                                                                                                                 .getInternalHostName()
                                                                                                  + ":6666"));
        FailureDetector failureDetector = scf.getFailureDetector();

        StoreClient<String, String> store1 = scf.getStoreClient("test");

        test(store1, true);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());

        for(Node n: failureDetector.getConfig().getNodes())
            assertTrue(failureDetector.isAvailable(n));

        stopClusterQuiet(hostNames, ec2RemoteTestConfig);

        test(store1, false);
        assertEquals(0, failureDetector.getAvailableNodeCount());

        for(Node n: failureDetector.getConfig().getNodes())
            assertFalse(failureDetector.isAvailable(n));

        startClusterAsync(hostNames, ec2RemoteTestConfig, nodeIds);

        for(Node n: failureDetector.getConfig().getNodes())
            failureDetector.waitForAvailability(n);

        test(store1, true);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());

        for(Node n: failureDetector.getConfig().getNodes())
            assertTrue(failureDetector.isAvailable(n));
    }

    private void test(StoreClient<String, String> store, boolean printErrors) {
        for(int i = 0; i < 1000; i++) {
            try {
                store.get("test_" + i);
            } catch(Exception e) {
                if(printErrors)
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(e);
            }
        }
    }

}
