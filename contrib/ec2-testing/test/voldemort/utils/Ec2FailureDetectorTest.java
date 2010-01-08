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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private static Ec2FailureDetectorTestConfig ec2FailureDetectorTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private FailureDetector failureDetector;
    private StoreClient<String, String> store;

    private static final Logger logger = Logger.getLogger(Ec2FailureDetectorTest.class);

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2FailureDetectorTestConfig = new Ec2FailureDetectorTestConfig();

        if(ec2FailureDetectorTestConfig.getInstanceCount() > 0) {
            hostNamePairs = createInstances(ec2FailureDetectorTestConfig);

            if(logger.isInfoEnabled())
                logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

            Thread.sleep(30000);
        } else {
            hostNamePairs = Arrays.asList(new HostNamePair("localhost", "localhost"));
        }

        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2FailureDetectorTestConfig);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(hostNames != null)
            destroyInstances(hostNames, ec2FailureDetectorTestConfig);
    }

    @Before
    public void setUp() throws Exception {
        deploy(hostNames, ec2FailureDetectorTestConfig);
        startClusterAsync(hostNames, ec2FailureDetectorTestConfig, nodeIds);

        String url = "tcp://" + getRandomHostName() + ":6666";
        StoreClientFactory scf = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(url));

        failureDetector = scf.getFailureDetector();
        store = scf.getStoreClient("test");
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2FailureDetectorTestConfig);
    }

    @Test
    public void testSingleNodeOffline() throws Exception {
        // 1. Get the node that we're going to take down...
        String offlineHostName = getRandomHostName();
        Node offlineNode = getNodeByHostName(offlineHostName, failureDetector);

        // 2. Hit the stores enough that we can reasonably assume that they're
        // up and reachable.
        test(store);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());
        assertTrue(failureDetector.isAvailable(offlineNode));

        // 3. Stop our node, then test enough that we can cause the node to be
        // marked as unavailable...
        stopClusterNode(offlineHostName, ec2FailureDetectorTestConfig);
        test(store);
        assertEquals(hostNamePairs.size() - 1, failureDetector.getAvailableNodeCount());
        assertFalse(failureDetector.isAvailable(offlineNode));

        // 4. Now start the node up, test, and make sure everything's OK.
        startClusterNode(offlineHostName, ec2FailureDetectorTestConfig, offlineNode.getId());
        failureDetector.waitForAvailability(offlineNode);
        test(store);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());
        assertTrue(failureDetector.isAvailable(offlineNode));
    }

    @Test
    public void testAllNodesOffline() throws Exception {
        // 1. Hit the stores enough that we can reasonably assume that they're
        // up and reachable.
        test(store);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());

        for(Node n: failureDetector.getConfig().getNodes())
            assertTrue(failureDetector.isAvailable(n));

        // 2. Stop all the nodes, then test enough that we can cause the nodes
        // to be marked as unavailable...
        stopClusterQuiet(hostNames, ec2FailureDetectorTestConfig);
        test(store);
        assertEquals(0, failureDetector.getAvailableNodeCount());

        for(Node n: failureDetector.getConfig().getNodes())
            assertFalse(failureDetector.isAvailable(n));

        // 3. Now start the cluster up, test, and make sure everything's OK.
        startClusterAsync(hostNames, ec2FailureDetectorTestConfig, nodeIds);

        for(Node n: failureDetector.getConfig().getNodes())
            failureDetector.waitForAvailability(n);

        test(store);
        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());

        for(Node n: failureDetector.getConfig().getNodes())
            assertTrue(failureDetector.isAvailable(n));
    }

    @Test
    public void testStress() throws Exception {
        final AtomicBoolean isRunning = new AtomicBoolean(true);
        ExecutorService threadPool = Executors.newFixedThreadPool(ec2FailureDetectorTestConfig.testThreads + 1);

        // 1. Create a bunch of threads that just hit the stores repeatedly.
        for(int i = 0; i < ec2FailureDetectorTestConfig.testThreads; i++) {
            threadPool.submit(new Runnable() {

                public void run() {
                    while(isRunning.get())
                        test(store, 100);
                }

            });
        }

        // 2. Create a thread that randomly brings a single node offline and
        // then up again, waiting some period of time in between.
        threadPool.submit(new Runnable() {

            public void run() {
                try {
                    Random random = new Random();

                    while(isRunning.get()) {
                        String offlineHostName = getRandomHostName();
                        Node offlineNode = getNodeByHostName(offlineHostName, failureDetector);

                        stopClusterNode(offlineHostName, ec2FailureDetectorTestConfig);
                        Thread.sleep(random.nextInt(10000));
                        startClusterNode(offlineHostName,
                                         ec2FailureDetectorTestConfig,
                                         offlineNode.getId());
                        Thread.sleep(random.nextInt(10000));
                    }
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(e);
                }
            }

        });

        // 3. Let it run for some "long" period of time...
        Thread.sleep(ec2FailureDetectorTestConfig.testTime * 60 * 1000);

        // 4. Now shut it down and wait.
        if(logger.isInfoEnabled())
            logger.info("Shutting down");

        isRunning.set(false);
        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        assertEquals(hostNamePairs.size(), failureDetector.getAvailableNodeCount());
    }

    private void test(StoreClient<String, String> store) {
        test(store, 1000);
    }

    private void test(StoreClient<String, String> store, int tests) {
        for(int i = 0; i < tests; i++) {
            try {
                store.get("test_" + i);
            } catch(Exception e) {
                if(logger.isDebugEnabled())
                    logger.debug(e);
            }
        }
    }

    private Node getNodeByHostName(String hostName, FailureDetector failureDetector)
            throws Exception {
        Integer offlineNodeId = nodeIds.get(hostName);

        for(Node n: failureDetector.getConfig().getNodes()) {
            if(offlineNodeId.equals(n.getId()))
                return n;
        }

        throw new Exception();
    }

    private String getRandomHostName() {
        Random random = new Random();
        return hostNames.get(random.nextInt(hostNames.size()));
    }

    /**
     * Ec2FailureDetectorTestConfig contains configuration for
     * {@link Ec2FailureDetectorTest}.
     * 
     * There are quite a few properties that are needed which are provided in a
     * *.properties file, the path of which is provided in the
     * "ec2PropertiesFile" System property. Below is a table of the properties
     * <i>in addition to those from {@link Ec2RemoteTestConfig}</i>:
     * 
     * <table>
     * <th>Name</th>
     * <th>Description</th>
     * <tr>
     * <td>ec2TestThreads</td>
     * <td>For the test, the number of threads to concurrently process "get"
     * requests.</td>
     * </tr>
     * <tr>
     * <td>ec2TestTime</td>
     * <td>The amount of time (in minutes) to run the test.</td>
     * </tr>
     * </table>
     * 
     * @author Kirk True
     */

    private static class Ec2FailureDetectorTestConfig extends Ec2RemoteTestConfig {

        private int testThreads;

        private int testTime;

        @Override
        protected void init(Properties properties) {
            super.init(properties);

            testThreads = getIntProperty(properties, "ec2TestThreads");
            testTime = getIntProperty(properties, "ec2TestTime");
        }

        @Override
        protected List<String> getRequiredPropertyNames() {
            List<String> requireds = super.getRequiredPropertyNames();
            requireds.addAll(Arrays.asList("ec2TestThreads", "ec2TestTime"));
            return requireds;
        }

    }

}