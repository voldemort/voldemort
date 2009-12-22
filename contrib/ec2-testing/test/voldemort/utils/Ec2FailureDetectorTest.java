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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.StoreVerifier;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;

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
    public void testTemporaryNodeOffline() throws Exception {
        HostNamePair offlineHost = hostNamePairs.get(0);
        Integer nodeId = nodeIds.get(offlineHost.getExternalHostName());
        Node node = new Node(nodeId,
                             offlineHost.getInternalHostName(),
                             8081,
                             6666,
                             6667,
                             Collections.<Integer> emptyList());

        StoreClientFactory scf = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://"
                                                                                                  + offlineHost.getInternalHostName()
                                                                                                  + ":6666")
                                                                                .setFailureDetectorAsyncRecoveryInterval(1000));
        FailureDetector failureDetector = scf.getFailureDetector();
        StoreVerifier storeVerifier = failureDetector.getConfig().getStoreVerifier();
        storeVerifier.verifyStore(node);

        Store<String, String> store = scf.getRawStore("test", null);

        test(store);
        Assert.assertEquals(1, failureDetector.getAvailableNodeCount());

        stopClusterNode(offlineHost.getExternalHostName(), ec2RemoteTestConfig);
        test(store);
        Assert.assertEquals(0, failureDetector.getAvailableNodeCount());

        startClusterNode(offlineHost.getExternalHostName(), ec2RemoteTestConfig, nodeId);

        failureDetector.waitForAvailability(node);

        test(store);
        Assert.assertEquals(1, failureDetector.getAvailableNodeCount());
    }

    private void test(Store<String, String> store) {
        for(int i = 0; i < 100; i++) {
            try {
                store.get(MetadataStore.NODE_ID_KEY);
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e);
            }
        }
    }

}
