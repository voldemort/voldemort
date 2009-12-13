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

import static voldemort.utils.Ec2InstanceRemoteTestUtils.createInstances;
import static voldemort.utils.Ec2InstanceRemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.executeRemoteTest;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Ec2SmokeTest contains two examples that interact with EC2.
 * 
 * There are quite a few properties that are needed which are provided for these
 * tests to run. Please see {@link Ec2SmokeTestConfig} for details.
 * 
 * @author Kirk True
 */

public class Ec2SmokeTest {

    private static Ec2SmokeTestConfig ec2SmokeTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private static final Logger logger = Logger.getLogger(Ec2SmokeTest.class);

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2SmokeTestConfig = new Ec2SmokeTestConfig();
        hostNamePairs = createInstances(ec2SmokeTestConfig);
        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2SmokeTestConfig);

        if(logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(hostNames != null)
            destroyInstances(hostNames, ec2SmokeTestConfig);
    }

    @Before
    public void setUp() throws Exception {
        deploy(hostNames, ec2SmokeTestConfig);
        startClusterAsync(hostNames, ec2SmokeTestConfig, nodeIds);
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2SmokeTestConfig);
    }

    @Test
    public void testRemoteTest() throws Exception {
        Map<String, String> commands = new HashMap<String, String>();
        List<String> hostNames = new ArrayList<String>();
        String bootstrapHostName = hostNamePairs.get(0).getInternalHostName();

        int i = 0;

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        for(HostNamePair hostNamePair: hostNamePairs) {
            String command = "cd " + ec2SmokeTestConfig.getVoldemortRootDirectory() + " ; sleep "
                             + (i * ec2SmokeTestConfig.rampTime)
                             + "; ./bin/voldemort-remote-test.sh -w -d --iterations "
                             + ec2SmokeTestConfig.iterations + " --start-key-index "
                             + (i * ec2SmokeTestConfig.numRequests) + " tcp://" + bootstrapHostName
                             + ":6666 test " + ec2SmokeTestConfig.numRequests;
            commands.put(hostNamePair.getExternalHostName(), command);
            i++;
        }

        executeRemoteTest(hostNames, ec2SmokeTestConfig, commands);
    }

    @Test
    public void testTemporaryNodeOffline() throws Exception {
        String offlineHostName = hostNames.get(0);
        Integer nodeId = nodeIds.get(offlineHostName);

        stopClusterNode(offlineHostName, ec2SmokeTestConfig);
        startClusterNode(offlineHostName, ec2SmokeTestConfig, nodeId);
    }

    /**
     * Ec2SmokeTestConfig contains configuration for {@link Ec2SmokeTest}.
     * 
     * There are quite a few properties that are needed which are provided in a
     * *.properties file, the path of which is provided in the
     * "ec2PropertiesFile" System property. Below is a table of the properties
     * <i>in addition to those from {@link Ec2Config}</i>:
     * 
     * <table>
     * <th>Name</th>
     * <th>Description</th>
     * <tr>
     * <td>ec2RampTime</td>
     * <td>For the remote test, the number of seconds to wait for each instance
     * before connecting to the server. Prevents the server nodes from being
     * flooded all at once.</td>
     * </tr>
     * <tr>
     * <td>ec2Iterations</td>
     * <td>For the remote test, the number of remote test iterations.</td>
     * </tr>
     * <tr>
     * <td>ec2NumRequests</td>
     * <td>For the remote test, the number of remote test requests per each
     * iteration.</td>
     * </tr>
     * </table>
     * 
     * @author Kirk True
     */

    private static class Ec2SmokeTestConfig extends Ec2Config {

        private int rampTime;

        private int iterations;

        private int numRequests;

        @Override
        protected void init(Properties properties) {
            super.init(properties);

            rampTime = getIntProperty(properties, "ec2RampTime");
            iterations = getIntProperty(properties, "ec2Iterations");
            numRequests = getIntProperty(properties, "ec2NumRequests");
        }

        @Override
        protected List<String> getRequiredPropertyNames() {
            List<String> requireds = super.getRequiredPropertyNames();
            requireds.addAll(Arrays.asList("ec2RampTime", "ec2Iterations", "ec2NumRequests"));
            return requireds;
        }

    }

}
