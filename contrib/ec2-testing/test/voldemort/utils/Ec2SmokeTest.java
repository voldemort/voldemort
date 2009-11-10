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
import static voldemort.utils.Ec2InstanceRemoteTestUtils.listInstances;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.executeRemoteTest;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.stopClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Ec2SmokeTest {

    private static String accessId;
    private static String secretKey;
    private static String ami;
    private static String keyPairId;
    private static String sshPrivateKeyPath;
    private static String hostUserId;
    private static File sshPrivateKey;
    private static String voldemortRootDirectory;
    private static String voldemortHomeDirectory;
    private static File sourceDirectory;
    private static String parentDirectory;
    private static File clusterXmlFile;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    @BeforeClass
    public static void setUpClass() throws Exception {
        accessId = getSystemProperty("ec2AccessId");
        secretKey = getSystemProperty("ec2SecretKey");
        ami = getSystemProperty("ec2Ami");
        keyPairId = getSystemProperty("ec2KeyPairId");
        sshPrivateKeyPath = getSystemProperty("ec2SshPrivateKeyPath");
        hostUserId = "root";
        sshPrivateKey = sshPrivateKeyPath != null ? new File(sshPrivateKeyPath) : null;
        voldemortRootDirectory = "voldemort";
        voldemortHomeDirectory = "voldemort/config/single_node_cluster";
        sourceDirectory = new File(getSystemProperty("user.dir"));
        parentDirectory = ".";
        clusterXmlFile = new File(getSystemProperty("user.dir"),
                                  "config/single_node_cluster/config/cluster.xml");

        hostNamePairs = listInstances(accessId, secretKey);
        int existing = hostNamePairs.size();
        int max = 4;
        int min = 2;

        if(existing < min) {
            createInstances(accessId, secretKey, ami, keyPairId, max - existing);
            hostNamePairs = listInstances(accessId, secretKey);
        }

        hostNames = toHostNames(hostNamePairs);

        nodeIds = generateClusterDescriptor(hostNamePairs, "test", clusterXmlFile);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        destroyInstances(accessId, secretKey, hostNames);
    }

    @Before
    public void setUp() throws Exception {
        stopClusterQuiet(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
    }

    @Test
    public void testRemoteTest() throws Exception {
        deploy(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory);

        try {
            startClusterAsync(hostNames,
                              sshPrivateKey,
                              hostUserId,
                              voldemortRootDirectory,
                              voldemortHomeDirectory,
                              nodeIds);

            executeRemoteTest(hostNamePairs, sshPrivateKey, hostUserId, 10, 10, 100);
        } finally {
            stopCluster(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        }
    }

    @Test
    public void testTemporaryNodeOffline() throws Exception {
        deploy(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory);

        try {
            startClusterAsync(hostNames,
                              sshPrivateKey,
                              hostUserId,
                              voldemortRootDirectory,
                              voldemortHomeDirectory,
                              nodeIds);

            String offlineHostName = hostNames.get(0);

            stopClusterNode(offlineHostName, sshPrivateKey, hostUserId, voldemortRootDirectory);

            startClusterNode(offlineHostName,
                             sshPrivateKey,
                             hostUserId,
                             voldemortRootDirectory,
                             voldemortHomeDirectory,
                             nodeIds.get(hostNames.get(0)));
        } finally {
            stopCluster(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        }
    }

    private static String getSystemProperty(String key) {
        String value = System.getProperty(key);

        if(value == null || value.trim().length() == 0)
            throw new RuntimeException("Missing value for system property " + key);

        return value;
    }

}
