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

import org.junit.Before;
import org.junit.Test;

public class SmokeTest {

    private String accessId;
    private String secretKey;
    private String ami;
    private String keyPairId;
    private String sshPrivateKeyPath;
    private String hostUserId;
    private File sshPrivateKey;
    private String voldemortRootDirectory;
    private String voldemortHomeDirectory;
    private File sourceDirectory;
    private String parentDirectory;
    private File clusterXmlFile;
    private List<HostNamePair> hostNamePairs;
    private List<String> hostNames;
    private Map<String, Integer> nodeIds;

    @Before
    public void setUp() throws Exception {
        accessId = System.getProperty("ec2AccessId");
        secretKey = System.getProperty("ec2SecretKey");
        ami = System.getProperty("ec2Ami");
        keyPairId = System.getProperty("ec2KeyPairId");
        sshPrivateKeyPath = System.getProperty("sshPrivateKeyPath");
        hostUserId = "root";
        sshPrivateKey = sshPrivateKeyPath != null ? new File(sshPrivateKeyPath) : null;
        voldemortRootDirectory = "voldemort";
        voldemortHomeDirectory = "voldemort/config/single_node_cluster";
        sourceDirectory = new File(System.getProperty("user.dir"));
        parentDirectory = ".";
        clusterXmlFile = new File(System.getProperty("user.dir"),
                                  "config/single_node_cluster/config/cluster.xml");

        hostNamePairs = listInstances(accessId, secretKey);
        int existing = hostNamePairs.size();
        int max = 6;
        int min = 3;

        if(existing < min) {
            createInstances(accessId, secretKey, ami, keyPairId, max - existing);
            hostNamePairs = listInstances(accessId, secretKey);
        }

        hostNames = toHostNames(hostNamePairs);

        nodeIds = generateClusterDescriptor(hostNamePairs, "test", clusterXmlFile);

        stopClusterQuiet(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        deploy(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory);
    }

    @Test
    public void test() throws Exception {
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
    public void test2() throws Exception {
        try {
            startClusterAsync(hostNames,
                              sshPrivateKey,
                              hostUserId,
                              voldemortRootDirectory,
                              voldemortHomeDirectory,
                              nodeIds);

            String badHostName = hostNames.get(0);

            stopClusterNode(badHostName, sshPrivateKey, hostUserId, voldemortRootDirectory);

            startClusterNode(badHostName,
                             sshPrivateKey,
                             hostUserId,
                             voldemortRootDirectory,
                             voldemortHomeDirectory,
                             nodeIds.get(hostNames.get(0)));
        } finally {
            stopCluster(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        }
    }
}
