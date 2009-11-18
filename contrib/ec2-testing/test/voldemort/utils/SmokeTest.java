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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import voldemort.utils.impl.RsyncDeployer;
import voldemort.utils.impl.SshClusterStarter;
import voldemort.utils.impl.SshClusterStopper;
import voldemort.utils.impl.SshRemoteTest;
import voldemort.utils.impl.TypicaEc2Connection;

public class SmokeTest {

    @Test
    public void test() throws Exception {
        List<HostNamePair> hostNamePairs = getInstances();

        if(hostNamePairs.size() < 3) {
            createInstances(6);
            hostNamePairs = getInstances();
        }

        final Map<String, Integer> nodeIds = generateClusterDescriptor(hostNamePairs,
                                                                       "/home/kirk/voldemortdev/voldemort/config/single_node_cluster/config/cluster.xml");

        final List<String> hostNames = new ArrayList<String>();
        String bootstrapHostName = hostNamePairs.get(0).getInternalHostName();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        final String hostUserId = "root";
        final File sshPrivateKey = new File("/home/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair");
        final String voldemortRootDirectory = "voldemort";
        final String voldemortHomeDirectory = "voldemort/config/single_node_cluster";
        final File sourceDirectory = new File("/home/kirk/voldemortdev/voldemort");
        String parentDirectory = ".";

        try {
            new SshClusterStopper(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory).execute();
        } catch(Exception e) {
            // Ignore...
        }

        new RsyncDeployer(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory).execute();

        new Thread(new Runnable() {

            public void run() {
                try {
                    new SshClusterStarter(hostNames,
                                          sshPrivateKey,
                                          hostUserId,
                                          voldemortRootDirectory,
                                          voldemortHomeDirectory,
                                          nodeIds).execute();
                } catch(RemoteOperationException e) {
                    e.printStackTrace();
                }
            }

        }).start();

        Thread.sleep(5000);

        Map<String, String> commands = new HashMap<String, String>();

        new SshRemoteTest(hostNames, sshPrivateKey, hostUserId, commands).execute();

        new SshClusterStopper(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory).execute();
    }

    private List<HostNamePair> createInstances(int count) throws Exception {
        String accessId = System.getProperty("ec2AccessId");
        String secretKey = System.getProperty("ec2SecretKey");
        String ami = System.getProperty("ec2Ami");
        String keyPairId = System.getProperty("ec2KeyPairId");
        Ec2Connection ec2 = new TypicaEc2Connection(accessId, secretKey);
        return ec2.create(ami, keyPairId, Ec2Connection.Ec2InstanceType.DEFAULT, count);
    }

    private List<HostNamePair> getInstances() throws Exception {
        String accessId = System.getProperty("ec2AccessId");
        String secretKey = System.getProperty("ec2SecretKey");
        Ec2Connection ec2 = new TypicaEc2Connection(accessId, secretKey);
        return ec2.list();
    }

    private Map<String, Integer> generateClusterDescriptor(List<HostNamePair> hostNamePairs,
                                                           String path) throws Exception {
        List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getInternalHostName());

        ClusterGenerator clusterGenerator = new ClusterGenerator();
        List<ClusterNodeDescriptor> nodes = clusterGenerator.createClusterNodeDescriptors(hostNames,
                                                                                          3);
        String clusterXml = clusterGenerator.createClusterDescriptor("test", nodes);
        FileUtils.writeStringToFile(new File(path), clusterXml);
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();

        for(ClusterNodeDescriptor node: nodes) {
            // OK, yeah, super-inefficient...
            for(HostNamePair hostNamePair: hostNamePairs) {
                if(node.getHostName().equals(hostNamePair.getInternalHostName()))
                    nodeIds.put(hostNamePair.getExternalHostName(), node.getId());
            }
        }

        return nodeIds;
    }

}
