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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.utils.impl.RsyncDeployer;
import voldemort.utils.impl.SshClusterStarter;
import voldemort.utils.impl.SshClusterStopper;
import voldemort.utils.impl.SshRemoteTest;

public class RemoteTestUtils {

    private static final Logger logger = Logger.getLogger(RemoteTestUtils.class);

    public static void deploy(List<String> hostNames,
                              File sshPrivateKey,
                              String hostUserId,
                              File sourceDirectory,
                              String parentDirectory) throws Exception {
        new RsyncDeployer(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory).execute();
    }

    public static void executeRemoteTest(List<String> hostNames,
                                         File sshPrivateKey,
                                         String hostUserId,
                                         Map<String, String> commands) throws Exception {
        new SshRemoteTest(hostNames, sshPrivateKey, hostUserId, commands).execute();
    }

    public static void executeRemoteTest(List<HostNamePair> hostNamePairs,
                                         String voldemortRootDirectory,
                                         File sshPrivateKey,
                                         String hostUserId,
                                         int rampTime,
                                         int iterations,
                                         int numRequests) throws Exception {
        Map<String, String> commands = new HashMap<String, String>();
        List<String> hostNames = new ArrayList<String>();
        String bootstrapHostName = hostNamePairs.get(0).getInternalHostName();

        int i = 0;

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        for(HostNamePair hostNamePair: hostNamePairs) {
            String command = "cd " + voldemortRootDirectory + " ; sleep " + (i * rampTime)
                             + "; ./bin/voldemort-remote-test.sh -w -d --iterations " + iterations
                             + " --start-key-index " + (i * numRequests) + " tcp://"
                             + bootstrapHostName + ":6666 test " + numRequests;
            commands.put(hostNamePair.getExternalHostName(), command);
            i++;
        }

        executeRemoteTest(hostNames, sshPrivateKey, hostUserId, commands);
    }

    public static void startClusterAsync(final List<String> hostNames,
                                         final File sshPrivateKey,
                                         final String hostUserId,
                                         final String voldemortRootDirectory,
                                         final String voldemortHomeDirectory,
                                         final Map<String, Integer> nodeIds) throws Exception {
        startCluster(hostNames,
                     sshPrivateKey,
                     hostUserId,
                     voldemortRootDirectory,
                     voldemortHomeDirectory,
                     nodeIds,
                     true,
                     10);
    }

    public static void startCluster(final List<String> hostNames,
                                    final File sshPrivateKey,
                                    final String hostUserId,
                                    final String voldemortRootDirectory,
                                    final String voldemortHomeDirectory,
                                    final Map<String, Integer> nodeIds,
                                    boolean startAsynchronously,
                                    int waitSeconds) throws Exception {
        if(startAsynchronously) {
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

            Thread.sleep(waitSeconds * 1000);
        } else {
            new SshClusterStarter(hostNames,
                                  sshPrivateKey,
                                  hostUserId,
                                  voldemortRootDirectory,
                                  voldemortHomeDirectory,
                                  nodeIds).execute();
        }
    }

    public static void startClusterNode(String hostName,
                                        File sshPrivateKey,
                                        String hostUserId,
                                        String voldemortRootDirectory,
                                        String voldemortHomeDirectory,
                                        int nodeId) throws Exception {
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();
        nodeIds.put(hostName, nodeId);

        startClusterAsync(new ArrayList<String>(Arrays.asList(hostName)),
                          sshPrivateKey,
                          hostUserId,
                          voldemortRootDirectory,
                          voldemortHomeDirectory,
                          nodeIds);
    }

    public static void stopCluster(List<String> hostNames,
                                   File sshPrivateKey,
                                   String hostUserId,
                                   String voldemortRootDirectory) throws Exception {
        new SshClusterStopper(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory, false).execute();
    }

    public static void stopClusterQuiet(List<String> hostNames,
                                        File sshPrivateKey,
                                        String hostUserId,
                                        String voldemortRootDirectory) throws Exception {
        try {
            new SshClusterStopper(hostNames,
                                  sshPrivateKey,
                                  hostUserId,
                                  voldemortRootDirectory,
                                  true).execute();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }
    }

    public static void stopClusterNode(String hostName,
                                       File sshPrivateKey,
                                       String hostUserId,
                                       String voldemortRootDirectory) throws Exception {
        stopCluster(new ArrayList<String>(Arrays.asList(hostName)),
                    sshPrivateKey,
                    hostUserId,
                    voldemortRootDirectory);
    }

    public static List<String> toHostNames(List<HostNamePair> hostNamePairs) {
        final List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        return hostNames;
    }

    public static Map<String, Integer> generateClusterDescriptor(List<HostNamePair> hostNamePairs,
                                                                 String clusterName,
                                                                 File clusterXmlFile)
            throws Exception {
        List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getInternalHostName());

        ClusterGenerator clusterGenerator = new ClusterGenerator();
        List<ClusterNodeDescriptor> nodes = clusterGenerator.createClusterNodeDescriptors(hostNames,
                                                                                          3);
        String clusterXml = clusterGenerator.createClusterDescriptor(clusterName, nodes);
        FileUtils.writeStringToFile(clusterXmlFile, clusterXml);
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();

        for(ClusterNodeDescriptor node: nodes) {
            // OK, yeah, this is super-inefficient...
            for(HostNamePair hostNamePair: hostNamePairs) {
                if(node.getHostName().equals(hostNamePair.getInternalHostName()))
                    nodeIds.put(hostNamePair.getExternalHostName(), node.getId());
            }
        }

        return nodeIds;
    }

}
