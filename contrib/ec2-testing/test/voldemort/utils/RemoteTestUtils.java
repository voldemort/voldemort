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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.utils.impl.*;

public class RemoteTestUtils {

    private static final Logger logger = Logger.getLogger(RemoteTestUtils.class);

    public static void deploy(List<String> hostNames, RemoteTestConfig remoteTestConfig)
            throws Exception {
        new RsyncDeployer(hostNames,
                          remoteTestConfig.getSshPrivateKey(),
                          remoteTestConfig.getHostUserId(),
                          remoteTestConfig.getSourceDirectory(),
                          remoteTestConfig.getParentDirectory()).execute();
    }

    public static void executeRemoteTest(List<String> hostNames,
                                         RemoteTestConfig remoteTestConfig,
                                         Map<String, String> commands) throws Exception {
        new SshRemoteTest(hostNames,
                          remoteTestConfig.getSshPrivateKey(),
                          remoteTestConfig.getHostUserId(),
                          commands).execute();
    }

    public static void startClusterAsync(List<String> hostNames,
                                         RemoteTestConfig remoteTestConfig,
                                         Map<String, Integer> nodeIds) throws Exception {
        startCluster(hostNames, remoteTestConfig, nodeIds, true, 10);
    }

    public static void startCluster(final List<String> hostNames,
                                    final RemoteTestConfig remoteTestConfig,
                                    final Map<String, Integer> nodeIds,
                                    boolean startAsynchronously,
                                    int waitSeconds) throws Exception {
        if(startAsynchronously) {
            new Thread(new Runnable() {

                public void run() {
                    try {
                        new SshClusterStarter(hostNames,
                                              remoteTestConfig.getSshPrivateKey(),
                                              remoteTestConfig.getHostUserId(),
                                              remoteTestConfig.getVoldemortRootDirectory(),
                                              remoteTestConfig.getVoldemortHomeDirectory(),
                                              nodeIds).execute();
                    } catch(RemoteOperationException e) {
                        e.printStackTrace();
                    }
                }

            }).start();

            Thread.sleep(waitSeconds * 1000);
        } else {
            new SshClusterStarter(hostNames,
                                  remoteTestConfig.getSshPrivateKey(),
                                  remoteTestConfig.getHostUserId(),
                                  remoteTestConfig.getVoldemortRootDirectory(),
                                  remoteTestConfig.getVoldemortHomeDirectory(),
                                  nodeIds).execute();
        }
    }

    public static void startClusterNode(String hostName,
                                        RemoteTestConfig remoteTestConfig,
                                        int nodeId) throws Exception {
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();
        nodeIds.put(hostName, nodeId);

        startClusterAsync(new ArrayList<String>(Arrays.asList(hostName)), remoteTestConfig, nodeIds);
    }

    public static void cleanupCluster(List<String> hostNames, RemoteTestConfig remoteTestConfig)
            throws Exception {
        new SshClusterCleaner(hostNames,
                              remoteTestConfig.getSshPrivateKey(),
                              remoteTestConfig.getHostUserId(),
                              remoteTestConfig.getVoldemortHomeDirectory()).execute();
    }
    
    public static void stopCluster(List<String> hostNames, RemoteTestConfig remoteTestConfig)
            throws Exception {
        new SshClusterStopper(hostNames,
                              remoteTestConfig.getSshPrivateKey(),
                              remoteTestConfig.getHostUserId(),
                              remoteTestConfig.getVoldemortRootDirectory(),
                              false).execute();
    }

    public static void stopClusterQuiet(List<String> hostNames, RemoteTestConfig remoteTestConfig)
            throws Exception {
        try {
            new SshClusterStopper(hostNames,
                                  remoteTestConfig.getSshPrivateKey(),
                                  remoteTestConfig.getHostUserId(),
                                  remoteTestConfig.getVoldemortRootDirectory(),
                                  true).execute();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }
    }

    public static void stopClusterNode(String hostName, RemoteTestConfig remoteTestConfig)
            throws Exception {
        stopCluster(Arrays.asList(hostName), remoteTestConfig);
    }

    public static List<String> toHostNames(List<HostNamePair> hostNamePairs) {
        final List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getExternalHostName());

        return hostNames;
    }

    public static Map<String, Integer> generateClusterDescriptor(List<HostNamePair> hostNamePairs,
                                                                 String clusterName,
                                                                 RemoteTestConfig remoteTestConfig)
            throws Exception {
        return generateClusterDescriptor(hostNamePairs, clusterName, remoteTestConfig, false);
    }

    public static Map<String, Integer> generateClusterDescriptor(List<HostNamePair> hostNamePairs,
                                                                  String clusterName,
                                                                  RemoteTestConfig remoteTestConfig,
                                                                  boolean useExternal)
             throws Exception {
        // This isn't too elegant, but it works
        List<String> hostNames = Lists.transform(hostNamePairs,
                                                 useExternal ?
                                                 new Function<HostNamePair, String> () {
                                                     public String apply(HostNamePair hostNamePair) {
                                                         return hostNamePair.getExternalHostName();
                                                     }
                                                 } :
                                                 new Function<HostNamePair, String> () {
                                                     public String apply(HostNamePair hostNamePair) {
                                                         return hostNamePair.getInternalHostName();
                                                     }
                                                 }
        );

        ClusterGenerator clusterGenerator = new ClusterGenerator();
        List<ClusterNodeDescriptor> nodes = clusterGenerator.createClusterNodeDescriptors(hostNames,
                                                                                          3);
        String clusterXml = clusterGenerator.createClusterDescriptor(clusterName, nodes);
        FileUtils.writeStringToFile(remoteTestConfig.getClusterXmlFile(), clusterXml);
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();

        for(ClusterNodeDescriptor node: nodes) {
            if (useExternal)
                nodeIds.put(node.getHostName(), node.getId());
            else {
                // OK, yeah, this is super-inefficient...
                for(HostNamePair hostNamePair: hostNamePairs)
                    if(node.getHostName().equals(hostNamePair.getInternalHostName()))
                        nodeIds.put(hostNamePair.getExternalHostName(), node.getId());
            }
        }

        return nodeIds;
    }

    // TODO: move this out to a separate place

    public static Map<String, Integer> generateClusterDescriptor(List<HostNamePair> hostNamePairs,
                                                                 Cluster cluster,
                                                                 RemoteTestConfig remoteTestConfig)
            throws Exception {
        List<String> hostNames = Lists.transform(hostNamePairs,
                                                 new Function<HostNamePair, String> () {
                                                     public String apply(HostNamePair hostNamePair) {
                                                         return hostNamePair.getExternalHostName();
                                                     }
                                                 });
        ClusterGenerator clusterGenerator = new ClusterGenerator();
        List<ClusterNodeDescriptor> nodes = clusterGenerator.createClusterNodeDescriptors(hostNames,
                                                                                          cluster);
        String clusterXml = clusterGenerator.createClusterDescriptor(cluster.getName(), nodes);
        FileUtils.writeStringToFile(remoteTestConfig.getClusterXmlFile(), clusterXml);
        Map<String,Integer> nodeIds = new HashMap<String,Integer>();

        for (ClusterNodeDescriptor node: nodes) {
            nodeIds.put(node.getHostName(), node.getId());
        }

        return nodeIds;
    }

}
