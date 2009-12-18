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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.utils.impl.TypicaEc2Connection;

public class Ec2RemoteTestUtils {

    public static List<HostNamePair> createInstances(Ec2RemoteTestConfig ec2Config)
            throws Exception {
        return createInstances(ec2Config.getInstanceCount(), ec2Config);
    }

    public static List<HostNamePair> createInstances(int instanceCount, Ec2RemoteTestConfig ec2Config) throws Exception{
        Ec2Connection ec2 = new TypicaEc2Connection(ec2Config.getAccessId(),
                                                    ec2Config.getSecretKey(),
                                                    new Ec2Listener(ec2Config.getInstanceIdFile()));
        return ec2.createInstances(ec2Config.getAmi(),
                                   ec2Config.getKeyPairId(),
                                   Ec2Connection.Ec2InstanceType.DEFAULT,
                                   instanceCount);

    }

    public static void destroyInstances(List<String> hostNames, Ec2RemoteTestConfig ec2Config)
            throws Exception {
        Ec2Connection ec2 = new TypicaEc2Connection(ec2Config.getAccessId(),
                                                    ec2Config.getSecretKey(),
                                                    new Ec2Listener(ec2Config.getInstanceIdFile()));
        ec2.deleteInstancesByHostName(hostNames);
    }

    public static List<HostNamePair> listInstances(Ec2RemoteTestConfig ec2Config) throws Exception {
        Ec2Connection ec2 = new TypicaEc2Connection(ec2Config.getAccessId(),
                                                    ec2Config.getSecretKey());
        return ec2.list();
    }

    public static Map<String, Integer> generateClusterDescriptor(List<HostNamePair> hostNamePairs,
                                                                 String clusterName,
                                                                 String path) throws Exception {
        List<String> hostNames = new ArrayList<String>();

        for(HostNamePair hostNamePair: hostNamePairs)
            hostNames.add(hostNamePair.getInternalHostName());

        ClusterGenerator clusterGenerator = new ClusterGenerator();
        List<ClusterNodeDescriptor> nodes = clusterGenerator.createClusterNodeDescriptors(hostNames,
                                                                                          3);
        String clusterXml = clusterGenerator.createClusterDescriptor(clusterName, nodes);
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

    private static class Ec2Listener implements Ec2ConnectionListener {

        private final File file;

        private final Logger logger = Logger.getLogger(getClass());

        public Ec2Listener(File file) {
            this.file = file;
        }

        @SuppressWarnings("unchecked")
        public synchronized void instanceCreated(String instanceId) {
            try {
                List<String> instanceIds = file.exists() ? FileUtils.readLines(file)
                                                        : new ArrayList<String>();
                instanceIds.add(instanceId);
                FileUtils.writeLines(file, instanceIds);

                if(logger.isInfoEnabled())
                    logger.info("Instances created: " + FileUtils.readLines(file));
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e, e);
            }
        }

        @SuppressWarnings("unchecked")
        public synchronized void instanceDestroyed(String instanceId) {
            try {
                List<String> instanceIds = file.exists() ? FileUtils.readLines(file)
                                                        : new ArrayList<String>();
                Iterator<String> i = instanceIds.iterator();

                while(i.hasNext()) {
                    if(instanceId.equals(i.next().trim()))
                        i.remove();
                }

                FileUtils.writeLines(file, instanceIds);

                if(logger.isInfoEnabled())
                    logger.info("Instances remaining: " + FileUtils.readLines(file));
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e, e);
            }
        }

    }

}
