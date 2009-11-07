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

import voldemort.utils.impl.TypicaEc2Connection;

public class Ec2InstanceRemoteTestUtils {

    public static List<HostNamePair> createInstances(String ec2AccessId,
                                                     String ec2SecretKey,
                                                     String ec2Ami,
                                                     String ec2KeyPairId,
                                                     int count) throws Exception {
        Ec2Connection ec2 = new TypicaEc2Connection(ec2AccessId, ec2SecretKey);
        return ec2.create(ec2Ami, ec2KeyPairId, Ec2Connection.Ec2InstanceType.DEFAULT, count);
    }

    public static List<HostNamePair> listInstances(String ec2AccessId, String ec2SecretKey)
            throws Exception {
        Ec2Connection ec2 = new TypicaEc2Connection(ec2AccessId, ec2SecretKey);
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

}
