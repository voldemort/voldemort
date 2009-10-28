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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;

/**
 * ClusterGenerator generates a cluster.xml file given either a list of hosts or
 * a list of ClusterNodeDescriptor instances.
 * 
 * <p/>
 * 
 * This is largely the same as the generate_cluster_xml.py script, but was
 * created because we need to be able to create new cluster.xml files
 * dynamically from JUnit. It seemed overly kludgey to have a script that calls
 * Java that then calls Python.
 * 
 * <p/>
 * 
 * <b>A note about host names</b>: the host name that is referred to in this
 * class is a system's <i>internal</i> host name, rather than its external name.
 * That is, depending on the network topology, a system may have an internal
 * host name by which it is known by in its local network and an external host
 * name by which it's known by systems external to that network.
 * 
 * <p/>
 * 
 * For example, EC2 systems have both internal host names and external host
 * names. When a system external to EC2 (e.g. a developer's machine or a machine
 * running a testing framework) wants to communicate with EC2 (via SSH, et al.),
 * he must use the EC2 instance's external host name. However, for the EC2
 * instances to communicate amongst themselves (e.g. when running the Voldemort
 * tests), the Voldemort cluster nodes and Voldemort test nodes must use the
 * internal host name. The external host name is used by the development/testing
 * system to reach EC2 for orchestrating the testing. But the communication of
 * the test and server nodes in the test are all on the same network, using the
 * internal host name.
 * 
 * @author Kirk True
 */

public class ClusterGenerator {

    /**
     * Creates a list of ClusterNodeDescriptor instances given a list of host
     * names.
     * 
     * <p/>
     * 
     * <b>Please see "A note about host names"</b> in the class' JavaDoc for
     * important clarification as to the type of host names used.
     * 
     * @param hostNames <i>Internal</i> host name
     * @param numPartitions Number of partitions per host
     * 
     * @return
     */

    public List<ClusterNodeDescriptor> createClusterNodeDescriptors(List<String> hostNames,
                                                                    int numPartitions) {
        List<Integer> ids = range(hostNames.size() * numPartitions);

        Random random = new Random(5276239082346L);
        Collections.shuffle(ids, random);

        List<ClusterNodeDescriptor> list = new ArrayList<ClusterNodeDescriptor>();

        for(int i = 0; i < hostNames.size(); i++) {
            String hostName = hostNames.get(i);
            List<Integer> partitions = ids.subList(i * numPartitions, (i + 1) * numPartitions);
            Collections.sort(partitions);

            ClusterNodeDescriptor cnd = new ClusterNodeDescriptor();
            cnd.setHostName(hostName);
            cnd.setId(i);
            cnd.setPartitions(partitions);

            list.add(cnd);
        }

        return list;
    }

    public String createClusterDescriptor(String clusterName,
                                          List<String> hostNames,
                                          int numPartitions) {
        List<ClusterNodeDescriptor> clusterNodeDescriptors = createClusterNodeDescriptors(hostNames,
                                                                                          numPartitions);

        return createClusterDescriptor(clusterName, clusterNodeDescriptors);
    }

    public String createClusterDescriptor(String clusterName,
                                          List<ClusterNodeDescriptor> clusterNodeDescriptors) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("<cluster>");
        pw.println("\t<name>" + clusterName + "</name>");

        for(ClusterNodeDescriptor cnd: clusterNodeDescriptors) {
            String partitions = StringUtils.join(cnd.getPartitions(), ", ");

            pw.println("\t<server>");
            pw.println("\t\t<id>" + cnd.getId() + "</id>");
            pw.println("\t\t<host>" + cnd.getHostName() + "</host>");
            pw.println("\t\t<http-port>" + cnd.getHttpPort() + "</http-port>");
            pw.println("\t\t<socket-port>" + cnd.getSocketPort() + "</socket-port>");
            pw.println("\t\t<partitions>" + partitions + "</partitions>");
            pw.println("\t</server>");
        }

        pw.println("</cluster>");

        return sw.toString();
    }

    private List<Integer> range(int stop) {
        List<Integer> ids = new ArrayList<Integer>();

        for(int i = 0; i < stop; i++)
            ids.add(i);

        return ids;
    }

}
