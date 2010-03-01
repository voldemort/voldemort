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
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

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
 * 
 * @see ClusterNodeDescriptor
 */

public class ClusterGenerator {

    private final static long SEED = 5276239082346L;

    /**
     * Creates a list of ClusterNodeDescriptor instances given a list of host
     * names. The returned list of ClusterNodeDescriptor have only the host
     * name, ID, and partition list attributes set.
     * 
     * <p/>
     * 
     * The list of partitions is randomly distributed over the list of hosts
     * using a hard-coded random seed. This mimics the behavior of
     * generate_cluster_xml.py.
     * 
     * <p/>
     * 
     * <b>Please see "A note about host names"</b> in the class' JavaDoc for
     * important clarification as to the type of host names used.
     * 
     * @param hostNames <i>Internal</i> host name
     * @param numPartitions Number of partitions <b>per host</b>
     * 
     * @return List of ClusterNodeDescriptor
     */

    public List<ClusterNodeDescriptor> createClusterNodeDescriptors(List<String> hostNames,
                                                                    int numPartitions) {
        // Create a list of integers [0..totalPartitions).
        int totalPartitions = hostNames.size() * numPartitions;
        List<Integer> allPartitionIds = new ArrayList<Integer>();

        for(int i = 0; i < totalPartitions; i++)
            allPartitionIds.add(i);

        Random random = new Random(SEED);
        Collections.shuffle(allPartitionIds, random);

        List<ClusterNodeDescriptor> list = new ArrayList<ClusterNodeDescriptor>();

        for(int i = 0; i < hostNames.size(); i++) {
            String hostName = hostNames.get(i);
            List<Integer> partitions = allPartitionIds.subList(i * numPartitions, (i + 1)
                                                                                  * numPartitions);
            Collections.sort(partitions);

            ClusterNodeDescriptor cnd = new ClusterNodeDescriptor();
            cnd.setHostName(hostName);
            cnd.setId(i);
            cnd.setPartitions(partitions);

            list.add(cnd);
        }

        return list;
    }

    public List<ClusterNodeDescriptor> createClusterNodeDescriptors(List<String> hostNames,
                                                                    Cluster cluster) {
        if (cluster.getNumberOfNodes() > hostNames.size())
            throw new IllegalStateException("cluster size exceeds the number of available instances");

        List<ClusterNodeDescriptor> list = new ArrayList<ClusterNodeDescriptor>();
        for(int i = 0; i < cluster.getNumberOfNodes(); i++) {
            Node node = cluster.getNodeById(i);
            String hostName = hostNames.get(i);
            List<Integer> partitions = node.getPartitionIds();

            ClusterNodeDescriptor cnd = new ClusterNodeDescriptor();
            cnd.setHostName(hostName);
            cnd.setId(i);
            cnd.setSocketPort(node.getSocketPort());
            cnd.setHttpPort(node.getHttpPort());
            cnd.setAdminPort(node.getAdminPort());
            cnd.setPartitions(partitions);

            list.add(cnd);
        }

        return list;
    }


    /**
     * Creates a String representing the format used by cluster.xml given the
     * cluster name, host names, and number of partitions for each host.
     * 
     * @param clusterName Name of cluster
     * @param hostNames <i>Internal</i> host name
     * @param numPartitions Number of partitions <b>per host</b>
     * 
     * @return String of formatted XML as used by cluster.xml
     */

    public String createClusterDescriptor(String clusterName,
                                          List<String> hostNames,
                                          int numPartitions) {
        List<ClusterNodeDescriptor> clusterNodeDescriptors = createClusterNodeDescriptors(hostNames,
                                                                                          numPartitions);

        return createClusterDescriptor(clusterName, clusterNodeDescriptors);
    }

    /**
     * Creates a String representing the format used by cluster.xml given the
     * cluster name and a list of ClusterNodeDescriptor instances.
     * 
     * @param clusterName Name of cluster
     * @param clusterNodeDescriptors List of ClusterNodeDescriptor instances
     *        with which the string is generated
     * 
     * @return String of formatted XML as used by cluster.xml
     */

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
            pw.println("\t\t<admin-port>" + cnd.getAdminPort() + "</admin-port>");
            pw.println("\t\t<partitions>" + partitions + "</partitions>");
            pw.println("\t</server>");
        }

        pw.println("</cluster>");

        return sw.toString();
    }

}
