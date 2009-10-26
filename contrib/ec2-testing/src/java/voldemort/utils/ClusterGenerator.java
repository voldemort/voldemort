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

public class ClusterGenerator {

    public List<ClusterNodeDescriptor> createClusterNodeDescriptors(List<String> hostNames,
                                                                    int partitions) {
        List<Integer> ids = range(hostNames.size() * partitions);

        Random random = new Random(5276239082346L);
        Collections.shuffle(ids, random);

        List<ClusterNodeDescriptor> list = new ArrayList<ClusterNodeDescriptor>();

        for(int i = 0; i < hostNames.size(); i++) {
            String hostName = hostNames.get(i);
            List<Integer> partitionsList = ids.subList(i * partitions, (i + 1) * partitions);
            Collections.sort(partitionsList);

            list.add(new ClusterNodeDescriptor(hostName, i, partitionsList));
        }

        return list;
    }

    public String createClusterDescriptor(String clusterName, List<String> hostNames, int partitions) {
        List<ClusterNodeDescriptor> clusterNodeDescriptors = createClusterNodeDescriptors(hostNames,
                                                                                          partitions);

        return createClusterDescriptor(clusterName, clusterNodeDescriptors);
    }

    public String createClusterDescriptor(String clusterName,
                                          List<ClusterNodeDescriptor> clusterNodeDescriptors) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("<cluster>");
        pw.println("    <name>" + clusterName + "</name>");

        for(ClusterNodeDescriptor clusterNodeDescriptor: clusterNodeDescriptors) {
            String partitionsList = StringUtils.join(clusterNodeDescriptor.getPartitions(), ", ");

            pw.println("    <server>");
            pw.println("        <id>" + clusterNodeDescriptor.getId() + "</id>");
            pw.println("        <host>" + clusterNodeDescriptor.getHostName() + "</host>");
            pw.println("        <http-port>8081</http-port>");
            pw.println("        <socket-port>6666</socket-port>");
            pw.println("        <partitions>" + partitionsList + "</partitions>");
            pw.println("    </server>");
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
