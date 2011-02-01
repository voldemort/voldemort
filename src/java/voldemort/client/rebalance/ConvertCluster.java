/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client.rebalance;

import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.store.StoreDefinition;
import voldemort.utils.KeyDistributionGenerator;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * Converts a cluster xml from a non-zone aware one to a zone aware one
 */
public class ConvertCluster {

    private final Cluster cluster;
    private final List<StoreDefinition> storeDefs;

    public ConvertCluster(Cluster cluster, List<StoreDefinition> storeDefs) {
        this.cluster = cluster;
        this.storeDefs = storeDefs;
    }

    public Cluster convertToZoneAware() {
        List<Node> newNodes = Lists.newArrayList();
        int origNodes = cluster.getNumberOfNodes() / 2;
        for(int nodeId = 0; nodeId < origNodes; nodeId++) {
            List<Integer> secondPartitions = Lists.newArrayList();
            List<Integer> firstPartitions = Lists.newArrayList();
            List<Integer> partitions = Lists.newArrayList(cluster.getNodeById(nodeId)
                                                                 .getPartitionIds());
            partitions.addAll(cluster.getNodeById(origNodes + nodeId).getPartitionIds());
            Collections.shuffle(partitions);
            int numPartitionsPerZone = partitions.size() / 2;

            if(numPartitionsPerZone <= 0)
                throw new IllegalStateException("Invalid number of partitions");

            for(int i = 0; i < numPartitionsPerZone; i++)
                firstPartitions.add(partitions.get(i));
            for(int i = numPartitionsPerZone; i < partitions.size(); i++)
                secondPartitions.add(partitions.get(i));

            newNodes.add(new Node(cluster.getNodeById(nodeId).getId(),
                                  cluster.getNodeById(nodeId).getHost(),
                                  cluster.getNodeById(nodeId).getHttpPort(),
                                  cluster.getNodeById(nodeId).getSocketPort(),
                                  cluster.getNodeById(nodeId).getAdminPort(),
                                  cluster.getNodeById(nodeId).getZoneId(),
                                  firstPartitions));
            newNodes.add(new Node(cluster.getNodeById(nodeId + origNodes).getId(),
                                  cluster.getNodeById(nodeId + origNodes).getHost(),
                                  cluster.getNodeById(nodeId + origNodes).getHttpPort(),
                                  cluster.getNodeById(nodeId + origNodes).getSocketPort(),
                                  cluster.getNodeById(nodeId + origNodes).getAdminPort(),
                                  cluster.getNodeById(nodeId + origNodes).getZoneId(),
                                  secondPartitions));
        }

        Cluster newCluster = new Cluster(cluster.getName(),
                                         newNodes,
                                         Lists.<Zone>newArrayList(cluster.getZones()));
        RebalanceClusterPlan plan = new RebalanceClusterPlan(cluster,
                                                             newCluster,
                                                             storeDefs,
                                                             false,
                                                             null);
        System.out.println(plan);
        for(StoreDefinition def: storeDefs) {
            System.out.println("Store: " + def);
            KeyDistributionGenerator.printDistribution((KeyDistributionGenerator.generateDistribution(newCluster,
                                                                                                      def,
                                                                                                      10000)));
        }

        return newCluster;
    }

    public static void main(String args[])  {
        if(args.length != 3)
            Utils.croak("Usage: ConvertCluster cluster.xml stores.xml targetCluster.xml");

        try {
            Cluster cluster = new ClusterMapper().readCluster(new File(args[0]));
            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(args[1]));

            ConvertCluster convertCluster = new ConvertCluster(cluster, storeDefs);
            Cluster targetCluster = convertCluster.convertToZoneAware();
            String targetClusterXml = new ClusterMapper().writeCluster(targetCluster);

            FileWriter clusterXmlWriter = new FileWriter(new File(args[2]));
            clusterXmlWriter.write(targetClusterXml);
            clusterXmlWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            Utils.croak(e.getMessage());
        }
    }
}
