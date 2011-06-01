/*
 * Copyright 2011 LinkedIn, Inc
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
import java.util.List;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;

public class RebalanceUtilsTest extends TestCase {

    public void testUpdateCluster() {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    public void testGetNodeIds() {
        List<Node> nodes = Lists.newArrayList();

        // Test with empty node list
        assertEquals(RebalanceUtils.getNodeIds(nodes).size(), 0);

        // Add one node
        nodes.add(new Node(0, "localhost", 1, 2, 3, new ArrayList<Integer>()));
        assertEquals(RebalanceUtils.getNodeIds(nodes).size(), 1);
        assertEquals(RebalanceUtils.getNodeIds(nodes).get(0).intValue(), 0);
    }

    public void testGetClusterWithNewNodes() {
        Cluster cluster = ServerTestUtils.getLocalCluster(2, 10, 1);

        // Generate a new cluster which contains 4 nodes instead of 2
        List<Node> nodes = Lists.newArrayList();
        for(int nodeId = 0; nodeId < 4; nodeId++) {
            List<Integer> partitionIds = Lists.newArrayList();
            for(int partitionId = nodeId * 5; partitionId < (nodeId + 1) * 5; partitionId++) {
                partitionIds.add(partitionId);
            }
            Node node = new Node(nodeId, "b", 0, 1, 2, 0, partitionIds);
            nodes.add(node);
        }
        Cluster newCluster = new Cluster(cluster.getName(),
                                         nodes,
                                         Lists.newArrayList(cluster.getZones()));

        Cluster generatedCluster = RebalanceUtils.getClusterWithNewNodes(cluster, newCluster);
        assertEquals(generatedCluster.getNumberOfNodes(), 4);
        assertEquals(Utils.compareList(generatedCluster.getNodeById(0).getPartitionIds(),
                                       cluster.getNodeById(0).getPartitionIds()), true);
        assertEquals(Utils.compareList(generatedCluster.getNodeById(1).getPartitionIds(),
                                       cluster.getNodeById(1).getPartitionIds()), true);
        assertEquals(generatedCluster.getNodeById(2).getPartitionIds().size(), 0);
        assertEquals(generatedCluster.getNodeById(3).getPartitionIds().size(), 0);

    }

    public void testGenerateMinCluster() {
        Cluster cluster = ServerTestUtils.getLocalCluster(6, 12, 2);

        List<Node> newNodes = Lists.newArrayList(cluster.getNodes());
        // Add some more nodes to the list
        newNodes.add(new Node(7, "blah", 0, 1, 2, 0, new ArrayList<Integer>()));
        newNodes.add(new Node(8, "blah2", 0, 1, 2, 0, new ArrayList<Integer>()));
        Cluster newCluster = RebalanceUtils.updateCluster(cluster, newNodes);

        Cluster returnedCluster = RebalanceUtils.generateMinCluster(cluster, newCluster);
        assertEquals(returnedCluster.getNumberOfPartitions(), cluster.getNumberOfPartitions());

    }
}
