/*
 * Copyright 2011-2013 LinkedIn, Inc
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
        assertEquals(NodeUtils.getNodeIds(nodes).size(), 0);

        // Add one node
        nodes.add(new Node(0, "localhost", 1, 2, 3, new ArrayList<Integer>()));
        assertEquals(NodeUtils.getNodeIds(nodes).size(), 1);
        assertEquals(NodeUtils.getNodeIds(nodes).get(0).intValue(), 0);
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

    public void testRemoveItemsToSplitListEvenly() {
        // input of size 5
        List<Integer> input = new ArrayList<Integer>();
        System.out.println("Input of size 5");
        for(int i = 0; i < 5; ++i) {
            input.add(i);
        }

        List<Integer> output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(1), new Integer(3));
        System.out.println("1 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("2 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("3 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("4 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 0);
        System.out.println("5 : " + output);

        // input of size 10
        input.clear();
        System.out.println("Input of size 10");
        for(int i = 0; i < 10; ++i) {
            input.add(i);
        }

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(4), new Integer(9));
        System.out.println("1 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(2), new Integer(8));
        System.out.println("2 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("3 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("4 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("5 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("6 : " + output);

        // input of size 20
        input.clear();
        System.out.println("Input of size 20");
        for(int i = 0; i < 20; ++i) {
            input.add(i);
        }

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 10);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(9), new Integer(19));
        System.out.println("1 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 6);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(5), new Integer(17));
        System.out.println("2 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(4), new Integer(17));
        System.out.println("3 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 4);
        assertEquals(output.get(0), new Integer(4));
        assertEquals(output.get(3), new Integer(16));
        System.out.println("4 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(5));
        assertEquals(output.get(2), new Integer(15));
        System.out.println("5 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("6 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 7);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("7 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 9);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("9 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 10);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("10 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 11);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("11 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 19);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("19 : " + output);

        output = RebalanceClusterUtils.removeItemsToSplitListEvenly(input, 20);
        assertEquals(output.size(), 0);
        System.out.println("20 : " + output);
    }
}
