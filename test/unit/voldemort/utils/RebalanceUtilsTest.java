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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;

public class RebalanceUtilsTest {

    @Test
    public void testUpdateCluster() {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    @Test
    public void testGetNodeIds() {
        List<Node> nodes = Lists.newArrayList();

        // Test with empty node list
        assertEquals(NodeUtils.getNodeIds(nodes).size(), 0);

        // Add one node
        nodes.add(new Node(0, "localhost", 1, 2, 3, new ArrayList<Integer>()));
        assertEquals(NodeUtils.getNodeIds(nodes).size(), 1);
        assertEquals(NodeUtils.getNodeIds(nodes).get(0).intValue(), 0);
    }

    @Test
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

    private void doClusterTransformationBase(Cluster currentC,
                                             Cluster targetC,
                                             Cluster finalC,
                                             boolean verify) {
        Cluster derivedTarget1 = RebalanceUtils.getClusterWithNewNodes(currentC, targetC);
        if(verify)
            assertEquals(targetC, derivedTarget1);

        Cluster derivedTarget2 = RebalanceUtils.getTargetCluster(currentC, finalC);
        if(verify)
            assertEquals(targetC, derivedTarget2);

        RebalanceUtils.validateCurrentFinalCluster(currentC, finalC);
        RebalanceUtils.validateCurrentTargetCluster(currentC, targetC);
        RebalanceUtils.validateTargetFinalCluster(targetC, finalC);
    }

    private void doClusterTransformation(Cluster currentC, Cluster targetC, Cluster finalC) {
        doClusterTransformationBase(currentC, targetC, finalC, false);
    }

    public void doClusterTransformationAndVerification(Cluster currentC,
                                                       Cluster targetC,
                                                       Cluster finalC) {
        doClusterTransformationBase(currentC, targetC, finalC, true);
    }

    @Test
    public void testClusterTransformationAndVerification() {
        // Two-zone cluster: no-op
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZCluster(),
                                               ClusterInstanceTest.getZZCluster(),
                                               ClusterInstanceTest.getZZCluster());

        // Two-zone cluster: rebalance
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZCluster(),
                                               ClusterInstanceTest.getZZCluster(),
                                               ClusterInstanceTest.getZZClusterWithSwappedPartitions());

        // Two-zone cluster: cluster expansion
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZCluster(),
                                               ClusterInstanceTest.getZZClusterWithNN(),
                                               ClusterInstanceTest.getZZClusterWithPP());

        // Three-zone cluster: no-op
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZZCluster(),
                                               ClusterInstanceTest.getZZZCluster(),
                                               ClusterInstanceTest.getZZZCluster());

        // Three-zone cluster: rebalance
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZZCluster(),
                                               ClusterInstanceTest.getZZZCluster(),
                                               ClusterInstanceTest.getZZZClusterWithSwappedPartitions());

        // Three-zone cluster: cluster expansion
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZZCluster(),
                                               ClusterInstanceTest.getZZZClusterWithNNN(),
                                               ClusterInstanceTest.getZZZClusterWithPPP());

        // TODO: Fix this test to pass. This test currently fails because the
        // method RebalanceUtils.getClusterWithNewNodes cannot handle a new zone
        // coming into existence between currentCluster & targetCluster.
        // Two- to Three-zone clusters: zone expansion
        /*-
        doClusterTransformationAndVerification(ClusterInstanceTest.getZZCluster(),
                                               ClusterInstanceTest.getZZECluster(),
                                               ClusterInstanceTest.getZZEClusterXXP());
         */
    }

    @Test
    public void testClusterTransformationAndVerificationExceptions() {
        boolean excepted;

        // Two-zone cluster: rebalance with extra partitions in target
        excepted = false;
        try {
            doClusterTransformation(ClusterInstanceTest.getZZCluster(),
                                    ClusterInstanceTest.getZZClusterWithExtraPartitions(),
                                    ClusterInstanceTest.getZZClusterWithSwappedPartitions());
        } catch(VoldemortException ve) {
            excepted = true;
        }
        assertTrue(excepted);

        // Two-zone cluster: rebalance with extra partitions in final
        excepted = false;
        try {
            doClusterTransformation(ClusterInstanceTest.getZZCluster(),
                                    ClusterInstanceTest.getZZCluster(),
                                    ClusterInstanceTest.getZZClusterWithExtraPartitions());
        } catch(VoldemortException ve) {
            excepted = true;
        }
        assertTrue(excepted);

        // Two-zone cluster: node ids swapped in target
        excepted = false;
        try {
            doClusterTransformation(ClusterInstanceTest.getZZCluster(),
                                    ClusterInstanceTest.getZZClusterWithNNWithSwappedNodeIds(),
                                    ClusterInstanceTest.getZZClusterWithPP());
        } catch(VoldemortException ve) {
            excepted = true;
        }
        assertTrue(excepted);

        // Two-zone cluster: node ids swapped in final is OK because this is the
        // same as partitions being migrated among nodes.
        excepted = false;
        try {
            doClusterTransformation(ClusterInstanceTest.getZZCluster(),
                                    ClusterInstanceTest.getZZClusterWithNN(),
                                    ClusterInstanceTest.getZZClusterWithPPWithSwappedNodeIds());
        } catch(VoldemortException ve) {
            excepted = true;
        }
        assertFalse(excepted);

        // Two-zone cluster: too many node ids in final
        excepted = false;
        try {
            doClusterTransformation(ClusterInstanceTest.getZZCluster(),
                                    ClusterInstanceTest.getZZClusterWithNN(),
                                    ClusterInstanceTest.getZZClusterWithPPWithTooManyNodes());
        } catch(VoldemortException ve) {
            excepted = true;
        }
        assertTrue(excepted);
    }
}
