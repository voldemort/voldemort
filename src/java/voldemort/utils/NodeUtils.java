/*
 * Copyright 2013 LinkedIn, Inc
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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;

import com.google.common.collect.Sets;

// TODO: (refactor) Move all of the static "util" methods for which Node is
// the only complex type that the method operates on to be members of the
// Cluster class. Unclear how to treat the Integer and List types of partition
// ids...

/**
 * NodeUtils provides basic tools for manipulating and inspecting nodes.
 * 
 * Methods in this util module take take exactly one Node object, or a
 * collection of Node objects, and possibly some other minor, simple arguments.
 */
public class NodeUtils {

    private static Logger logger = Logger.getLogger(NodeUtils.class);

    /**
     * Creates a replica of the node with the new partitions list
     * 
     * @param node The node whose replica we are creating
     * @param partitionsList The new partitions list
     * @return Replica of node with new partitions list
     */
    public static Node updateNode(Node node, List<Integer> partitionsList) {
        return new Node(node.getId(),
                        node.getHost(),
                        node.getHttpPort(),
                        node.getSocketPort(),
                        node.getAdminPort(),
                        node.getZoneId(),
                        partitionsList);
    }

    /**
     * Add a partition to the node provided
     * 
     * @param node The node to which we'll add the partition
     * @param donatedPartition The partition to add
     * @return The new node with the new partition
     */
    public static Node addPartitionToNode(final Node node, Integer donatedPartition) {
        return addPartitionToNode(node, Sets.newHashSet(donatedPartition));
    }

    /**
     * Remove a partition from the node provided
     * 
     * @param node The node from which we're removing the partition
     * @param donatedPartition The partitions to remove
     * @return The new node without the partition
     */
    public static Node removePartitionToNode(final Node node, Integer donatedPartition) {
        return removePartitionToNode(node, Sets.newHashSet(donatedPartition));
    }

    /**
     * Add the set of partitions to the node provided
     * 
     * @param node The node to which we'll add the partitions
     * @param donatedPartitions The list of partitions to add
     * @return The new node with the new partitions
     */
    public static Node addPartitionToNode(final Node node, final Set<Integer> donatedPartitions) {
        List<Integer> deepCopy = new ArrayList<Integer>(node.getPartitionIds());
        deepCopy.addAll(donatedPartitions);
        Collections.sort(deepCopy);
        return updateNode(node, deepCopy);
    }

    /**
     * Remove the set of partitions from the node provided
     * 
     * @param node The node from which we're removing the partitions
     * @param donatedPartitions The list of partitions to remove
     * @return The new node without the partitions
     */
    public static Node removePartitionToNode(final Node node, final Set<Integer> donatedPartitions) {
        List<Integer> deepCopy = new ArrayList<Integer>(node.getPartitionIds());
        deepCopy.removeAll(donatedPartitions);
        return updateNode(node, deepCopy);
    }

    /**
     * Given a list of nodes, retrieves the list of node ids
     * 
     * @param nodes The list of nodes
     * @return Returns a list of node ids
     */
    public static List<Integer> getNodeIds(List<Node> nodes) {
        List<Integer> nodeIds = new ArrayList<Integer>(nodes.size());
        for(Node node: nodes) {
            nodeIds.add(node.getId());
        }
        return nodeIds;
    }
}
