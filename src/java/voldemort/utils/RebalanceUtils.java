/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.RebalanceNodePlan;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.VoldemortConfig;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.tools.PartitionBalance;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * RebalanceUtils provide basic functionality for rebalancing.
 * 
 */
public class RebalanceUtils {

    private static Logger logger = Logger.getLogger(RebalanceUtils.class);

    public final static List<String> canRebalanceList = Arrays.asList(BdbStorageConfiguration.TYPE_NAME,
                                                                      ReadOnlyStorageConfiguration.TYPE_NAME);

    public final static String currentClusterFileName = "current-cluster.xml";
    public final static String finalClusterFileName = "final-cluster.xml";

    /**
     * Given the current replica to partition list, try to check if the donor
     * node would already contain that partition and if yes, ignore it
     * 
     * @param stealerNodeId Stealer node id
     * @param cluster Cluster metadata
     * @param storeDef Store definition
     * @param currentReplicaToPartitionList Current replica to partition list
     * @return Optimized replica to partition list
     */
    public static HashMap<Integer, List<Integer>> getOptimizedReplicaToPartitionList(int stealerNodeId,
                                                                                     Cluster cluster,
                                                                                     StoreDefinition storeDef,
                                                                                     HashMap<Integer, List<Integer>> currentReplicaToPartitionList) {

        HashMap<Integer, List<Integer>> optimizedReplicaToPartitionList = Maps.newHashMap();
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);
        for(Entry<Integer, List<Integer>> tuple: currentReplicaToPartitionList.entrySet()) {
            List<Integer> partitionList = Lists.newArrayList();
            for(int partition: tuple.getValue()) {
                List<Integer> preferenceList = strategy.getReplicatingPartitionList(partition);

                // If this node was already in the
                // preference list before, a copy of the
                // data will already exist - Don't copy
                // it!
                if(!ClusterUtils.containsPreferenceList(cluster, preferenceList, stealerNodeId)) {
                    partitionList.add(partition);
                }
            }

            if(partitionList.size() > 0) {
                optimizedReplicaToPartitionList.put(tuple.getKey(), partitionList);
            }
        }

        return optimizedReplicaToPartitionList;

    }

    /**
     * Get the latest cluster from all available nodes in the cluster<br>
     * 
     * Throws exception if:<br>
     * A) Any node in the required nodes list fails to respond.<br>
     * B) Cluster is in inconsistent state with concurrent versions for cluster
     * metadata on any two nodes.<br>
     * 
     * @param requiredNodes List of nodes from which we definitely need an
     *        answer
     * @param adminClient Admin client used to query the nodes
     * @return Returns the latest cluster metadata
     */
    public static Versioned<Cluster> getLatestCluster(List<Integer> requiredNodes,
                                                      AdminClient adminClient) {
        Versioned<Cluster> latestCluster = new Versioned<Cluster>(adminClient.getAdminClientCluster());
        ArrayList<Versioned<Cluster>> clusterList = new ArrayList<Versioned<Cluster>>();

        clusterList.add(latestCluster);
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            try {
                Versioned<Cluster> versionedCluster = adminClient.metadataMgmtOps.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();
                if(null != newClock && !clusterList.contains(versionedCluster)) {
                    // check no two clocks are concurrent.
                    checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occurred occurred = newClock.compare(latestCluster.getVersion());
                    if(Occurred.AFTER.equals(occurred))
                        latestCluster = versionedCluster;
                }
            } catch(Exception e) {
                if(null != requiredNodes && requiredNodes.contains(node.getId()))
                    throw new VoldemortException("Failed on node " + node.getId(), e);
                else
                    logger.info("Failed on node " + node.getId(), e);
            }
        }

        return latestCluster;
    }

    private static void checkNotConcurrent(ArrayList<Versioned<Cluster>> clockList,
                                           VectorClock newClock) {
        for(Versioned<Cluster> versionedCluster: clockList) {
            VectorClock clock = (VectorClock) versionedCluster.getVersion();
            if(Occurred.CONCURRENTLY.equals(clock.compare(newClock)))
                throw new VoldemortException("Cluster is in inconsistent state because we got conflicting clocks "
                                             + clock + " and on current node " + newClock);

        }
    }

    /**
     * Given a list of partition informations check all of them belong to the
     * same donor node
     * 
     * @param partitionInfos List of partition infos
     * @param expectedDonorId Expected donor node id ( If -1, then just checks
     *        if all are same )
     */
    public static void assertSameDonor(List<RebalancePartitionsInfo> partitionInfos,
                                       int expectedDonorId) {
        int donorId = (expectedDonorId < 0) ? partitionInfos.get(0).getDonorId() : expectedDonorId;
        for(RebalancePartitionsInfo info: partitionInfos) {
            if(info.getDonorId() != donorId) {
                throw new VoldemortException("Found a stealer information " + info
                                             + " having a different donor node from others ( "
                                             + donorId + " )");
            }
        }
    }

    /**
     * Check the execution state of the server by checking the state of
     * {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are all in normal state (
     * {@link VoldemortState#NORMAL_SERVER}).
     * 
     * @param cluster Cluster metadata whose nodes we are checking
     * @param adminClient Admin client used to query
     * @throws VoldemortRebalancingException if any node is not in normal state
     */
    public static void validateProdClusterStateIsNormal(final Cluster cluster,
                                                        final AdminClient adminClient) {
        for(Node node: cluster.getNodes()) {
            Versioned<VoldemortState> versioned = adminClient.rebalanceOps.getRemoteServerState(node.getId());

            if(!VoldemortState.NORMAL_SERVER.equals(versioned.getValue())) {
                throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                        + node.getId() + " (" + node.getHost()
                                                        + ") is not in normal state, but in "
                                                        + versioned.getValue());
            } else {
                if(logger.isInfoEnabled()) {
                    logger.info("Node " + node.getId() + " (" + node.getHost()
                                + ") is ready for rebalance.");
                }
            }
        }
    }

    /**
     * Verify store definitions are congruent with cluster definition.
     * 
     * @param cluster
     * @param stores
     */
    public static void validateClusterStores(final Cluster cluster,
                                             final List<StoreDefinition> storeDefs) {
        // Constructing a PartitionBalance object has the (desirable in this
        // case) side-effect of verifying that the store definition is congruent
        // with the cluster definition. If there are issues, exceptions are
        // thrown.
        new PartitionBalance(cluster, storeDefs);
        return;
    }

    /**
     * A final cluster ought to be a super set of current cluster. I.e.,
     * existing node IDs ought to map to same server, but partition layout can
     * have changed and there may exist new nodes.
     * 
     * @param currentCluster
     * @param finalCluster
     */
    public static void validateCurrentFinalCluster(final Cluster currentCluster,
                                                   final Cluster finalCluster) {
        validateClusterPartitionCounts(currentCluster, finalCluster);
        validateClusterNodeState(currentCluster, finalCluster);

        return;
    }

    /**
     * A target cluster ought to be a super set of current cluster. I.e., it
     * ought to either be the same as current cluster (every partition is mapped
     * to the same node of current & target), or it ought to have more nodes
     * (possibly in new zones) without partitions.
     * 
     * @param currentCluster
     * @param targetCluster
     */
    public static void validateCurrentTargetCluster(final Cluster currentCluster,
                                                    final Cluster targetCluster) {
        validateClusterPartitionCounts(currentCluster, targetCluster);
        validateClusterNodeState(currentCluster, targetCluster);
        validateClusterPartitionState(currentCluster, targetCluster);

        return;
    }

    /**
     * Target and final ought to have same partition counts, same zones, and
     * same node state. Partitions per node may of course differ.
     * 
     * @param targetCluster
     * @param finalCluster
     */
    public static void validateTargetFinalCluster(final Cluster targetCluster,
                                                  final Cluster finalCluster) {
        validateClusterPartitionCounts(targetCluster, finalCluster);
        validateClusterZonesSame(targetCluster, finalCluster);
        validateClusterNodeCounts(targetCluster, finalCluster);
        validateClusterNodeState(targetCluster, finalCluster);
        return;
    }

    /**
     * Confirms that both clusters have the same number of total partitions.
     * 
     * @param lhs
     * @param rhs
     */
    public static void validateClusterPartitionCounts(final Cluster lhs, final Cluster rhs) {
        if(lhs.getNumberOfPartitions() != rhs.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should be equal [ lhs cluster ("
                                         + lhs.getNumberOfPartitions()
                                         + ") not equal to rhs cluster ("
                                         + rhs.getNumberOfPartitions() + ") ]");
    }

    /**
     * Confirm that all nodes shared between clusters host exact same partition
     * IDs and that nodes only in the super set cluster have no partition IDs.
     * 
     * @param subsetCluster
     * @param supersetCluster
     */
    public static void validateClusterPartitionState(final Cluster subsetCluster,
                                                     final Cluster supersetCluster) {
        if(!supersetCluster.getNodeIds().containsAll(subsetCluster.getNodeIds())) {
            throw new VoldemortException("Superset cluster does not contain all nodes from subset cluster[ subset cluster node ids ("
                                         + subsetCluster.getNodeIds()
                                         + ") are not a subset of superset cluster node ids ("
                                         + supersetCluster.getNodeIds() + ") ]");

        }
        for(int nodeId: subsetCluster.getNodeIds()) {
            Node supersetNode = supersetCluster.getNodeById(nodeId);
            Node subsetNode = subsetCluster.getNodeById(nodeId);
            if(!supersetNode.getPartitionIds().equals(subsetNode.getPartitionIds())) {
                throw new VoldemortRebalancingException("Partition IDs do not match between clusters for nodes with id "
                                                        + nodeId
                                                        + " : subset cluster has "
                                                        + subsetNode.getPartitionIds()
                                                        + " and superset cluster has "
                                                        + supersetNode.getPartitionIds());
            }
        }
        Set<Integer> nodeIds = supersetCluster.getNodeIds();
        nodeIds.removeAll(subsetCluster.getNodeIds());
        for(int nodeId: nodeIds) {
            Node supersetNode = supersetCluster.getNodeById(nodeId);
            if(!supersetNode.getPartitionIds().isEmpty()) {
                throw new VoldemortRebalancingException("New node "
                                                        + nodeId
                                                        + " in superset cluster already has partitions: "
                                                        + supersetNode.getPartitionIds());
            }
        }
    }

    /**
     * Confirms that both clusters have the same set of zones defined.
     * 
     * @param lhs
     * @param rhs
     */
    public static void validateClusterZonesSame(final Cluster lhs, final Cluster rhs) {
        if(lhs.getZones().equals(rhs.getZones()))
            throw new VoldemortException("Zones are not the same [ lhs cluster zones ("
                                         + lhs.getZones() + ") not equal to rhs cluster zones ("
                                         + rhs.getZones() + ") ]");
    }

    /**
     * Confirms that both clusters have the same number of nodes by comparing
     * set of node Ids between clusters.
     * 
     * @param lhs
     * @param rhs
     */
    public static void validateClusterNodeCounts(final Cluster lhs, final Cluster rhs) {
        if(!lhs.getNodeIds().equals(rhs.getNodeIds())) {
            throw new VoldemortException("Node ids are not the same [ lhs cluster node ids ("
                                         + lhs.getNodeIds()
                                         + ") not equal to rhs cluster node ids ("
                                         + rhs.getNodeIds() + ") ]");
        }
    }

    /**
     * Confirms that any nodes from supersetCluster that are in subsetCluster
     * have the same state (i.e., node id, host name, and ports). Specific
     * partitions hosted are not compared.
     * 
     * @param subsetCluster
     * @param supersetCluster
     */
    public static void validateClusterNodeState(final Cluster subsetCluster,
                                                final Cluster supersetCluster) {
        if(!supersetCluster.getNodeIds().containsAll(subsetCluster.getNodeIds())) {
            throw new VoldemortException("Superset cluster does not contain all nodes from subset cluster[ subset cluster node ids ("
                                         + subsetCluster.getNodeIds()
                                         + ") are not a subset of superset cluster node ids ("
                                         + supersetCluster.getNodeIds() + ") ]");

        }
        for(Node subsetNode: subsetCluster.getNodes()) {
            Node supersetNode = supersetCluster.getNodeById(subsetNode.getId());
            if(!subsetNode.isEqualState(supersetNode)) {
                throw new VoldemortException("Nodes do not have same state[ subset node state ("
                                             + subsetNode.getStateString()
                                             + ") not equal to superset node state ("
                                             + supersetNode.getStateString() + ") ]");
            }
        }
    }

    /**
     * Given the current cluster and final cluster, generates a target cluster
     * with empty new nodes (and zones).
     * 
     * @param currentCluster Current cluster metadata
     * @param finalCluster Final cluster metadata
     * @return Returns a new target cluster which contains nodes and zones of
     *         final cluster, but with empty partition lists if they were not
     *         present in current cluster.
     */
    public static Cluster getTargetCluster(Cluster currentCluster, Cluster finalCluster) {
        List<Node> newNodeList = new ArrayList<Node>(currentCluster.getNodes());
        for(Node node: finalCluster.getNodes()) {
            if(!ClusterUtils.containsNode(currentCluster, node.getId())) {
                newNodeList.add(NodeUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        Collections.sort(newNodeList);
        return new Cluster(currentCluster.getName(),
                           newNodeList,
                           Lists.newArrayList(finalCluster.getZones()));
    }

    /**
     * Given the current cluster and a target cluster, generates a cluster with
     * new nodes ( which in turn contain empty partition lists )
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster Target cluster metadata
     * @return Returns a new cluster which contains nodes of the current cluster
     *         + new nodes
     */
    public static Cluster getClusterWithNewNodes(Cluster currentCluster, Cluster targetCluster) {
        ArrayList<Node> newNodes = new ArrayList<Node>();
        for(Node node: targetCluster.getNodes()) {
            if(!ClusterUtils.containsNode(currentCluster, node.getId())) {
                newNodes.add(NodeUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        return updateCluster(currentCluster, newNodes);
    }

    /**
     * Concatenates the list of current nodes in the given cluster with the new
     * nodes provided and returns an updated cluster metadata. <br>
     * If the nodes being updated already exist in the current metadata, we take
     * the updated ones
     * 
     * @param currentCluster The current cluster metadata
     * @param updatedNodeList The list of new nodes to be added
     * @return New cluster metadata containing both the sets of nodes
     */
    public static Cluster updateCluster(Cluster currentCluster, List<Node> updatedNodeList) {
        List<Node> newNodeList = new ArrayList<Node>(updatedNodeList);
        for(Node currentNode: currentCluster.getNodes()) {
            if(!updatedNodeList.contains(currentNode))
                newNodeList.add(currentNode);
        }

        Collections.sort(newNodeList);
        return new Cluster(currentCluster.getName(),
                           newNodeList,
                           Lists.newArrayList(currentCluster.getZones()));
    }

    /**
     * Updates the existing cluster such that we remove partitions mentioned
     * from the stealer node and add them to the donor node
     * 
     * @param currentCluster Existing cluster metadata. Both stealer and donor
     *        node should already exist in this metadata
     * @param stealerNodeId Id of node for which we are stealing the partitions
     * @param donatedPartitions List of partitions we are moving
     * @param partitionList List of partitions we are moving
     * @return Updated cluster metadata
     */
    public static Cluster createUpdatedCluster(Cluster currentCluster,
                                               int stealerNodeId,
                                               List<Integer> donatedPartitions) {

        // Clone the cluster
        ClusterMapper mapper = new ClusterMapper();
        Cluster updatedCluster = mapper.readCluster(new StringReader(mapper.writeCluster(currentCluster)));

        // Go over every donated partition one by one
        for(int donatedPartition: donatedPartitions) {

            // Gets the donor Node that owns this donated partition
            Node donorNode = updatedCluster.getNodeForPartitionId(donatedPartition);
            Node stealerNode = updatedCluster.getNodeById(stealerNodeId);

            if(donorNode == stealerNode) {
                // Moving to the same location = No-op
                continue;
            }

            // Update the list of partitions for this node
            donorNode = NodeUtils.removePartitionToNode(donorNode, donatedPartition);
            stealerNode = NodeUtils.addPartitionToNode(stealerNode, donatedPartition);

            // Sort the nodes
            updatedCluster = updateCluster(updatedCluster,
                                           Lists.newArrayList(donorNode, stealerNode));

        }

        return updatedCluster;
    }

    /**
     * Attempt to propagate a cluster definition to all nodes. Also rollback is
     * in place in case one of them fails
     * 
     * @param adminClient {@link voldemort.client.protocol.admin.AdminClient}
     *        instance to use.
     * @param cluster Cluster definition to propagate
     */
    public static void propagateCluster(AdminClient adminClient, Cluster cluster) {

        // Contains a mapping of node id to the existing cluster definition
        HashMap<Integer, Cluster> currentClusters = Maps.newHashMap();

        Versioned<Cluster> latestCluster = new Versioned<Cluster>(cluster);
        ArrayList<Versioned<Cluster>> clusterList = new ArrayList<Versioned<Cluster>>();
        clusterList.add(latestCluster);

        for(Node node: cluster.getNodes()) {
            try {
                Versioned<Cluster> versionedCluster = adminClient.metadataMgmtOps.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();

                // Update the current cluster information
                currentClusters.put(node.getId(), versionedCluster.getValue());

                if(null != newClock && !clusterList.contains(versionedCluster)) {
                    // check no two clocks are concurrent.
                    checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occurred occurred = newClock.compare(latestCluster.getVersion());
                    if(Occurred.AFTER.equals(occurred))
                        latestCluster = versionedCluster;
                }

            } catch(Exception e) {
                throw new VoldemortException("Failed to get cluster version from node "
                                             + node.getId(), e);
            }
        }

        // Vector clock to propagate
        VectorClock latestClock = ((VectorClock) latestCluster.getVersion()).incremented(0,
                                                                                         System.currentTimeMillis());

        // Alright, now try updating the values...
        Set<Integer> completedNodeIds = Sets.newHashSet();
        try {
            for(Node node: cluster.getNodes()) {
                logger.info("Updating cluster definition on remote node " + node);
                adminClient.metadataMgmtOps.updateRemoteCluster(node.getId(), cluster, latestClock);
                logger.info("Updated cluster definition " + cluster + " on remote node "
                            + node.getId());
                completedNodeIds.add(node.getId());
            }
        } catch(VoldemortException e) {
            // Fail early...
            for(Integer completedNodeId: completedNodeIds) {
                try {
                    adminClient.metadataMgmtOps.updateRemoteCluster(completedNodeId,
                                                                    currentClusters.get(completedNodeId),
                                                                    latestClock);
                } catch(VoldemortException exception) {
                    logger.error("Could not revert cluster metadata back on node "
                                 + completedNodeId);
                }
            }
            throw e;
        }

    }

    /**
     * For a particular stealer node find all the "primary" <replica, partition>
     * tuples it will steal. In other words, expect the "replica" part to be 0
     * always.
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param targetCluster The target cluster definition
     * @param stealNodeId Node id of the stealer node
     * @return Returns a list of primary partitions which this stealer node will
     *         get
     */
    public static List<Integer> getStolenPrimaryPartitions(final Cluster currentCluster,
                                                           final Cluster targetCluster,
                                                           final int stealNodeId) {
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(ClusterUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return targetList;
    }

    /**
     * Find all [replica_type, partition] tuples to be stolen
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster Target cluster metadata
     * @param storeDef Store Definition
     * @return Map of stealer node id to sets of [ replica_type, partition ]
     *         tuples
     */
    public static Map<Integer, Set<Pair<Integer, Integer>>> getStolenPartitionTuples(final Cluster currentCluster,
                                                                                     final Cluster targetCluster,
                                                                                     final StoreDefinition storeDef) {
        Map<Integer, Set<Pair<Integer, Integer>>> currentNodeIdToReplicas = getNodeIdToAllPartitions(currentCluster,
                                                                                                     storeDef,
                                                                                                     true);
        Map<Integer, Set<Pair<Integer, Integer>>> targetNodeIdToReplicas = getNodeIdToAllPartitions(targetCluster,
                                                                                                    storeDef,
                                                                                                    true);

        Map<Integer, Set<Pair<Integer, Integer>>> stealerNodeToStolenPartitionTuples = Maps.newHashMap();
        for(int stealerId: NodeUtils.getNodeIds(Lists.newArrayList(targetCluster.getNodes()))) {
            Set<Pair<Integer, Integer>> clusterStealerReplicas = currentNodeIdToReplicas.get(stealerId);
            Set<Pair<Integer, Integer>> targetStealerReplicas = targetNodeIdToReplicas.get(stealerId);

            Set<Pair<Integer, Integer>> diff = Utils.getAddedInTarget(clusterStealerReplicas,
                                                                      targetStealerReplicas);

            if(diff != null && diff.size() > 0) {
                stealerNodeToStolenPartitionTuples.put(stealerId, diff);
            }
        }
        return stealerNodeToStolenPartitionTuples;
    }

    /**
     * Given a mapping of existing node ids to their partition tuples and
     * another new set of node ids to partition tuples, combines them together
     * and puts it into the existing partition tuples
     * 
     * @param existingPartitionTuples Existing partition tuples ( Will include
     *        the new partition tuples at the end of this function )
     * @param newPartitionTuples New partition tuples
     */
    public static void combinePartitionTuples(Map<Integer, Set<Pair<Integer, Integer>>> existingPartitionTuples,
                                              Map<Integer, Set<Pair<Integer, Integer>>> newPartitionTuples) {

        for(int nodeId: newPartitionTuples.keySet()) {
            Set<Pair<Integer, Integer>> tuples = null;
            if(existingPartitionTuples.containsKey(nodeId)) {
                tuples = existingPartitionTuples.get(nodeId);
            } else {
                tuples = Sets.newHashSet();
                existingPartitionTuples.put(nodeId, tuples);
            }
            tuples.addAll(newPartitionTuples.get(nodeId));
        }
    }

    /**
     * For a particular cluster creates a mapping of node id to their
     * corresponding list of [ replicaType, partition ] tuple
     * 
     * @param cluster The cluster metadata
     * @param storeDef The store definition
     * @param includePrimary Include the primary partition?
     * @return Map of node id to set of [ replicaType, partition ] tuple
     */
    public static Map<Integer, Set<Pair<Integer, Integer>>> getNodeIdToAllPartitions(final Cluster cluster,
                                                                                     final StoreDefinition storeDef,
                                                                                     boolean includePrimary) {
        final RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                   cluster);

        final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToReplicas = new HashMap<Integer, Set<Pair<Integer, Integer>>>();
        final Map<Integer, Integer> partitionToNodeIdMap = ClusterUtils.getCurrentPartitionMapping(cluster);

        // Map initialization.
        for(Node node: cluster.getNodes()) {
            nodeIdToReplicas.put(node.getId(), new HashSet<Pair<Integer, Integer>>());
        }

        // Track how many zones actually have partitions (and so replica types)
        // in them.
        int zonesWithPartitions = 0;
        for(Integer zoneId: cluster.getZoneIds()) {
            if(cluster.getNumberOfPartitionsInZone(zoneId) > 0) {
                zonesWithPartitions++;
            }
        }

        // Loops through all nodes
        for(Node node: cluster.getNodes()) {

            // Gets the partitions that this node was configured with.
            for(Integer primary: node.getPartitionIds()) {

                // Gets the list of replicating partitions.
                List<Integer> replicaPartitionList = routingStrategy.getReplicatingPartitionList(primary);

                if((replicaPartitionList.size() % zonesWithPartitions != 0)
                   || ((replicaPartitionList.size() / zonesWithPartitions) != (storeDef.getReplicationFactor() / cluster.getNumberOfZones()))) {

                    throw new VoldemortException("Number of replicas returned ("
                                                 + replicaPartitionList.size()
                                                 + ") does not make sense given the replication factor ("
                                                 + storeDef.getReplicationFactor()
                                                 + ") and that there are "
                                                 + cluster.getNumberOfZones()
                                                 + " zones of which "
                                                 + zonesWithPartitions
                                                 + " have partitions (and of which "
                                                 + (cluster.getNumberOfZones() - zonesWithPartitions)
                                                 + " are empty).");
                }

                int replicaType = 0;
                if(!includePrimary) {
                    replicaPartitionList.remove(primary);
                    replicaType = 1;
                }

                // Get the node that this replicating partition belongs to.
                for(Integer replicaPartition: replicaPartitionList) {
                    Integer replicaNodeId = partitionToNodeIdMap.get(replicaPartition);

                    // The replicating node will have a copy of primary.
                    nodeIdToReplicas.get(replicaNodeId).add(Pair.create(replicaType, primary));

                    replicaType++;
                }
            }
        }
        return nodeIdToReplicas;
    }

    /**
     * Print log to the following logger ( Info level )
     * 
     * @param taskId Task id
     * @param logger Logger class
     * @param message The message to print
     */
    public static void printLog(int taskId, Logger logger, String message) {
        logger.info("Task id [" + Integer.toString(taskId) + "] " + message);
    }

    /**
     * Print log to the following logger ( Error level )
     * 
     * @param taskId Stealer node id
     * @param logger Logger class
     * @param message The message to print
     */
    public static void printErrorLog(int taskId, Logger logger, String message, Exception e) {
        if(e == null) {
            logger.error("Task id " + Integer.toString(taskId) + "] " + message);
        } else {
            logger.error("Task id " + Integer.toString(taskId) + "] " + message, e);
        }
    }

    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig,
                                                    Cluster cluster,
                                                    int numConnPerNode) {
        AdminClientConfig config = new AdminClientConfig().setMaxConnectionsPerNode(numConnPerNode)
                                                          .setAdminConnectionTimeoutSec(voldemortConfig.getAdminConnectionTimeout())
                                                          .setAdminSocketTimeoutSec(voldemortConfig.getAdminSocketTimeout())
                                                          .setAdminSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new AdminClient(cluster, config, new ClientConfig());
    }

    /**
     * Given the cluster metadata and admin client, retrieves the list of store
     * definitions.
     * 
     * <br>
     * 
     * It also checks if the store definitions are consistent across the cluster
     * 
     * @param cluster The cluster metadata
     * @param adminClient The admin client to use to retrieve the store
     *        definitions
     * @return List of store definitions
     */
    public static List<StoreDefinition> getCurrentStoreDefinitions(Cluster cluster,
                                                                   AdminClient adminClient) {
        List<StoreDefinition> storeDefs = null;
        for(Node node: cluster.getNodes()) {
            List<StoreDefinition> storeDefList = adminClient.metadataMgmtOps.getRemoteStoreDefList(node.getId())
                                                                            .getValue();
            if(storeDefs == null) {
                storeDefs = storeDefList;
            } else {

                // Compare against the previous store definitions
                if(!Utils.compareList(storeDefs, storeDefList)) {
                    throw new VoldemortException("Store definitions on node " + node.getId()
                                                 + " does not match those on other nodes");
                }
            }
        }

        if(storeDefs == null) {
            throw new VoldemortException("Could not retrieve list of store definitions correctly");
        } else {
            return storeDefs;
        }
    }

    /**
     * Given a list of store definitions, makes sure that rebalance supports all
     * of them. If not it throws an error.
     * 
     * @param storeDefList List of store definitions
     * @return Filtered list of store definitions which rebalancing supports
     */
    public static List<StoreDefinition> validateRebalanceStore(List<StoreDefinition> storeDefList) {
        List<StoreDefinition> returnList = new ArrayList<StoreDefinition>(storeDefList.size());

        for(StoreDefinition def: storeDefList) {
            if(!def.isView() && !canRebalanceList.contains(def.getType())) {
                throw new VoldemortException("Rebalance does not support rebalancing of stores of type "
                                             + def.getType() + " - " + def.getName());
            } else if(!def.isView()) {
                returnList.add(def);
            } else {
                logger.debug("Ignoring view " + def.getName() + " for rebalancing");
            }
        }
        return returnList;
    }

    /**
     * Given a list of store definitions, cluster and admin client returns a
     * boolean indicating if all RO stores are in the correct format.
     * 
     * <br>
     * 
     * This function also takes into consideration nodes which are being
     * bootstrapped for the first time, in which case we can safely ignore
     * checking them ( as they will have default to ro0 )
     * 
     * @param cluster Cluster metadata
     * @param storeDefs Complete list of store definitions
     * @param adminClient Admin client
     */
    public static void validateReadOnlyStores(Cluster cluster,
                                              List<StoreDefinition> storeDefs,
                                              AdminClient adminClient) {
        List<StoreDefinition> readOnlyStores = StoreDefinitionUtils.filterStores(storeDefs, true);

        if(readOnlyStores.size() == 0) {
            // No read-only stores
            return;
        }

        List<String> storeNames = StoreDefinitionUtils.getStoreNames(readOnlyStores);
        for(Node node: cluster.getNodes()) {
            if(node.getNumberOfPartitions() != 0) {
                for(Entry<String, String> storeToStorageFormat: adminClient.readonlyOps.getROStorageFormat(node.getId(),
                                                                                                           storeNames)
                                                                                       .entrySet()) {
                    if(storeToStorageFormat.getValue()
                                           .compareTo(ReadOnlyStorageFormat.READONLY_V2.getCode()) != 0) {
                        throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                                + node.getId() + " has store "
                                                                + storeToStorageFormat.getKey()
                                                                + " not using format "
                                                                + ReadOnlyStorageFormat.READONLY_V2);
                    }
                }
            }
        }
    }

    /**
     * Returns a string representation of the cluster
     * 
     * <pre>
     * Current Cluster:
     * 0 - [0, 1, 2, 3] + [7, 8, 9]
     * 1 - [4, 5, 6] + [0, 1, 2, 3]
     * 2 - [7, 8, 9] + [4, 5, 6]
     * </pre>
     * 
     * @param nodeIdToAllPartitions Mapping of node id to all tuples
     * @return Returns a string representation of the cluster
     */
    public static String printMap(final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToAllPartitions) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Integer, Set<Pair<Integer, Integer>>> entry: nodeIdToAllPartitions.entrySet()) {
            final Integer nodeId = entry.getKey();
            final Set<Pair<Integer, Integer>> allPartitions = entry.getValue();

            final HashMap<Integer, List<Integer>> replicaTypeToPartitions = flattenPartitionTuples(allPartitions);

            // Put into sorted key order such that primary replicas occur before
            // secondary replicas and so on...
            final TreeMap<Integer, List<Integer>> sortedReplicaTypeToPartitions = new TreeMap<Integer, List<Integer>>(replicaTypeToPartitions);

            sb.append(nodeId);
            if(replicaTypeToPartitions.size() > 0) {
                for(Entry<Integer, List<Integer>> partitions: sortedReplicaTypeToPartitions.entrySet()) {
                    Collections.sort(partitions.getValue());
                    sb.append(" - " + partitions.getValue());
                }
            } else {
                sb.append(" - empty");
            }
            sb.append(Utils.NEWLINE);
        }
        return sb.toString();
    }

    /**
     * Given the initial and final cluster dumps it into the output directory
     * 
     * @param currentCluster Initial cluster metadata
     * @param finalCluster Final cluster metadata
     * @param outputDir Output directory where to dump this file
     * @param filePrefix String to prepend to the initial & final cluster
     *        metadata files
     * @throws IOException
     */
    public static void dumpClusters(Cluster currentCluster,
                                    Cluster finalCluster,
                                    String outputDirName,
                                    String filePrefix) {
        dumpClusterToFile(outputDirName, filePrefix + currentClusterFileName, currentCluster);
        dumpClusterToFile(outputDirName, filePrefix + finalClusterFileName, finalCluster);
    }

    /**
     * Given the initial and final cluster dumps it into the output directory
     * 
     * @param initialCluster Initial cluster metadata
     * @param finalCluster Final cluster metadata
     * @param outputDir Output directory where to dump this file
     * @throws IOException
     */
    public static void dumpClusters(Cluster initialCluster,
                                    Cluster finalCluster,
                                    String outputDirName) {
        dumpClusters(initialCluster, finalCluster, outputDirName, "");
    }

    /**
     * Prints a cluster xml to a file.
     * 
     * @param outputDirName
     * @param fileName
     * @param cluster
     */
    public static void dumpClusterToFile(String outputDirName, String fileName, Cluster cluster) {

        if(outputDirName != null) {
            File outputDir = new File(outputDirName);
            if(!outputDir.exists()) {
                Utils.mkdirs(outputDir);
            }

            try {
                FileUtils.writeStringToFile(new File(outputDirName, fileName),
                                            new ClusterMapper().writeCluster(cluster));
            } catch(IOException e) {
                logger.error("IOException during dumpClusterToFile: " + e);
            }
        }
    }

    /**
     * Prints a balance analysis to a file.
     * 
     * @param outputDirName
     * @param baseFileName suffix '.analysis' is appended to baseFileName.
     * @param partitionBalance
     */
    public static void dumpAnalysisToFile(String outputDirName,
                                          String baseFileName,
                                          PartitionBalance partitionBalance) {
        if(outputDirName != null) {
            File outputDir = new File(outputDirName);
            if(!outputDir.exists()) {
                Utils.mkdirs(outputDir);
            }

            try {
                FileUtils.writeStringToFile(new File(outputDirName, baseFileName + ".analysis"),
                                            partitionBalance.toString());
            } catch(IOException e) {
                logger.error("IOException during dumpAnalysisToFile: " + e);
            }
        }
    }

    /**
     * Given a list of tuples of [replica_type, partition], flattens it and
     * generates a map of replica_type to partition mapping
     * 
     * @param partitionTuples Set of <replica_type, partition> tuples
     * @return Map of replica_type to set of partitions
     */
    public static HashMap<Integer, List<Integer>> flattenPartitionTuples(Set<Pair<Integer, Integer>> partitionTuples) {
        HashMap<Integer, List<Integer>> flattenedTuples = Maps.newHashMap();
        for(Pair<Integer, Integer> pair: partitionTuples) {
            if(flattenedTuples.containsKey(pair.getFirst())) {
                flattenedTuples.get(pair.getFirst()).add(pair.getSecond());
            } else {
                List<Integer> newPartitions = Lists.newArrayList();
                newPartitions.add(pair.getSecond());
                flattenedTuples.put(pair.getFirst(), newPartitions);
            }
        }
        return flattenedTuples;
    }

    /**
     * Given a map of replica_type to partition mapping gives back a set of
     * tuples of [replica_type, partition]
     * 
     * @param replicaToPartitionList Map of replica_type to set of partitions
     * @return Set of <replica_type, partition> tuples
     */
    public static Set<Pair<Integer, Integer>> flattenPartitionTuples(HashMap<Integer, List<Integer>> replicaToPartitionList) {
        Set<Pair<Integer, Integer>> partitionTuples = Sets.newHashSet();

        for(Entry<Integer, List<Integer>> entry: replicaToPartitionList.entrySet()) {
            for(Iterator<Integer> iter = entry.getValue().iterator(); iter.hasNext();) {
                partitionTuples.add(new Pair<Integer, Integer>(entry.getKey(), iter.next()));
            }
        }

        return partitionTuples;
    }

    /**
     * Given a list of node plans flattens it into a list of partitions info
     * 
     * @param rebalanceNodePlanList Complete list of rebalance node plan
     * @return Flattened list of partition plans
     */
    public static List<RebalancePartitionsInfo> flattenNodePlans(List<RebalanceNodePlan> rebalanceNodePlanList) {
        List<RebalancePartitionsInfo> list = new ArrayList<RebalancePartitionsInfo>();
        for(RebalanceNodePlan rebalanceNodePlan: rebalanceNodePlanList) {
            for(final RebalancePartitionsInfo stealInfo: rebalanceNodePlan.getRebalanceTaskList()) {
                list.add(stealInfo);
            }
        }
        return list;
    }

    /**
     * Given a set of [ replica, partition ] tuples, flatten it to retrieve only
     * the partitions
     * 
     * @param tuples The [ replica, partition ] tuples
     * @return List of partitions
     */
    public static List<Integer> getPartitionsFromTuples(Set<Pair<Integer, Integer>> tuples) {
        List<Integer> partitions = Lists.newArrayList();

        if(tuples != null) {
            for(Pair<Integer, Integer> tuple: tuples) {
                partitions.add(tuple.getSecond());
            }
        }
        return partitions;
    }

    /**
     * Given a list of partition plans and a set of stores, copies the store
     * names to every individual plan and creates a new list
     * 
     * @param existingPlanList Existing partition plan list
     * @param storeDefs List of store names we are rebalancing
     * @return List of updated partition plan
     */
    public static List<RebalancePartitionsInfo> filterPartitionPlanWithStores(List<RebalancePartitionsInfo> existingPlanList,
                                                                              List<StoreDefinition> storeDefs) {
        List<RebalancePartitionsInfo> plans = Lists.newArrayList();
        List<String> storeNames = StoreDefinitionUtils.getStoreNames(storeDefs);

        for(RebalancePartitionsInfo existingPlan: existingPlanList) {
            RebalancePartitionsInfo info = RebalancePartitionsInfo.create(existingPlan.toJsonString());

            // Filter the plans only for stores given
            HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartitions = info.getStoreToReplicaToAddPartitionList();

            HashMap<String, HashMap<Integer, List<Integer>>> newStoreToReplicaToAddPartitions = Maps.newHashMap();
            for(String storeName: storeNames) {
                if(storeToReplicaToAddPartitions.containsKey(storeName))
                    newStoreToReplicaToAddPartitions.put(storeName,
                                                         storeToReplicaToAddPartitions.get(storeName));
            }
            info.setStoreToReplicaToAddPartitionList(newStoreToReplicaToAddPartitions);

            plans.add(info);
        }

        return plans;
    }

    /**
     * Given a list of partition infos, generates a map of stealer / donor node
     * to list of partition infos
     * 
     * @param rebalancePartitionPlanList Complete list of partition plans
     * @param groupByStealerNode Boolean indicating if we want to group by
     *        stealer node ( or donor node )
     * @return Flattens it into a map on a per node basis
     */
    public static HashMap<Integer, List<RebalancePartitionsInfo>> groupPartitionsInfoByNode(List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                                                                            boolean groupByStealerNode) {
        HashMap<Integer, List<RebalancePartitionsInfo>> nodeToPartitionsInfo = Maps.newHashMap();
        if(rebalancePartitionPlanList != null) {
            for(RebalancePartitionsInfo partitionInfo: rebalancePartitionPlanList) {
                int nodeId = groupByStealerNode ? partitionInfo.getStealerId()
                                               : partitionInfo.getDonorId();
                List<RebalancePartitionsInfo> partitionInfos = nodeToPartitionsInfo.get(nodeId);
                if(partitionInfos == null) {
                    partitionInfos = Lists.newArrayList();
                    nodeToPartitionsInfo.put(nodeId, partitionInfos);
                }
                partitionInfos.add(partitionInfo);
            }
        }
        return nodeToPartitionsInfo;
    }

    /**
     * Wait to shutdown service
     * 
     * @param executorService Executor service to shutdown
     * @param timeOutSec Time we wait for
     */
    public static void executorShutDown(ExecutorService executorService, long timeOutSec) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(timeOutSec, TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service.", e);
        }
    }

    public static ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    /**
     * Compares current cluster with target cluster. Uses pertinent store defs
     * for each cluster to determine if a node that hosts a zone-primary in the
     * current cluster will no longer host any zone-nary in the target cluster.
     * This check is the precondition for a server returning an invalid metadata
     * exception to a client on a normal-case put or get. Normal-case being that
     * the zone-primary receives the pseudo-master put or the get operation.
     * 
     * @param currentCluster
     * @param currentStoreDefs
     * @param targetCluster
     * @param targetStoreDefs
     * @return pretty-printed string documenting invalid metadata rates for each
     *         zone.
     */
    public static String analyzeInvalidMetadataRate(final Cluster currentCluster,
                                                    List<StoreDefinition> currentStoreDefs,
                                                    final Cluster targetCluster,
                                                    List<StoreDefinition> targetStoreDefs) {
        StringBuilder sb = new StringBuilder();
        sb.append("Dump of invalid metadata rates per zone").append(Utils.NEWLINE);

        HashMap<StoreDefinition, Integer> uniqueStores = KeyDistributionGenerator.getUniqueStoreDefinitionsWithCounts(currentStoreDefs);

        for(StoreDefinition currentStoreDef: uniqueStores.keySet()) {
            sb.append("Store exemplar: " + currentStoreDef.getName())
              .append(Utils.NEWLINE)
              .append("\tThere are " + uniqueStores.get(currentStoreDef) + " other similar stores.")
              .append(Utils.NEWLINE);

            StoreRoutingPlan currentSRP = new StoreRoutingPlan(currentCluster, currentStoreDef);
            StoreDefinition targetStoreDef = StoreUtils.getStoreDef(targetStoreDefs,
                                                                    currentStoreDef.getName());
            StoreRoutingPlan targetSRP = new StoreRoutingPlan(targetCluster, targetStoreDef);

            // Only care about existing zones
            for(int zoneId: currentCluster.getZoneIds()) {
                int zoneLocalPrimaries = 0;
                int invalidMetadata = 0;
                // Examine nodes in current cluster in existing zone.
                for(int nodeId: currentCluster.getNodeIdsInZone(zoneId)) {
                    for(int partitionId: targetSRP.getZonePrimaryPartitionIds(nodeId)) {
                        zoneLocalPrimaries++;
                        if(!currentSRP.getNaryPartitionIds(nodeId).contains(partitionId)) {
                            invalidMetadata++;
                        }
                    }
                }
                float rate = invalidMetadata / (float) zoneLocalPrimaries;
                sb.append("\tZone " + zoneId)
                  .append(" : total zone primaries " + zoneLocalPrimaries)
                  .append(", # that trigger invalid metadata " + invalidMetadata)
                  .append(" => " + rate)
                  .append(Utils.NEWLINE);
            }
        }

        return sb.toString();
    }

}
