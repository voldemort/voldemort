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

package voldemort.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * RebalanceUtils provide basic functionality for rebalancing.
 * 
 */
public class RebalanceUtils {

    private static Logger logger = Logger.getLogger(RebalanceUtils.class);

    public final static List<String> rebalancingStoreEngineBlackList = Arrays.asList(MysqlStorageConfiguration.TYPE_NAME,
                                                                                     ViewStorageConfiguration.TYPE_NAME);

    /**
     * Given a cluster and a node id checks if the node exists
     * 
     * @param cluster The cluster metadata to check in
     * @param nodeId The node id to search for
     * @return True if cluster contains the node id, else false
     */
    public static boolean containsNode(Cluster cluster, int nodeId) {
        try {
            cluster.getNodeById(nodeId);
            return true;
        } catch(VoldemortException e) {
            return false;
        }
    }

    /**
     * Updates the existing cluster such that we remove partitions mentioned
     * from the stealer node and add them to the donor node
     * 
     * @param cluster Existing cluster metadata
     * @param stealerNode Node from which we are stealing the partitions
     * @param donorNode Node to which we are donating
     * @param partitionList List of partitions we are moving
     * @return Updated cluster metadata
     */
    public static Cluster createUpdatedCluster(Cluster cluster,
                                               Node stealerNode,
                                               Node donorNode,
                                               List<Integer> partitionList) {
        List<Integer> stealerPartitionList = new ArrayList<Integer>(stealerNode.getPartitionIds());
        List<Integer> donorPartitionList = new ArrayList<Integer>(donorNode.getPartitionIds());

        for(int p: cluster.getNodeById(stealerNode.getId()).getPartitionIds()) {
            if(!stealerPartitionList.contains(p))
                stealerPartitionList.add(p);
        }

        for(int partition: partitionList) {
            for(int i = 0; i < donorPartitionList.size(); i++) {
                if(partition == donorPartitionList.get(i)) {
                    donorPartitionList.remove(i);
                }
            }
            if(!stealerPartitionList.contains(partition))
                stealerPartitionList.add(partition);
        }

        // sort both list
        Collections.sort(stealerPartitionList);
        Collections.sort(donorPartitionList);

        // update both nodes
        stealerNode = updateNode(stealerNode, stealerPartitionList);
        donorNode = updateNode(donorNode, donorPartitionList);

        Cluster updatedCluster = updateCluster(cluster, Arrays.asList(stealerNode, donorNode));
        return updatedCluster;
    }

    /**
     * Creates a new cluster by adding a donated partition to a new or existing
     * node.
     * 
     * @param currentCluster current cluster used to copy from.
     * @param stealerNode now or existing node being updated.
     * @param donatedPartition partition donated to the <code>stealerNode</code>
     */
    public static Cluster createUpdatedCluster(final Cluster currentCluster,
                                               Node stealerNode,
                                               final int donatedPartition) {
        // Gets the donor Node that owns this donated partition
        Node donorNode = RebalanceUtils.getNodeByPartitionId(currentCluster, donatedPartition);

        // Removes the node from the list,
        final List<Node> nodes = new ArrayList<Node>(currentCluster.getNodes());
        nodes.remove(donorNode);
        nodes.remove(stealerNode);

        // Update the list of partitions for this node
        donorNode = RebalanceUtils.removePartitionToNode(donorNode, donatedPartition);
        stealerNode = RebalanceUtils.addPartitionToNode(stealerNode, donatedPartition);

        // Add the updated nodes (donor and stealer).
        nodes.add(donorNode);
        nodes.add(stealerNode);

        // After the stealer & donor were fixed recreate the cluster.
        // Sort the nodes so they will appear in the same order all the time.
        Collections.sort(nodes);
        return new Cluster(currentCluster.getName(),
                           nodes,
                           Lists.newArrayList(currentCluster.getZones()));
    }

    /**
     * Concatenates the list of current nodes in the given cluster with the new
     * nodes provided and returns an updated cluster metadata
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
        return RebalanceUtils.updateNode(node, deepCopy);
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
        return RebalanceUtils.updateNode(node, deepCopy);
    }

    /**
     * Given the cluster metadata returns a mapping of partition to node
     * 
     * @param currentCluster Cluster metadata
     * @return Map of partition id to node id
     */
    public static Map<Integer, Integer> getCurrentPartitionMapping(Cluster currentCluster) {

        Map<Integer, Integer> partitionToNode = new LinkedHashMap<Integer, Integer>();

        for(Node n: currentCluster.getNodes()) {
            for(Integer partition: n.getPartitionIds()) {
                // Check if partition is on another node
                Integer previousRegisteredNodeId = partitionToNode.get(partition);
                if(previousRegisteredNodeId != null) {
                    throw new IllegalArgumentException("Partition id " + partition
                                                       + " found on two nodes : " + n.getId()
                                                       + " and " + previousRegisteredNodeId);
                }

                partitionToNode.put(partition, n.getId());
            }
        }

        return partitionToNode;
    }

    /**
     * Get the latest cluster from all available nodes in the cluster<br>
     * Throws exception if:<br>
     * any node in the RequiredNode list fails to respond.<br>
     * Cluster is in inconsistent state with concurrent versions for cluster
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
                Versioned<Cluster> versionedCluster = adminClient.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();
                if(null != newClock && !clusterList.contains(versionedCluster)) {
                    // check no two clocks are concurrent.
                    checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occured occured = newClock.compare(latestCluster.getVersion());
                    if(Occured.AFTER.equals(occured))
                        latestCluster = versionedCluster;
                }
            } catch(Exception e) {
                if(null != requiredNodes && requiredNodes.contains(node.getId()))
                    throw new VoldemortException("Failed to get Cluster version from node:" + node,
                                                 e);
                else
                    logger.info("Failed to get Cluster version from node:" + node, e);
            }
        }

        return latestCluster;
    }

    public static void checkNotConcurrent(ArrayList<Versioned<Cluster>> clockList,
                                          VectorClock newClock) {
        for(Versioned<Cluster> versionedCluster: clockList) {
            VectorClock clock = (VectorClock) versionedCluster.getVersion();
            if(Occured.CONCURRENTLY.equals(clock.compare(newClock)))
                throw new VoldemortException("Cluster is in inconsistent state got conflicting clocks "
                                             + clock + " and " + newClock);

        }
    }

    /**
     * Attempt to propagate cluster definition to all nodes in the cluster.
     * 
     * @throws VoldemortException If we can't propagate to a list of require
     *         nodes.
     * @param adminClient {@link voldemort.client.protocol.admin.AdminClient}
     *        instance to use
     * @param cluster Cluster definition we wish to propagate
     * @param clock Vector clock to attach to the cluster definition
     * @param requireNodeIds If we can't propagate to these node ids, roll back
     *        and throw an exception
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster cluster,
                                        VectorClock clock,
                                        List<Integer> requireNodeIds) {
        List<Integer> allNodeIds = new ArrayList<Integer>();
        for(Node node: cluster.getNodes()) {
            allNodeIds.add(node.getId());
        }
        propagateCluster(adminClient, cluster, clock, allNodeIds, requireNodeIds);
    }

    /**
     * Attempt to propagate a cluster definition to specified nodes.
     * 
     * @throws VoldemortException If we can't propagate to a list of require
     *         nodes.
     * @param adminClient {@link voldemort.client.protocol.admin.AdminClient}
     *        instance to use.
     * @param cluster Cluster definition we wish to propagate
     * @param clock Vector clock to attach to the cluster definition
     * @param attemptNodeIds Attempt to propagate to these node ids
     * @param requiredNodeIds If we can't propagate can't propagate to these
     *        node ids, roll back and throw an exception
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster cluster,
                                        VectorClock clock,
                                        List<Integer> attemptNodeIds,
                                        List<Integer> requiredNodeIds) {
        List<Integer> failures = new ArrayList<Integer>();

        // copy everywhere else first
        for(int nodeId: attemptNodeIds) {
            if(!requiredNodeIds.contains(nodeId)) {
                try {
                    adminClient.updateRemoteCluster(nodeId, cluster, clock);
                } catch(VoldemortException e) {
                    // ignore these
                    logger.debug("Failed to copy new cluster.xml(" + cluster
                                 + ") on non-required node:" + nodeId, e);
                }
            }
        }

        // attempt copying on all required nodes.
        for(int nodeId: requiredNodeIds) {
            Node node = cluster.getNodeById(nodeId);
            try {
                logger.debug("Updating remote node:" + nodeId + " with cluster:" + cluster);
                adminClient.updateRemoteCluster(node.getId(), cluster, clock);
            } catch(Exception e) {
                failures.add(node.getId());
                logger.debug(e);
            }
        }

        if(failures.size() > 0) {
            throw new VoldemortException("Failed to copy updated cluster.xml:" + cluster
                                         + " on required nodes:" + failures);
        }
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
            if(!RebalanceUtils.containsNode(currentCluster, node.getId())) {
                newNodes.add(RebalanceUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        return RebalanceUtils.updateCluster(currentCluster, newNodes);
    }

    /**
     * For a particular stealer node find all the "primary" partitions it will
     * steal
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param targetCluster The target cluster definition
     * @param stealerNode The stealer node
     * @return Returns a list of partitions which this stealer node will get
     */
    public static Set<Integer> getStolenPrimaries(final Cluster currentCluster,
                                                  final Cluster targetCluster,
                                                  final Node stealerNode) {
        int stealNodeId = stealerNode.getId();
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return new TreeSet<Integer>(targetList);
    }

    /**
     * Returns the Node associated to the provided partition.
     * 
     * @param cluster The cluster in which to find the node
     * @param partitionId Partition id for which we want the corresponding node
     * @return Node that owns the partition
     */
    public static Node getNodeByPartitionId(Cluster cluster, int partitionId) {
        for(Node node: cluster.getNodes()) {
            if(node.getPartitionIds().contains(partitionId)) {
                return node;
            }
        }
        return null;
    }

    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig,
                                                    Cluster cluster,
                                                    int numThreads,
                                                    int numConnPerNode) {
        AdminClientConfig config = new AdminClientConfig().setMaxConnectionsPerNode(numConnPerNode)
                                                          .setMaxThreads(numThreads)
                                                          .setAdminConnectionTimeoutSec(voldemortConfig.getAdminConnectionTimeout())
                                                          .setAdminSocketTimeoutSec(voldemortConfig.getAdminSocketTimeout())
                                                          .setAdminSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new AdminClient(cluster, config);
    }

    public static List<StoreDefinition> getStoreNameList(Cluster cluster, AdminClient adminClient) {
        for(Node node: cluster.getNodes()) {
            try {
                List<StoreDefinition> storeDefList = adminClient.getRemoteStoreDefList(node.getId())
                                                                .getValue();
                return getWritableStores(storeDefList);
            } catch(VoldemortException e) {
                logger.warn(e);
            }
        }

        throw new VoldemortException("Unable to get StoreDefList from any node for cluster:"
                                     + cluster);
    }

    public static List<StoreDefinition> getWritableStores(List<StoreDefinition> storeDefList) {
        List<StoreDefinition> storeNameList = new ArrayList<StoreDefinition>(storeDefList.size());

        for(StoreDefinition def: storeDefList) {
            if(!def.isView() && !rebalancingStoreEngineBlackList.contains(def.getType())) {
                storeNameList.add(def);
            } else {
                logger.debug("ignoring store " + def.getName() + " for rebalancing");
            }
        }
        return storeNameList;
    }

    public static StoreDefinition getMaxReplicationStore(List<StoreDefinition> storeDefList) {
        int maxReplication = 0;
        StoreDefinition maxStore = null;
        for(StoreDefinition def: storeDefList) {
            if(maxReplication < def.getReplicationFactor()) {
                maxReplication = def.getReplicationFactor();
                maxStore = def;
            }
        }

        return maxStore;
    }

    /**
     * Given a list of store definitions return a list of store names
     * 
     * @param storeDefList The list of store definitions
     * @return Returns a list of store names
     */
    public static List<String> getStoreNames(List<StoreDefinition> storeDefList) {
        List<String> storeList = new ArrayList<String>(storeDefList.size());
        for(StoreDefinition def: storeDefList) {
            storeList.add(def.getName());
        }
        return storeList;
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

    public static void executorShutDown(ExecutorService executorService, int timeOutSec) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(timeOutSec, TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service.", e);
        }
    }

    public static ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }
}
