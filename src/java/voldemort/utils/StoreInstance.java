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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;

import com.google.common.collect.Lists;

// TODO: Add StoreInstanceTest unit test for these helper methods.

/**
 * This class wraps up a Cluster object and a StoreDefinition. The methods are
 * effectively helper or util style methods for analyzing partitions and so on
 * which are a function of both Cluster and StoreDefinition.
 */
public class StoreInstance {

    // TODO: (refactor) Improve upon the name "StoreInstance". Object-oriented
    // meaning of 'instance' is too easily confused with system notion of an
    // "instance of a cluster" (the intended usage in this class name).

    private final Cluster cluster;
    private final StoreDefinition storeDefinition;

    private final Map<Integer, Integer> partitionIdToNodeIdMap;

    public StoreInstance(Cluster cluster, StoreDefinition storeDefinition) {
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;

        partitionIdToNodeIdMap = ClusterUtils.getCurrentPartitionMapping(cluster);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public StoreDefinition getStoreDefinition() {
        return storeDefinition;
    }

    /**
     * Determines list of partition IDs that replicate the master partition ID.
     * 
     * @param masterPartitionId
     * @return List of partition IDs that replicate the master partition ID.
     */
    public List<Integer> getReplicationPartitionList(int masterPartitionId) {
        return new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster)
                                           .getReplicatingPartitionList(masterPartitionId);
    }

    /**
     * Determines list of partition IDs that replicate the key.
     * 
     * @param key
     * @return List of partition IDs that replicate the partition ID.
     */
    public List<Integer> getReplicationPartitionList(final byte[] key) {
        return getReplicationPartitionList(getMasterPartitionId(key));
    }

    /**
     * Determines master partition ID for the key.
     * 
     * @param key
     * @return
     */
    public int getMasterPartitionId(final byte[] key) {
        return new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster)
                                           .getMasterPartition(key);
    }

    /**
     * Determines node ID that hosts the specified partition ID.
     * 
     * @param partitionId
     * @return
     */
    public int getNodeIdForPartitionId(int partitionId) {
        return partitionIdToNodeIdMap.get(partitionId);
    }

    /**
     * Determines the partition ID that replicates the key on the given node.
     * 
     * @param nodeId of the node
     * @param key to look up.
     * @return partitionId if found, otherwise null.
     */
    public Integer getNodesPartitionIdForKey(int nodeId, final byte[] key) {
        List<Integer> partitionIds = getReplicationPartitionList(key);
        for(Integer partitionId: partitionIds) {
            if(getNodeIdForPartitionId(partitionId) == nodeId) {
                return partitionId;
            }
        }
        return null;
    }

    /**
     * Converts from partitionId to nodeId. The list of partition IDs,
     * partitionIds, is expected to be a "replicating partition list", i.e., the
     * mapping from partition ID to node ID should be one to one.
     * 
     * @param partitionIds List of partition IDs for which to find the Node ID
     *        for the Node that owns the partition.
     * @return List of node ids, one for each partition ID in partitionIds
     * @throws VoldemortException If multiple partition IDs in partitionIds map
     *         to the same Node ID.
     */
    private List<Integer> getNodeIdListForPartitionIdList(List<Integer> partitionIds)
            throws VoldemortException {
        List<Integer> nodeIds = new ArrayList<Integer>(partitionIds.size());
        for(Integer partitionId: partitionIds) {
            int nodeId = getNodeIdForPartitionId(partitionId);
            if(nodeIds.contains(nodeId)) {
                throw new VoldemortException("Node ID " + nodeId + " already in list of Node IDs.");
            } else {
                nodeIds.add(nodeId);
            }
        }
        return nodeIds;
    }

    public List<Integer> getReplicationNodeList(int partitionId) throws VoldemortException {
        return getNodeIdListForPartitionIdList(getReplicationPartitionList(partitionId));
    }

    // TODO: (refactor) Move from static methods to non-static methods that use
    // this object's cluster and storeDefinition member for the various
    // check*BelongsTo* methods.

    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param keyPartitions Preference list of the key
     * @param nodePartitions Partition list on this node
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(List<Integer> keyPartitions,
                                                     List<Integer> nodePartitions,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList) {
        // Check for null
        replicaToPartitionList = Utils.notNull(replicaToPartitionList);

        for(int replicaNum = 0; replicaNum < keyPartitions.size(); replicaNum++) {

            // If this partition belongs to node partitions + master is in
            // replicaToPartitions list -> match
            if(nodePartitions.contains(keyPartitions.get(replicaNum))) {
                List<Integer> partitionsToMove = replicaToPartitionList.get(replicaNum);
                if(partitionsToMove != null && partitionsToMove.size() > 0) {
                    if(partitionsToMove.contains(keyPartitions.get(0))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param nodeId Node on which this is running ( generally stealer node )
     * @param key The key to check
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @param cluster Cluster metadata
     * @param storeDef The store definition
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(int nodeId,
                                                     byte[] key,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList,
                                                     Cluster cluster,
                                                     StoreDefinition storeDef) {
        boolean checkResult = false;
        if(storeDef.getRoutingStrategyType().equals(RoutingStrategyType.TO_ALL_STRATEGY)
           || storeDef.getRoutingStrategyType()
                      .equals(RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY)) {
            checkResult = true;
        } else {
            List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                             cluster)
                                                                      .getPartitionList(key);
            List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
            checkResult = StoreInstance.checkKeyBelongsToPartition(keyPartitions,
                                                                   nodePartitions,
                                                                   replicaToPartitionList);
        }
        return checkResult;
    }

    /***
     * 
     * @return true if the partition belongs to the node with given replicatype
     */
    public static boolean checkPartitionBelongsToNode(int partitionId,
                                                      int replicaType,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        boolean belongs = false;
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partitionId);
        // validate replicaType
        if(replicaType < replicatingPartitions.size()) {
            // check if the replicaType'th partition in the replicating list,
            // belongs to the given node
            if(nodePartitions.contains(replicatingPartitions.get(replicaType)))
                belongs = true;
        }

        return belongs;
    }

    /**
     * Given a key and a list of steal infos give back a list of stealer node
     * ids which will steal this.
     * 
     * @param key Byte array of key
     * @param stealerNodeToMappingTuples Pairs of stealer node id to their
     *        corresponding [ partition - replica ] tuples
     * @param cluster Cluster metadata
     * @param storeDef Store definitions
     * @return List of node ids
     */
    public static List<Integer> checkKeyBelongsToPartition(byte[] key,
                                                           Set<Pair<Integer, HashMap<Integer, List<Integer>>>> stealerNodeToMappingTuples,
                                                           Cluster cluster,
                                                           StoreDefinition storeDef) {
        List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                         cluster)
                                                                  .getPartitionList(key);
        List<Integer> nodesToPush = Lists.newArrayList();
        for(Pair<Integer, HashMap<Integer, List<Integer>>> stealNodeToMap: stealerNodeToMappingTuples) {
            List<Integer> nodePartitions = cluster.getNodeById(stealNodeToMap.getFirst())
                                                  .getPartitionIds();
            if(StoreInstance.checkKeyBelongsToPartition(keyPartitions,
                                                        nodePartitions,
                                                        stealNodeToMap.getSecond())) {
                nodesToPush.add(stealNodeToMap.getFirst());
            }
        }
        return nodesToPush;
    }

    /***
     * Checks if a given partition is stored in the node. (It can be primary or
     * a secondary)
     * 
     * @param partition
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return
     */
    public static boolean checkPartitionBelongsToNode(int partition,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partition);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    /**
     * 
     * @param key
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return true if the key belongs to the node as some replica
     */
    public static boolean checkKeyBelongsToNode(byte[] key,
                                                int nodeId,
                                                Cluster cluster,
                                                StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getPartitionList(key);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

}
