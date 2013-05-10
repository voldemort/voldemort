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

package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.utils.MoveMap;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

// TODO: (refactor) Rename RebalanceClusterPlan to RebalanceBatchPlan
// TODO: (refactor) Fix cluster nomenclature in general: make sure there are
// exactly three prefixes used to distinguish cluster xml: initial or current,
// target or spec or expanded, and final. 'target' has historically been
// overloaded to mean spec/expanded or final depending on context.
// TODO: (refactor) Fix this header comment after all the refactoring...
/**
 * Compares the target cluster configuration with the final cluster
 * configuration and generates a plan to move the partitions. The plan can be
 * either generated from the perspective of the stealer or the donor. <br>
 * 
 * The end result is one of the following -
 * 
 * <li>A map of stealer node-ids to partitions desired to be stolen from various
 * donor nodes
 * 
 * <li>A map of donor node-ids to partitions which we need to donate to various
 * stealer nodes
 * 
 */
public class RebalanceClusterPlan {

    private final Cluster targetCluster;
    private final Cluster finalCluster;
    private final List<StoreDefinition> storeDefs;

    protected final List<RebalancePartitionsInfo> batchPlan;

    /**
     * Compares the targetCluster configuration with the desired finalClsuter
     * and builds a map of Target node-id to map of source node-ids and
     * partitions desired to be stolen/fetched.
     * 
     * @param targetCluster The current cluster definition
     * @param finalCluster The target cluster definition
     * @param storeDefList The list of store definitions to rebalance
     * @param enabledDeletePartition Delete the RW partition on the donor side
     *        after rebalance
     * @param isStealerBased Do we want to generate the final plan based on the
     *        stealer node or the donor node?
     */
    public RebalanceClusterPlan(final Cluster targetCluster,
                                final Cluster finalCluster,
                                final List<StoreDefinition> storeDefs) {
        this.targetCluster = targetCluster;
        this.finalCluster = finalCluster;
        this.storeDefs = storeDefs;
        RebalanceUtils.validateTargetFinalCluster(targetCluster, finalCluster);
        RebalanceUtils.validateClusterStores(targetCluster, storeDefs);
        RebalanceUtils.validateClusterStores(finalCluster, storeDefs);

        this.batchPlan = batchPlan();

    }

    public Cluster getCurrentCluster() {
        return targetCluster;
    }

    public Cluster getFinalCluster() {
        return finalCluster;
    }

    public List<StoreDefinition> getStoreDefs() {
        return storeDefs;
    }

    public List<RebalancePartitionsInfo> getBatchPlan() {
        return batchPlan;
    }

    public MoveMap getZoneMoveMap() {
        MoveMap moveMap = new MoveMap(finalCluster.getZoneIds());

        for(RebalancePartitionsInfo info: batchPlan) {
            int fromZoneId = finalCluster.getNodeById(info.getDonorId()).getZoneId();
            int toZoneId = finalCluster.getNodeById(info.getStealerId()).getZoneId();
            moveMap.add(fromZoneId, toZoneId, info.getPartitionStoreMoves());
        }

        return moveMap;
    }

    public MoveMap getNodeMoveMap() {
        MoveMap moveMap = new MoveMap(finalCluster.getNodeIds());

        for(RebalancePartitionsInfo info: batchPlan) {
            moveMap.add(info.getDonorId(), info.getStealerId(), info.getPartitionStoreMoves());
        }

        return moveMap;
    }

    public int getCrossZonePartitionStoreMoves() {
        int xzonePartitionStoreMoves = 0;
        for(RebalancePartitionsInfo info: batchPlan) {
            Node donorNode = finalCluster.getNodeById(info.getDonorId());
            Node stealerNode = finalCluster.getNodeById(info.getStealerId());

            if(donorNode.getZoneId() != stealerNode.getZoneId()) {
                xzonePartitionStoreMoves += info.getPartitionStoreMoves();
            }
        }

        return xzonePartitionStoreMoves;
    }

    /**
     * Return the total number of partition-store moves
     * 
     * @return Number of moves
     */
    public int getPartitionStoreMoves() {
        int partitionStoreMoves = 0;

        for(RebalancePartitionsInfo info: batchPlan) {
            partitionStoreMoves += info.getPartitionStoreMoves();
        }

        return partitionStoreMoves;
    }

    // TODO: Simplify / kill this complicated struct once
    // RebalancePartitionsInfo is rationalized.
    private class UnnecessarilyComplicatedDataStructure {

        final HashMap<Pair<Integer, Integer>, HashMap<String, HashMap<Integer, List<Integer>>>> stealerDonorToStoreToStealPartition;

        UnnecessarilyComplicatedDataStructure() {
            stealerDonorToStoreToStealPartition = Maps.newHashMap();
        }

        public void shovelCrapIn(int stealerNodeId,
                                 int donorNodeId,
                                 String storeName,
                                 int donorReplicaType,
                                 int partitionId) {
            Pair<Integer, Integer> stealerDonor = new Pair<Integer, Integer>(stealerNodeId,
                                                                             donorNodeId);
            if(!stealerDonorToStoreToStealPartition.containsKey(stealerDonor)) {
                stealerDonorToStoreToStealPartition.put(stealerDonor,
                                                        new HashMap<String, HashMap<Integer, List<Integer>>>());
            }

            HashMap<String, HashMap<Integer, List<Integer>>> storeToStealPartition = stealerDonorToStoreToStealPartition.get(stealerDonor);
            if(!storeToStealPartition.containsKey(storeName)) {
                storeToStealPartition.put(storeName, new HashMap<Integer, List<Integer>>());
            }

            HashMap<Integer, List<Integer>> replicaTypeToPartitionIdList = storeToStealPartition.get(storeName);
            if(!replicaTypeToPartitionIdList.containsKey(donorReplicaType)) {
                replicaTypeToPartitionIdList.put(donorReplicaType, new ArrayList<Integer>());
            }

            List<Integer> partitionIdList = replicaTypeToPartitionIdList.get(donorReplicaType);
            partitionIdList.add(partitionId);
        }

        public List<RebalancePartitionsInfo> shovelCrapOut() {
            final List<RebalancePartitionsInfo> result = new ArrayList<RebalancePartitionsInfo>();

            for(Pair<Integer, Integer> stealerDonor: stealerDonorToStoreToStealPartition.keySet()) {
                result.add(new RebalancePartitionsInfo(stealerDonor.getFirst(),
                                                       stealerDonor.getSecond(),
                                                       stealerDonorToStoreToStealPartition.get(stealerDonor),
                                                       targetCluster));
            }
            return result;
        }
    }

    // TODO: Revisit these "two principles". I am not sure about the second one.
    // Either because I don't like delete being mixed with this code or because
    // we probably want to copy from a to-be-deleted partitoin-store.
    /*
     * Generate the list of partition movement based on 2 principles:
     * 
     * <ol> <li>The number of partitions don't change; they are only
     * redistributed across nodes <li>A primary or replica partition that is
     * going to be deleted is never used to copy from data from another stealer
     * </ol>
     * 
     * @param targetCluster Current cluster configuration
     * 
     * @param finalCluster Target cluster configuration
     * 
     * @param storeDefs List of store definitions
     * 
     * @param stealerNodeId Id of the stealer node
     * 
     * @param enableDeletePartition To delete or not to delete?
     */
    // TODO: more verbose javadoc based on above (possibly)
    /**
     * Determine the batch plan!
     * 
     * @return
     */
    private List<RebalancePartitionsInfo> batchPlan() {
        // Construct all store routing plans once.
        HashMap<String, StoreRoutingPlan> targetStoreRoutingPlans = new HashMap<String, StoreRoutingPlan>();
        HashMap<String, StoreRoutingPlan> finalStoreRoutingPlans = new HashMap<String, StoreRoutingPlan>();
        for(StoreDefinition storeDef: storeDefs) {
            targetStoreRoutingPlans.put(storeDef.getName(), new StoreRoutingPlan(targetCluster,
                                                                                 storeDef));
            finalStoreRoutingPlans.put(storeDef.getName(), new StoreRoutingPlan(finalCluster,
                                                                                storeDef));
        }

        UnnecessarilyComplicatedDataStructure ucds = new UnnecessarilyComplicatedDataStructure();
        // For every node in the final cluster ...
        for(Node stealerNode: finalCluster.getNodes()) {
            int stealerZoneId = stealerNode.getZoneId();
            int stealerNodeId = stealerNode.getId();

            // Consider all store definitions ...
            for(StoreDefinition storeDef: storeDefs) {
                StoreRoutingPlan targetSRP = targetStoreRoutingPlans.get(storeDef.getName());
                StoreRoutingPlan finalSRP = finalStoreRoutingPlans.get(storeDef.getName());
                for(int stealerPartitionId: finalSRP.getNaryPartitionIds(stealerNodeId)) {
                    // ... and all nary partition-stores
                    // steal what is needed!

                    // Do not steal a partition-store you host
                    if(targetSRP.getReplicationNodeList(stealerPartitionId).contains(stealerNodeId)) {
                        continue;
                    }

                    int stealerZoneReplicaType = finalSRP.getZoneReplicaType(stealerZoneId,
                                                                             stealerNodeId,
                                                                             stealerPartitionId);

                    int donorZoneId;
                    if(targetSRP.hasZoneReplicaType(stealerZoneId, stealerPartitionId)) {
                        // Steal from local n-ary (since one exists).
                        donorZoneId = stealerZoneId;
                    } else {
                        // Steal from zone that hosts primary partition Id.
                        // TODO: Add option to steal from specific
                        // donorZoneId.
                        int targetMasterNodeId = targetSRP.getNodeIdForPartitionId(stealerPartitionId);
                        donorZoneId = targetCluster.getNodeById(targetMasterNodeId).getZoneId();
                    }

                    int donorNodeId = targetSRP.getZoneReplicaNodeId(donorZoneId,
                                                                     stealerZoneReplicaType,
                                                                     stealerPartitionId);
                    int donorReplicaType = targetSRP.getReplicaType(donorNodeId, stealerPartitionId);

                    ucds.shovelCrapIn(stealerNodeId,
                                      donorNodeId,
                                      storeDef.getName(),
                                      donorReplicaType,
                                      stealerPartitionId);
                }
            }
        }

        return ucds.shovelCrapOut();
    }

    @Override
    public String toString() {
        if(batchPlan == null || batchPlan.isEmpty()) {
            return "No rebalancing required since batch plan is empty";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Rebalancing Batch Plan : ").append(Utils.NEWLINE);
        builder.append(RebalancePartitionsInfo.taskListToString(batchPlan));

        return builder.toString();
    }
}