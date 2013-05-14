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

// TODO: (refactor) Fix cluster nomenclature in general: make sure there are
// exactly three prefixes used to distinguish cluster xml: initial or current,
// target or spec or expanded, and final. 'target' has historically been
// overloaded to mean spec/expanded or final depending on context.
/**
 * Constructs a batch plan that goes from targetCluster to finalCluster. The
 * partition-stores included in the move are based on those listed in storeDefs.
 * This batch plan is execution-agnostic, i.e., a plan is generated and later
 * stealer- versus donor-based execution of that plan is decided.
 */
public class RebalanceBatchPlan {

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
    public RebalanceBatchPlan(final Cluster targetCluster,
                              final Cluster finalCluster,
                              final List<StoreDefinition> storeDefs) {
        this.targetCluster = targetCluster;
        this.finalCluster = finalCluster;
        this.storeDefs = storeDefs;
        RebalanceUtils.validateTargetFinalCluster(targetCluster, finalCluster);
        RebalanceUtils.validateClusterStores(targetCluster, storeDefs);
        RebalanceUtils.validateClusterStores(finalCluster, storeDefs);

        this.batchPlan = constructBatchPlan();

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

    // TODO: (replicaType) As part of dropping replicaType and
    // RebalancePartitionsInfo from code, simplify this object.
    /**
     * Gathers all of the state necessary to build a
     * List<RebalancePartitionsInfo> which is effectively a (batch) plan.
     */
    private class RebalancePartitionsInfoBuilder {

        final HashMap<Pair<Integer, Integer>, HashMap<String, HashMap<Integer, List<Integer>>>> stealerDonorToStoreToStealPartition;

        RebalancePartitionsInfoBuilder() {
            stealerDonorToStoreToStealPartition = Maps.newHashMap();
        }

        public void addPartitionStoreMove(int stealerNodeId,
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

        public List<RebalancePartitionsInfo> buildRebalancePartitionsInfos() {
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

    /**
     * Determine the batch plan and return it. The batch plan has the following
     * properties:
     * 
     * 1) A stealer node does not steal any partition-stores it already hosts.
     * 
     * 2) Use current policy to decide which node to steal from: see getDonorId
     * method.
     * 
     * Currently, this batch plan avoids all unnecessary cross zone moves,
     * distributes cross zone moves into new zones evenly across existing zones,
     * and copies replicaFactor partition-stores into any new zone.
     * 
     * @return the batch plan
     */
    private List<RebalancePartitionsInfo> constructBatchPlan() {
        // Construct all store routing plans once.
        HashMap<String, StoreRoutingPlan> targetStoreRoutingPlans = new HashMap<String, StoreRoutingPlan>();
        HashMap<String, StoreRoutingPlan> finalStoreRoutingPlans = new HashMap<String, StoreRoutingPlan>();
        for(StoreDefinition storeDef: storeDefs) {
            targetStoreRoutingPlans.put(storeDef.getName(), new StoreRoutingPlan(targetCluster,
                                                                                 storeDef));
            finalStoreRoutingPlans.put(storeDef.getName(), new StoreRoutingPlan(finalCluster,
                                                                                storeDef));
        }

        RebalancePartitionsInfoBuilder rpiBuilder = new RebalancePartitionsInfoBuilder();
        // For every node in the final cluster ...
        for(Node stealerNode: finalCluster.getNodes()) {
            int stealerZoneId = stealerNode.getZoneId();
            int stealerNodeId = stealerNode.getId();

            // Consider all store definitions ...
            for(StoreDefinition storeDef: storeDefs) {
                StoreRoutingPlan targetSRP = targetStoreRoutingPlans.get(storeDef.getName());
                StoreRoutingPlan finalSRP = finalStoreRoutingPlans.get(storeDef.getName());
                for(int stealerPartitionId: finalSRP.getZoneNAryPartitionIds(stealerNodeId)) {
                    // ... and all nary partition-stores,
                    // now steal what is needed

                    // Optimization: Do not steal a partition-store you already
                    // host!
                    if(targetSRP.getReplicationNodeList(stealerPartitionId).contains(stealerNodeId)) {
                        continue;
                    }

                    // Determine which node to steal from.
                    int donorNodeId = getDonorId(targetSRP,
                                                 finalSRP,
                                                 stealerZoneId,
                                                 stealerNodeId,
                                                 stealerPartitionId);

                    // Add this specific partition-store steal to the overall
                    // plan
                    int donorReplicaType = targetSRP.getReplicaType(donorNodeId, stealerPartitionId);
                    rpiBuilder.addPartitionStoreMove(stealerNodeId,
                                                     donorNodeId,
                                                     storeDef.getName(),
                                                     donorReplicaType,
                                                     stealerPartitionId);
                }
            }
        }

        return rpiBuilder.buildRebalancePartitionsInfos();
    }

    /**
     * Decide which donor node to steal from. This is a policy implementation.
     * I.e., in the future, additional policies could be considered. At that
     * time, this method should be overridden in a sub-class, or a policy object
     * ought to implement this algorithm.
     * 
     * Current policy:
     * 
     * 1) If possible, a stealer node that is the zone n-ary in the finalCluster
     * steals from the zone n-ary in the targetCluster in the same zone.
     * 
     * 2) If there are no partition-stores to steal in the same zone (i.e., this
     * is the "zone expansion" use case), then a differnt policy must be used.
     * The stealer node that is the zone n-ary in the finalCluster determines
     * which pre-existing zone in the targetCluster hosts the primary partition
     * id for the partition-store. The stealer then steals the zone n-ary from
     * that pre-existing zone.
     * 
     * This policy avoids unnecessary cross-zone moves and distributes the load
     * of cross-zone moves approximately-uniformly across pre-existing zones.
     * 
     * Other policies to consider:
     * 
     * - For zone expansion, steal all partition-stores from one specific
     * pre-existing zone.
     * 
     * - Replace heuristic to approximately uniformly distribute load among
     * existing zones to something more concrete (i.e. track steals from each
     * pre-existing zone and forcibly balance them).
     * 
     * - Select a single donor for all replicas in a new zone. This will require
     * donor-based rebalancing to be run (at least for this specific part of the
     * plan). This would reduce the number of donor-side scans of data. (But
     * still send replication factor copies over the WAN.) This would require
     * apparatus in the RebalanceController to work.
     * 
     * - Set up some sort of chain-replication in which a single stealer in the
     * new zone steals some replica from a pre-exising zone, and then other
     * n-aries in the new zone steal from the single cross-zone stealer in the
     * zone. This would require apparatus in the RebalanceController to work.
     * 
     * @param targetSRP
     * @param finalSRP
     * @param stealerZoneId
     * @param stealerNodeId
     * @param stealerPartitionId
     * @return the node id of the donor for this partition Id.
     */
    protected int getDonorId(StoreRoutingPlan targetSRP,
                             StoreRoutingPlan finalSRP,
                             int stealerZoneId,
                             int stealerNodeId,
                             int stealerPartitionId) {

        int donorZoneId;
        if(targetSRP.zoneHasReplica(stealerZoneId, stealerPartitionId)) {
            // Steal from local n-ary (since one exists).
            donorZoneId = stealerZoneId;
        } else {
            // Steal from zone that hosts primary partition Id.
            int targetMasterNodeId = targetSRP.getNodeIdForPartitionId(stealerPartitionId);
            donorZoneId = targetCluster.getNodeById(targetMasterNodeId).getZoneId();
        }

        int stealerZoneNAry = finalSRP.getZoneNaryForNodesPartition(stealerZoneId,
                                                                    stealerNodeId,
                                                                    stealerPartitionId);
        return targetSRP.getNodeIdForZoneNary(donorZoneId, stealerZoneNAry, stealerPartitionId);

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