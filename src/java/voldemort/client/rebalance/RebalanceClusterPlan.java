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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.utils.MoveMap;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

// TODO: (refactor) Rename RebalanceClusterPlan to RebalanceBatchPlan
// TODO: (refactor) Fix cluster nomenclature in general: make sure there are
// exactly three prefixes used to distinguish cluster xml: initial or current,
// target or spec or expanded, and final. 'target' has historically been
// overloaded to mean spec/expanded or final depending on context.
// TODO: Remove stealerBased boolean argument from constructor. If a
// stealer-based or donor-based plan is needed, then either
// RebalanceStealerBasedBatchPlan or RebalanceDonorBasedBatchPlan should be
// constructed.
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

    @Deprecated
    private final Queue<RebalanceNodePlan> rebalanceTaskQueue;
    /**
     * For the purposes of printing we collect the partition tuples for all
     * nodes for all stores
     */
    @Deprecated
    private final Map<Integer, Set<Pair<Integer, Integer>>> currentAllStoresNodeIdToAllPartitionTuples;
    @Deprecated
    private final Map<Integer, Set<Pair<Integer, Integer>>> targetAllStoresNodeIdToAllPartitionTuples;

    /**
     * Compares the targetCluster configuration with the desired finalCluster
     * and builds a map of Target node-id to map of source node-ids and
     * partitions desired to be stolen/fetched.
     * 
     * @param targetCluster The current cluster definition
     * @param finalCluster The target cluster definition
     * @param storeDefs The list of store definitions to rebalance
     * @param enabledDeletePartition Delete the RW partition on the donor side
     *        after rebalance
     * @param isStealerBased Do we want to generate the final plan based on the
     *        stealer node or the donor node?
     */
    public RebalanceClusterPlan(final Cluster targetCluster,
                                final Cluster finalCluster,
                                final List<StoreDefinition> storeDefs,
                                final boolean enabledDeletePartition) {
        this(targetCluster, finalCluster, storeDefs, enabledDeletePartition, true);
    }

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
                                final List<StoreDefinition> storeDefs,
                                final boolean enabledDeletePartition,
                                final boolean isStealerBased) {
        this.targetCluster = targetCluster;
        this.finalCluster = finalCluster;
        this.storeDefs = storeDefs;
        RebalanceUtils.validateTargetFinalCluster(targetCluster, finalCluster);
        RebalanceUtils.validateClusterStores(targetCluster, storeDefs);
        RebalanceUtils.validateClusterStores(finalCluster, storeDefs);

        this.currentAllStoresNodeIdToAllPartitionTuples = Maps.newHashMap();
        this.targetAllStoresNodeIdToAllPartitionTuples = Maps.newHashMap();

        this.batchPlan = batchPlan();
        /*-
        for(Node node: finalCluster.getNodes()) {
            this.batchPlan.addAll(getRebalancePartitionsInfo(node.getId(), enabledDeletePartition));
        }
         */

        // TODO: Add some priority ordering somewhere in this class.
        // prioritizeBatchPlan();

        // TODO: Once plan-level optimization is complete, remove server side
        // "optimization" that does not bother to steal partition-stores it
        // already hosts. That code will be unnecessary.

        // TODO: (begin) Remove this redundant code once the
        // getRebalancingTaskQueue method is actually removed from this class.
        // This method must be retained until the rebalance controlelr is
        // switched over to use RebalancePlan. (Why this list is constructed
        // with a concurrent linked queue is an open question...)
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        HashMap<Integer, List<RebalancePartitionsInfo>> nodeToBatchPlan = new HashMap<Integer, List<RebalancePartitionsInfo>>();
        for(RebalancePartitionsInfo info: batchPlan) {
            int nodeId = info.getDonorId();
            if(isStealerBased) {
                nodeId = info.getStealerId();
            }
            if(!nodeToBatchPlan.containsKey(nodeId)) {
                nodeToBatchPlan.put(nodeId, new ArrayList<RebalancePartitionsInfo>());
            }
            nodeToBatchPlan.get(nodeId).add(info);
        }

        for(int nodeId: nodeToBatchPlan.keySet()) {
            RebalanceNodePlan rnp = new RebalanceNodePlan(nodeId,
                                                          Lists.newArrayList(nodeToBatchPlan.get(nodeId)),
                                                          isStealerBased);
            this.rebalanceTaskQueue.offer(rnp);
        }
        // TODO: (end) Remove ...
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

    @Deprecated
    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        return rebalanceTaskQueue;
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

    /**
     * Prioritize the batchPlan such that primary partitions are ordered ahead
     * or other nary partitions in the list.
     */
    private void prioritizeBatchPlan() {
        // TODO: Steal logic from "ORderedClusterTransition" and put it here.
        // The RebalancePlan ought to specify the priority ordering and so place
        // primary moves ahead of nary moves.

        // TODO: Fix this to use zonePrimary rather than just primary. Need to
        // rebase with master to pick up appropriate helper methods first
        // though.

        // NO-OP

        /*-
         * Start of code to implement the basics.
        HashMap<Integer, List<RebalancePartitionsInfo>> naryToBatchPlan = new HashMap<Integer, List<RebalancePartitionsInfo>>();
        for(RebalancePartitionsInfo info: batchPlan) {
        }
        List<RebalancePartitionsInfo> infos = Lists.newArrayList(batchPlan);
         */
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
                                                       new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                       targetCluster,
                                                       0));
            }
            return result;
        }
    }

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

    // TODO: Get rid of this method
    // TODO: Revisit these "two principles". I am not sure about the second one.
    // Either because I don't like delete being mixed with this code or because
    // we probably want to copy from a to-be-deleted partitoin-store.
    /**
     * Generate the list of partition movement based on 2 principles:
     * 
     * <ol>
     * <li>The number of partitions don't change; they are only redistributed
     * across nodes
     * <li>A primary or replica partition that is going to be deleted is never
     * used to copy from data from another stealer
     * </ol>
     * 
     * @param targetCluster Current cluster configuration
     * @param finalCluster Target cluster configuration
     * @param storeDefs List of store definitions
     * @param stealerNodeId Id of the stealer node
     * @param enableDeletePartition To delete or not to delete?
     */
    @Deprecated
    private List<RebalancePartitionsInfo> getRebalancePartitionsInfo(final int stealerNodeId,
                                                                     final boolean enableDeletePartition) {
        final HashMap<Integer, HashMap<String, HashMap<Integer, List<Integer>>>> donorNodeToStoreToStealPartition = Maps.newHashMap();
        final HashMap<Integer, HashMap<String, HashMap<Integer, List<Integer>>>> donorNodeToStoreToDeletePartition = Maps.newHashMap();

        // Generate plans for individual stores
        for(StoreDefinition storeDef: storeDefs) {

            // For the "current" cluster, creates a map of node-ids to
            // corresponding list of [replica,partition] tuples (primary &
            // replicas)
            Map<Integer, Set<Pair<Integer, Integer>>> currentNodeIdToAllPartitionTuples = RebalanceUtils.getNodeIdToAllPartitions(targetCluster,
                                                                                                                                  storeDef,
                                                                                                                                  true);
            RebalanceUtils.combinePartitionTuples(currentAllStoresNodeIdToAllPartitionTuples,
                                                  currentNodeIdToAllPartitionTuples);

            // For the "target" cluster, creates a map of node-ids to
            // corresponding list of [replica,partition] tuples (primary &
            // replicas)
            Map<Integer, Set<Pair<Integer, Integer>>> targetNodeIdToAllPartitionTuples = RebalanceUtils.getNodeIdToAllPartitions(finalCluster,
                                                                                                                                 storeDef,
                                                                                                                                 true);

            RebalanceUtils.combinePartitionTuples(targetAllStoresNodeIdToAllPartitionTuples,
                                                  targetNodeIdToAllPartitionTuples);

            // A map of all the stealer nodes to their corresponding stolen
            Map<Integer, Set<Pair<Integer, Integer>>> stealerNodeIdToStolenPartitionTuples = RebalanceUtils.getStolenPartitionTuples(targetCluster,
                                                                                                                                     finalCluster,
                                                                                                                                     storeDef);

            // If null, done with this stealer node for this store
            if(stealerNodeIdToStolenPartitionTuples.get(stealerNodeId) == null) {
                continue;
            }

            // Now we find out which donor can donate partitions to this stealer
            final Set<Pair<Integer, Integer>> haveToStealTuples = Sets.newHashSet(stealerNodeIdToStolenPartitionTuples.get(stealerNodeId));
            for(Node donorNode: targetCluster.getNodes()) {

                // The same node can't donate
                if(donorNode.getId() == stealerNodeId)
                    continue;

                // Finished treating all partitions?
                if(haveFinishedPartitions(haveToStealTuples)) {
                    break;
                }

                final Set<Pair<Integer, Integer>> trackStealPartitionsTuples = new HashSet<Pair<Integer, Integer>>();
                final Set<Pair<Integer, Integer>> trackDeletePartitionsTuples = new HashSet<Pair<Integer, Integer>>();

                // TODO: donatePartitionTuple ought to take all donor nodes
                // as input and select "best" partition to donate. E.g., from
                // within the zone!

                // Checks if this donor node can donate any tuples
                donatePartitionTuple(donorNode,
                                     haveToStealTuples,
                                     trackStealPartitionsTuples,
                                     currentNodeIdToAllPartitionTuples.get(donorNode.getId()));

                // Check if we can delete the partitions this donor just donated
                donateDeletePartitionTuple(donorNode,
                                           trackStealPartitionsTuples,
                                           trackDeletePartitionsTuples,
                                           stealerNodeIdToStolenPartitionTuples.get(donorNode.getId()),
                                           enableDeletePartition);

                // Add it to our completed list
                if(trackStealPartitionsTuples.size() > 0) {
                    addPartitionsToPlan(trackStealPartitionsTuples,
                                        donorNodeToStoreToStealPartition,
                                        donorNode.getId(),
                                        storeDef.getName());

                }

                // Add it to our completed list
                if(trackDeletePartitionsTuples.size() > 0) {
                    addPartitionsToPlan(trackDeletePartitionsTuples,
                                        donorNodeToStoreToDeletePartition,
                                        donorNode.getId(),
                                        storeDef.getName());
                }
            }

        }

        // Now combine the plans generated individually into the actual
        // rebalance partitions information
        final List<RebalancePartitionsInfo> result = new ArrayList<RebalancePartitionsInfo>();

        for(int donorNodeId: donorNodeToStoreToStealPartition.keySet()) {
            HashMap<String, HashMap<Integer, List<Integer>>> storeToStealPartition = donorNodeToStoreToStealPartition.get(donorNodeId);
            HashMap<String, HashMap<Integer, List<Integer>>> storeToDeletePartition = donorNodeToStoreToDeletePartition.containsKey(donorNodeId) ? donorNodeToStoreToDeletePartition.get(donorNodeId)
                                                                                                                                                : new HashMap<String, HashMap<Integer, List<Integer>>>();
            result.add(new RebalancePartitionsInfo(stealerNodeId,
                                                   donorNodeId,
                                                   storeToStealPartition,
                                                   storeToDeletePartition,
                                                   targetCluster,
                                                   0));
        }

        return result;
    }

    private void addPartitionsToPlan(Set<Pair<Integer, Integer>> trackPartitionsTuples,
                                     HashMap<Integer, HashMap<String, HashMap<Integer, List<Integer>>>> donorNodeToStoreToPartitionTuples,
                                     int donorNodeId,
                                     String storeName) {
        HashMap<String, HashMap<Integer, List<Integer>>> storeToStealPartitionTuples = null;
        if(donorNodeToStoreToPartitionTuples.containsKey(donorNodeId)) {
            storeToStealPartitionTuples = donorNodeToStoreToPartitionTuples.get(donorNodeId);
        } else {
            storeToStealPartitionTuples = Maps.newHashMap();
            donorNodeToStoreToPartitionTuples.put(donorNodeId, storeToStealPartitionTuples);
        }
        storeToStealPartitionTuples.put(storeName,
                                        RebalanceUtils.flattenPartitionTuples(trackPartitionsTuples));
    }

    // TODO: (refactor) trackStealPartitoinsTuples is updated in this method.
    // I.e., the 'void' return code is misleading. AND, only specific
    // partitionId:replicaType's are actually stolen. AND, 'haveToStealTuples'
    // is also modified. Clean up this method!
    // TODO: (refactor): At least remove commented out historic code...
    /**
     * Given a donor node and a set of tuples that need to be stolen, checks if
     * the donor can contribute any
     * 
     * @param donorNode Donor node we are checking
     * @param haveToStealTuples The partition tuples which we ideally want to
     *        steal
     * @param trackStealPartitionsTuples Set of partitions tuples already stolen
     * @param donorPartitionTuples All partition tuples on donor node
     */
    /*-
    private void donatePartitionTuple(final Node donorNode,
                                      Set<Pair<Integer, Integer>> haveToStealTuples,
                                      Set<Pair<Integer, Integer>> trackStealPartitionsTuples,
                                      Set<Pair<Integer, Integer>> donorPartitionTuples) {
        final Iterator<Pair<Integer, Integer>> iter = haveToStealTuples.iterator();

        // Iterate over the partition tuples to steal and check if this node can
        // donate it
        while(iter.hasNext()) {
            Pair<Integer, Integer> partitionTupleToSteal = iter.next();
            if(donorPartitionTuples.contains(partitionTupleToSteal)) {
                trackStealPartitionsTuples.add(partitionTupleToSteal);

                // This partition has been donated, remove it
                iter.remove();
            }
        }
    }
     */
    private void donatePartitionTuple(final Node donorNode,
                                      Set<Pair<Integer, Integer>> haveToStealTuples,
                                      Set<Pair<Integer, Integer>> trackStealPartitionsTuples,
                                      Set<Pair<Integer, Integer>> donorPartitionTuples) {
        final Iterator<Pair<Integer, Integer>> iter = haveToStealTuples.iterator();

        // Iterate over the partition tuples to steal and check if this node can
        // donate it
        while(iter.hasNext()) {
            Pair<Integer, Integer> partitionTupleToSteal = iter.next();

            // TODO: HACK to steal from ANY node that has the desired partition.
            // Totally ignoring the replicaType.
            for(Pair<Integer, Integer> rt: donorPartitionTuples) {
                if(rt.getSecond() == partitionTupleToSteal.getSecond()) {
                    // TODO: passing in 'rt' instead of 'partitoinTupleToSteal'
                    // is a one-line change that circumvents server-side checks
                    // during execution that the "correct" replicaType is being
                    // stolen from a donor. This change is fragile and should be
                    // hardened by removing such replicaType checks from the
                    // code path.
                    trackStealPartitionsTuples.add(rt);
                    // trackStealPartitionsTuples.add(partitionTupleToSteal);

                    // This partition has been donated, remove it
                    iter.remove();
                }
            }
        }
    }

    /**
     * We do not delete if this donor node is also the stealer node for another
     * plan and is stealing data for the same partition ( irrespective of the
     * replica type ). This is a problem only for BDB storage engine but has
     * been placed at this level since for RO stores we don't delete the data
     * anyways.
     * 
     * @param donor Donor node
     * @param trackStealPartitionsTuples Partitions being stolen
     * @param trackDeletePartitionsTuples Partitions going to be deleted
     * @param donorPartitionTuples All partition tuples on donor node
     * @param enableDeletePartition Are we allowed to delete?
     */
    private void donateDeletePartitionTuple(final Node donor,
                                            Set<Pair<Integer, Integer>> trackStealPartitionsTuples,
                                            Set<Pair<Integer, Integer>> trackDeletePartitionsTuples,
                                            Set<Pair<Integer, Integer>> donorPartitionTuples,
                                            boolean enableDeletePartition) {

        // Are we allowed to delete AND do we have anything to check for
        // deletion?
        if(enableDeletePartition && trackStealPartitionsTuples.size() > 0) {

            // Retrieve information about what this donor node will steal
            List<Integer> partitionsStolenByDonor = RebalanceUtils.getPartitionsFromTuples(donorPartitionTuples);

            // Check if this stolen partitions overlap with those being donated
            for(Pair<Integer, Integer> tuple: trackStealPartitionsTuples) {
                if(!partitionsStolenByDonor.contains(tuple.getSecond())) {
                    trackDeletePartitionsTuples.add(tuple);
                }
            }
        }
    }

    private boolean haveFinishedPartitions(Set<Pair<Integer, Integer>> set) {
        return (set == null || set.size() == 0);
    }

    @Override
    public String toString() {
        if(batchPlan == null || batchPlan.isEmpty()) {
            return "No rebalancing required since batch plan is empty";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Rebalancing Batch Plan : ").append(Utils.NEWLINE);

        for(RebalancePartitionsInfo rebalancePartitionsInfo: batchPlan) {
            builder.append(rebalancePartitionsInfo).append(Utils.NEWLINE);
        }

        return builder.toString();

    }

    // TODO: deprecate this method
    @Deprecated
    public String printPartitionDistribution() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current Cluster: ")
          .append(Utils.NEWLINE)
          .append(RebalanceUtils.printMap(currentAllStoresNodeIdToAllPartitionTuples))
          .append(Utils.NEWLINE);
        sb.append("Target Cluster: ")
          .append(Utils.NEWLINE)
          .append(RebalanceUtils.printMap(targetAllStoresNodeIdToAllPartitionTuples));
        return sb.toString();
    }

}