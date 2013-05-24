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

package voldemort.client.rebalance;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.tools.PartitionBalance;
import voldemort.utils.MoveMap;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultimap;

/**
 * RebalancePlan encapsulates all aspects of planning a shuffle, cluster
 * expansion, or zone expansion.
 */
public class RebalancePlan {

    private static final Logger logger = Logger.getLogger(RebalancePlan.class);

    /**
     * The number of "primary" partition IDs to move in each batch of the plan.
     * Moving a primary partition ID between nodes results in between zero and
     * (# of zones) * (2) * (# stores) partition-stores being moved. The (2)
     * comes from an upper bound of a single move affecting two-nodes per zone.
     */
    public final static int BATCH_SIZE = Integer.MAX_VALUE;

    private final Cluster currentCluster;
    private final List<StoreDefinition> currentStoreDefs;
    private final Cluster finalCluster;
    private final List<StoreDefinition> finalStoreDefs;
    private final int batchSize;
    private final String outputDir;

    private List<RebalanceBatchPlan> batchPlans;

    // Aggregate stats
    private int numPrimaryPartitionMoves;
    private int numPartitionStoreMoves;
    private int numXZonePartitionStoreMoves;
    private final MoveMap nodeMoveMap;
    private final MoveMap zoneMoveMap;

    /**
     * Constructs a plan for the specified change from currentCluster/StoreDefs
     * to finalCluster/StoreDefs.
     * 
     * finalStoreDefs are needed for the zone expansion use case since store
     * definitions depend on the number of zones.
     * 
     * In theory, since current & final StoreDefs are passed in, this plan could
     * be used to transform deployed store definitions. In practice, this use
     * case has not been tested.
     * 
     * @param currentCluster current deployed cluster.
     * @param currentStoreDefs current deployed store defs
     * @param finalCluster desired deployed cluster
     * @param finalStoreDefs desired deployed store defs
     * @param batchSize number of primary partitions to move in each batch.
     * @param outputDir directory in which to dump metadata files for the plan
     */
    public RebalancePlan(final Cluster currentCluster,
                         final List<StoreDefinition> currentStoreDefs,
                         final Cluster finalCluster,
                         final List<StoreDefinition> finalStoreDefs,
                         int batchSize,
                         String outputDir) {
        this.currentCluster = currentCluster;
        this.currentStoreDefs = RebalanceUtils.validateRebalanceStore(currentStoreDefs);
        this.finalCluster = finalCluster;
        this.finalStoreDefs = RebalanceUtils.validateRebalanceStore(finalStoreDefs);
        this.batchSize = batchSize;
        this.outputDir = outputDir;

        // Derive the interimCluster from current & final cluster xml
        RebalanceUtils.validateCurrentFinalCluster(this.currentCluster, this.finalCluster);
        Cluster interimCluster = RebalanceUtils.getInterimCluster(this.currentCluster,
                                                                  this.finalCluster);

        // Verify each cluster/storedefs pair
        RebalanceUtils.validateClusterStores(this.currentCluster, this.currentStoreDefs);
        RebalanceUtils.validateClusterStores(this.finalCluster, this.finalStoreDefs);
        RebalanceUtils.validateClusterStores(interimCluster, this.finalStoreDefs);

        // Log key arguments
        logger.info("Current cluster : " + currentCluster);
        logger.info("Interim cluster : " + interimCluster);
        logger.info("Final cluster : " + finalCluster);
        logger.info("Batch size : " + batchSize);

        // Initialize the plan
        batchPlans = new ArrayList<RebalanceBatchPlan>();

        // Initialize aggregate statistics
        numPrimaryPartitionMoves = 0;
        numPartitionStoreMoves = 0;
        numXZonePartitionStoreMoves = 0;
        nodeMoveMap = new MoveMap(interimCluster.getNodeIds());
        zoneMoveMap = new MoveMap(interimCluster.getZoneIds());

        plan();
    }

    public RebalancePlan(final Cluster currentCluster,
                         final List<StoreDefinition> currentStores,
                         final Cluster finalCluster,
                         int batchSize,
                         String outputDir) {
        this(currentCluster, currentStores, finalCluster, currentStores, batchSize, outputDir);
    }

    /**
     * Create a plan. The plan consists of batches. Each batch involves the
     * movement of no more than batchSize primary partitions. The movement of a
     * single primary partition may require migration of other n-ary replicas,
     * and potentially deletions. Migrating a primary or n-ary partition
     * requires migrating one partition-store for every store hosted at that
     * partition.
     * 
     */
    private void plan() {
        // Mapping of stealer node to list of primary partitions being moved
        final TreeMultimap<Integer, Integer> stealerToStolenPrimaryPartitions = TreeMultimap.create();

        // Output initial and final cluster
        if(outputDir != null)
            RebalanceUtils.dumpClusters(currentCluster, finalCluster, outputDir);

        // Determine which partitions must be stolen
        for(Node stealerNode: finalCluster.getNodes()) {
            List<Integer> stolenPrimaryPartitions = RebalanceUtils.getStolenPrimaryPartitions(currentCluster,
                                                                                              finalCluster,
                                                                                              stealerNode.getId());
            if(stolenPrimaryPartitions.size() > 0) {
                numPrimaryPartitionMoves += stolenPrimaryPartitions.size();
                stealerToStolenPrimaryPartitions.putAll(stealerNode.getId(),
                                                        stolenPrimaryPartitions);
            }
        }

        // Determine plan batch-by-batch
        int batches = 0;
        Cluster batchCurrentCluster = Cluster.cloneCluster(currentCluster);
        List<StoreDefinition> batchCurrentStoreDefs = this.currentStoreDefs;
        List<StoreDefinition> batchFinalStoreDefs = this.finalStoreDefs;
        Cluster batchFinalCluster = RebalanceUtils.getInterimCluster(this.currentCluster,
                                                                     this.finalCluster);

        while(!stealerToStolenPrimaryPartitions.isEmpty()) {

            int partitions = 0;
            List<Entry<Integer, Integer>> partitionsMoved = Lists.newArrayList();
            for(Entry<Integer, Integer> stealerToPartition: stealerToStolenPrimaryPartitions.entries()) {
                partitionsMoved.add(stealerToPartition);
                batchFinalCluster = RebalanceUtils.createUpdatedCluster(batchFinalCluster,
                                                                        stealerToPartition.getKey(),
                                                                        Lists.newArrayList(stealerToPartition.getValue()));
                partitions++;
                if(partitions == batchSize)
                    break;
            }

            // Remove the partitions moved
            for(Iterator<Entry<Integer, Integer>> partitionMoved = partitionsMoved.iterator(); partitionMoved.hasNext();) {
                Entry<Integer, Integer> entry = partitionMoved.next();
                stealerToStolenPrimaryPartitions.remove(entry.getKey(), entry.getValue());
            }

            if(outputDir != null)
                RebalanceUtils.dumpClusters(batchCurrentCluster,
                                            batchFinalCluster,
                                            outputDir,
                                            "batch-" + Integer.toString(batches) + ".");

            // Generate a plan to compute the tasks
            final RebalanceBatchPlan RebalanceBatchPlan = new RebalanceBatchPlan(batchCurrentCluster,
                                                                                 batchCurrentStoreDefs,
                                                                                 batchFinalCluster,
                                                                                 batchFinalStoreDefs);
            batchPlans.add(RebalanceBatchPlan);

            numXZonePartitionStoreMoves += RebalanceBatchPlan.getCrossZonePartitionStoreMoves();
            numPartitionStoreMoves += RebalanceBatchPlan.getPartitionStoreMoves();
            nodeMoveMap.add(RebalanceBatchPlan.getNodeMoveMap());
            zoneMoveMap.add(RebalanceBatchPlan.getZoneMoveMap());

            batches++;
            batchCurrentCluster = Cluster.cloneCluster(batchFinalCluster);
            // batchCurrentStoreDefs can only be different from
            // batchFinalStoreDefs for the initial batch.
            batchCurrentStoreDefs = batchFinalStoreDefs;
        }

        logger.info(this);
    }

    /**
     * Determines storage overhead and returns pretty printed summary.
     * 
     * @param finalNodeToOverhead Map of node IDs from final cluster to number
     *        of partition-stores to be moved to the node.
     * @return pretty printed string summary of storage overhead.
     */
    private String storageOverhead(Map<Integer, Integer> finalNodeToOverhead) {
        double maxOverhead = Double.MIN_VALUE;
        PartitionBalance pb = new PartitionBalance(currentCluster, currentStoreDefs);
        StringBuilder sb = new StringBuilder();
        sb.append("Per-node store-overhead:").append(Utils.NEWLINE);
        DecimalFormat doubleDf = new DecimalFormat("####.##");
        for(int nodeId: finalCluster.getNodeIds()) {
            Node node = finalCluster.getNodeById(nodeId);
            String nodeTag = "Node " + String.format("%4d", nodeId) + " (" + node.getHost() + ")";
            int initialLoad = 0;
            if(currentCluster.getNodeIds().contains(nodeId)) {
                initialLoad = pb.getNaryPartitionCount(nodeId);
            }
            int toLoad = 0;
            if(finalNodeToOverhead.containsKey(nodeId)) {
                toLoad = finalNodeToOverhead.get(nodeId);
            }
            double overhead = (initialLoad + toLoad) / (double) initialLoad;
            if(initialLoad > 0 && maxOverhead < overhead) {
                maxOverhead = overhead;
            }

            String loadTag = String.format("%6d", initialLoad) + " + "
                             + String.format("%6d", toLoad) + " -> "
                             + String.format("%6d", initialLoad + toLoad) + " ("
                             + doubleDf.format(overhead) + " X)";
            sb.append(nodeTag + " : " + loadTag).append(Utils.NEWLINE);
        }
        sb.append(Utils.NEWLINE)
          .append("****  Max per-node storage overhead: " + doubleDf.format(maxOverhead) + " X.")
          .append(Utils.NEWLINE);
        return (sb.toString());
    }

    public Cluster getCurrentCluster() {
        return currentCluster;
    }

    public List<StoreDefinition> getCurrentStores() {
        return currentStoreDefs;
    }

    public Cluster getFinalCluster() {
        return finalCluster;
    }

    public List<StoreDefinition> getFinalStores() {
        return finalStoreDefs;
    }

    /**
     * 
     * @return The plan!
     */
    public List<RebalanceBatchPlan> getPlan() {
        return batchPlans;
    }

    /**
     * Total number of rebalancing tasks in the plan.
     * 
     * @return number of rebalancing tasks in the plan.
     */
    public int taskCount() {
        int numTasks = 0;
        for(RebalanceBatchPlan batchPlan: batchPlans) {
            numTasks += batchPlan.getTaskCount();
        }
        return numTasks;
    }

    public int getPrimariesMoved() {
        return numPrimaryPartitionMoves;
    }

    public int getPartitionStoresMoved() {
        return numPartitionStoreMoves;
    }

    public int getPartitionStoresMovedXZone() {
        return numXZonePartitionStoreMoves;
    }

    public MoveMap getNodeMoveMap() {
        return nodeMoveMap;
    }

    public MoveMap getZoneMoveMap() {
        return zoneMoveMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // Add entire plan batch-by-batch, partition info-by-partition info...
        for(RebalanceBatchPlan batchPlan: batchPlans) {
            sb.append(batchPlan).append(Utils.NEWLINE);
        }
        // Add invalid metadata rate analysis
        sb.append(RebalanceUtils.analyzeInvalidMetadataRate(currentCluster,
                                                            currentStoreDefs,
                                                            finalCluster,
                                                            finalStoreDefs));
        // Summarize aggregate plan stats
        sb.append("Total number of primary partition moves : " + numPrimaryPartitionMoves)
          .append(Utils.NEWLINE)
          .append("Total number of rebalance tasks : " + taskCount())
          .append(Utils.NEWLINE)
          .append("Total number of partition-store moves : " + numPartitionStoreMoves)
          .append(Utils.NEWLINE)
          .append("Total number of cross-zone partition-store moves :"
                  + numXZonePartitionStoreMoves)
          .append(Utils.NEWLINE)
          .append("Zone move map (partition-stores):")
          .append(Utils.NEWLINE)
          .append("(zone id) -> (zone id) = # of partition-stores moving from the former zone to the latter")
          .append(Utils.NEWLINE)
          .append(zoneMoveMap)
          .append(Utils.NEWLINE)
          .append("Node flow map (partition-stores):")
          .append(Utils.NEWLINE)
          .append("# partitions-stores stealing into -> (node id) -> # of partition-stores donating out of")
          .append(Utils.NEWLINE)
          .append(nodeMoveMap.toFlowString())
          .append(Utils.NEWLINE)
          .append(storageOverhead(nodeMoveMap.groupByTo()))
          .append(Utils.NEWLINE);

        return sb.toString();
    }
}
