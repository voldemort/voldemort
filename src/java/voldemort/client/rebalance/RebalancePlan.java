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

import java.io.File;
import java.io.StringReader;
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
import voldemort.utils.MoveMap;
import voldemort.utils.PartitionBalance;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultimap;

// TODO: Add a header comment.
// TODO: Remove stealerBased from the constructor once RebalanceController is
// switched over to use RebalancePlan. (Or sooner)
// TODO: Add (simple & basic) tests of RebalancePlan
public class RebalancePlan {

    private static final Logger logger = Logger.getLogger(RebalancePlan.class);

    private final Cluster currentCluster;
    private final List<StoreDefinition> currentStores;
    private final Cluster finalCluster;
    private final List<StoreDefinition> finalStores;
    private final boolean stealerBased;
    private final int batchSize;
    private final String outputDir;

    // TODO: (refactor) Better name than targetCluster? expandedCluster?
    // specCluster?
    private final Cluster targetCluster;
    private List<RebalanceClusterPlan> batchPlans;

    // Aggregate stats
    private int numPrimaryPartitionMoves;
    private int numPartitionStoreMoves;
    private int numXZonePartitionStoreMoves;
    private final MoveMap nodeMoveMap;
    private final MoveMap zoneMoveMap;

    public RebalancePlan(final Cluster currentCluster,
                         final List<StoreDefinition> currentStores,
                         final Cluster finalCluster,
                         final List<StoreDefinition> finalStores,
                         boolean stealerBased,
                         int batchSize,
                         String outputDir) {
        this.currentCluster = currentCluster;
        this.currentStores = RebalanceUtils.validateRebalanceStore(currentStores);
        this.finalCluster = finalCluster;
        this.finalStores = RebalanceUtils.validateRebalanceStore(finalStores);
        this.stealerBased = stealerBased;
        this.batchSize = batchSize;
        this.outputDir = outputDir;

        // Derive the targetCluster from current & final cluster xml
        RebalanceUtils.validateCurrentFinalCluster(this.currentCluster, this.finalCluster);
        this.targetCluster = RebalanceUtils.getTargetCluster(this.currentCluster, this.finalCluster);

        // Verify each cluster/storedefs pair
        RebalanceUtils.validateClusterStores(this.currentCluster, this.currentStores);
        RebalanceUtils.validateClusterStores(this.finalCluster, this.finalStores);
        RebalanceUtils.validateClusterStores(this.targetCluster, this.finalStores);

        // Log key arguments
        logger.info("Current cluster : " + currentCluster);
        logger.info("Target cluster : " + targetCluster);
        logger.info("Final cluster : " + finalCluster);
        logger.info("Batch size : " + batchSize);

        // Initialize the plan
        batchPlans = new ArrayList<RebalanceClusterPlan>();

        // Initialize aggregate statistics
        numPrimaryPartitionMoves = 0;
        numPartitionStoreMoves = 0;
        numXZonePartitionStoreMoves = 0;
        nodeMoveMap = new MoveMap(targetCluster.getNodeIds());
        zoneMoveMap = new MoveMap(targetCluster.getZoneIds());

        plan();
    }

    public RebalancePlan(final Cluster currentCluster,
                         final List<StoreDefinition> currentStores,
                         final Cluster finalCluster,
                         boolean stealerBased,
                         int batchSize,
                         String outputDir) {
        this(currentCluster,
             currentStores,
             finalCluster,
             currentStores,
             stealerBased,
             batchSize,
             outputDir);
    }

    /**
     * Create a plan. The plan consists of batches. Each batch involves the
     * movement of nor more than batchSize primary partitions. The movement of a
     * single primary partition may require migration of other n-ary replicas,
     * and potentially deletions. Migrating a primary or n-ary partition
     * requires migrating one partition-store for every store hosted at that
     * partition.
     * 
     */
    private void plan() {
        // Mapping of stealer node to list of primary partitions being moved
        final TreeMultimap<Integer, Integer> stealerToStolenPrimaryPartitions = TreeMultimap.create();

        // Used for creating clones
        ClusterMapper mapper = new ClusterMapper();

        // Output initial and final cluster
        if(outputDir != null)
            RebalanceUtils.dumpCluster(targetCluster, finalCluster, new File(outputDir));

        // Determine which partitions must be stolen
        for(Node stealerNode: finalCluster.getNodes()) {
            List<Integer> stolenPrimaryPartitions = RebalanceUtils.getStolenPrimaryPartitions(targetCluster,
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
        Cluster batchTargetCluster = mapper.readCluster(new StringReader(mapper.writeCluster(targetCluster)));
        while(!stealerToStolenPrimaryPartitions.isEmpty()) {

            // Generate a batch partitions to move
            Cluster batchFinalCluster = mapper.readCluster(new StringReader(mapper.writeCluster(batchTargetCluster)));
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

            // TODO: Change naming convention in dumpCluster to be current- &
            // final- or target- & final-
            if(outputDir != null)
                RebalanceUtils.dumpCluster(batchTargetCluster,
                                           batchFinalCluster,
                                           new File(outputDir),
                                           "batch-" + Integer.toString(batches) + ".");

            // Generate a plan to compute the tasks
            // TODO: OK to remove option to "delete" from planning?
            boolean deleteEnabled = false;
            final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(batchTargetCluster,
                                                                                       batchFinalCluster,
                                                                                       finalStores,
                                                                                       deleteEnabled,
                                                                                       stealerBased);
            batchPlans.add(rebalanceClusterPlan);

            numXZonePartitionStoreMoves += rebalanceClusterPlan.getCrossZonePartitionStoreMoves();
            numPartitionStoreMoves += rebalanceClusterPlan.getPartitionStoreMoves();
            nodeMoveMap.add(rebalanceClusterPlan.getNodeMoveMap());
            zoneMoveMap.add(rebalanceClusterPlan.getZoneMoveMap());

            batches++;
            batchTargetCluster = mapper.readCluster(new StringReader(mapper.writeCluster(batchFinalCluster)));
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
        PartitionBalance pb = new PartitionBalance(currentCluster, currentStores);
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

    /**
     * 
     * @return The plan!
     */
    public List<RebalanceClusterPlan> getPlan() {
        return batchPlans;
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
        // Dump entire plan batch-by-batch, partition info-by-partition info...
        for(RebalanceClusterPlan batchPlan: batchPlans) {
            sb.append(batchPlan).append(Utils.NEWLINE);
        }
        // Dump aggregate stats of the plan
        sb.append("Total number of primary partition moves : " + numPrimaryPartitionMoves)
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
