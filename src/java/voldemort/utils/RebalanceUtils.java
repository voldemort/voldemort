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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.client.rebalance.RebalancePlan;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.tools.PartitionBalance;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
     * Given the current partitionIds list, try to check if the donor node would
     * already contain that partition and if yes, ignore it
     * 
     * @param stealerNodeId Stealer node id
     * @param cluster Cluster metadata
     * @param storeDef Store definition
     * @return Optimized partition list
     */
    public static List<Integer> getOptimizedPartitionIds(int stealerNodeId,
                                                         Cluster cluster,
                                                         StoreDefinition storeDef,
                                                         List<Integer> partitionIds) {

        List<Integer> optimizedPartitionIds = Lists.newArrayList();
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);
        List<Integer> partitionList = Lists.newArrayList();
        for(Integer partition: partitionIds) {
            List<Integer> preferenceList = strategy.getReplicatingPartitionList(partition);
            // If this node was already in the preference list before, a copy of
            // the data will
            // already exist - Don't copy it!
            if(!ClusterUtils.containsPreferenceList(cluster, preferenceList, stealerNodeId)) {
                partitionList.add(partition);
            }
        }
        if(partitionList.size() > 0) {
            optimizedPartitionIds.addAll(partitionList);
        }
        return optimizedPartitionIds;
    }

    /**
     * Given a list of partition informations check all of them belong to the
     * same donor node
     * 
     * @param partitionInfos List of partition infos
     * @param expectedDonorId Expected donor node id ( If -1, then just checks
     *        if all are same )
     */
    public static void assertSameDonor(List<RebalancePartitionsInfo> partitionsInfo,
                                       int expectedDonorId) {
        int donorId = (expectedDonorId < 0) ? partitionsInfo.get(0).getDonorId() : expectedDonorId;
        for(RebalancePartitionsInfo info: partitionsInfo) {
            if(info.getDonorId() != donorId) {
                throw new VoldemortException("Found a stealer information " + info
                                             + " having a different donor node from others ( "
                                             + donorId + " )");
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
        // Constructing a StoreRoutingPlan has the (desirable in this
        // case) side-effect of verifying that the store definition is congruent
        // with the cluster definition. If there are issues, exceptions are
        // thrown.
        for(StoreDefinition storeDefinition: storeDefs) {
            new StoreRoutingPlan(cluster, storeDefinition);
        }
        return;
    }

    // TODO: This method is biased towards the 3 currently supported use cases:
    // shuffle, cluster expansion, and zone expansion. There are two other use
    // cases we need to consider: cluster contraction (reducing # nodes in a
    // zone) and zone contraction (reducing # of zones). We probably want to end
    // up pass an enum into this method so we can do proper checks based on use
    // case.
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
     * An interim cluster ought to be a super set of current cluster. I.e., it
     * ought to either be the same as current cluster (every partition is mapped
     * to the same node of current & interim), or it ought to have more nodes
     * (possibly in new zones) without partitions.
     * 
     * @param currentCluster
     * @param interimCluster
     */
    public static void validateCurrentInterimCluster(final Cluster currentCluster,
                                                     final Cluster interimCluster) {
        validateClusterPartitionCounts(currentCluster, interimCluster);
        validateClusterNodeState(currentCluster, interimCluster);
        validateClusterPartitionState(currentCluster, interimCluster);

        return;
    }

    /**
     * Interim and final clusters ought to have same partition counts, same
     * zones, and same node state. Partitions per node may of course differ.
     * 
     * @param interimCluster
     * @param finalCluster
     */
    public static void validateInterimFinalCluster(final Cluster interimCluster,
                                                   final Cluster finalCluster) {
        validateClusterPartitionCounts(interimCluster, finalCluster);
        validateClusterZonesSame(interimCluster, finalCluster);
        validateClusterNodeCounts(interimCluster, finalCluster);
        validateClusterNodeState(interimCluster, finalCluster);
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
        Set<Zone> lhsSet = new HashSet<Zone>(lhs.getZones());
        Set<Zone> rhsSet = new HashSet<Zone>(rhs.getZones());
        if(!lhsSet.equals(rhsSet))
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
     * Given the current cluster and final cluster, generates an interim cluster
     * with empty new nodes (and zones).
     * 
     * @param currentCluster Current cluster metadata
     * @param finalCluster Final cluster metadata
     * @return Returns a new interim cluster which contains nodes and zones of
     *         final cluster, but with empty partition lists if they were not
     *         present in current cluster.
     */
    public static Cluster getInterimCluster(Cluster currentCluster, Cluster finalCluster) {
        List<Node> newNodeList = new ArrayList<Node>(currentCluster.getNodes());
        for(Node node: finalCluster.getNodes()) {
            if(!currentCluster.hasNodeWithId(node.getId())) {
                newNodeList.add(UpdateClusterUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        Collections.sort(newNodeList);
        return new Cluster(currentCluster.getName(),
                           newNodeList,
                           Lists.newArrayList(finalCluster.getZones()));
    }

    /**
     * For a particular stealer node find all the "primary" <replica, partition>
     * tuples it will steal. In other words, expect the "replica" part to be 0
     * always.
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param finalCluster The final cluster definition
     * @param stealNodeId Node id of the stealer node
     * @return Returns a list of primary partitions which this stealer node will
     *         get
     */
    public static List<Integer> getStolenPrimaryPartitions(final Cluster currentCluster,
                                                           final Cluster finalCluster,
                                                           final int stealNodeId) {
        List<Integer> finalList = new ArrayList<Integer>(finalCluster.getNodeById(stealNodeId)
                                                                     .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if (currentCluster.hasNodeWithId(stealNodeId)) {
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();
        } else {
            if(logger.isDebugEnabled()) {
                logger.debug("Current cluster does not contain stealer node (cluster : [[["
                             + currentCluster + "]]], node id " + stealNodeId + ")");
            }
        }
        finalList.removeAll(currentList);

        return finalList;
    }

    // TODO: (replicaType) deprecate.
   
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
        final Map<Integer, Integer> partitionToNodeIdMap = cluster.getPartitionIdToNodeIdMap();

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

                    // For zone expansion & shrinking, this warning is expected
                    // in some cases. For other use cases (shuffling, cluster
                    // expansion), this warning indicates that something
                    // is wrong between the clusters and store defs.
                    logger.warn("Number of replicas returned (" + replicaPartitionList.size()
                                + ") does not make sense given the replication factor ("
                                + storeDef.getReplicationFactor() + ") and that there are "
                                + cluster.getNumberOfZones() + " zones of which "
                                + zonesWithPartitions + " have partitions (and of which "
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
     * @param batchId Task id
     * @param logger Logger class
     * @param message The message to print
     */
    public static void printBatchLog(int batchId, Logger logger, String message) {
        logger.info("[Rebalance batch id " + batchId + "] " + message);
    }

    /**
     * Print log to the following logger ( Info level )
     * 
     * @param batchId
     * @param taskId
     * @param logger
     * @param message
     */
    public static void printBatchTaskLog(int batchId, int taskId, Logger logger, String message) {
        logger.info("[Rebalance batch/task id " + batchId + "/" + taskId + "] " + message);
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

    // TODO: (replicaType) deprecate this method.
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
     * Given the current and final cluster dumps it into the output directory
     * 
     * @param currentCluster Initial cluster metadata
     * @param finalCluster Final cluster metadata
     * @param outputDir Output directory where to dump this file
     * @throws IOException
     */
    public static void dumpClusters(Cluster currentCluster,
                                    Cluster finalCluster,
                                    String outputDirName) {
        dumpClusters(currentCluster, finalCluster, outputDirName, "");
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
     * Prints the plan to a file.
     * 
     * @param outputDirName
     * @param plan
     */
    public static void dumpPlanToFile(String outputDirName, RebalancePlan plan) {
        if(outputDirName != null) {
            File outputDir = new File(outputDirName);
            if(!outputDir.exists()) {
                Utils.mkdirs(outputDir);
            }
            try {
                FileUtils.writeStringToFile(new File(outputDirName, "plan.out"), plan.toString());
            } catch(IOException e) {
                logger.error("IOException during dumpPlanToFile: " + e);
            }
        }
    }

    // TODO: (replicaType) deprecate.
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

    public static int countPartitionStores(List<RebalancePartitionsInfo> infos) {
        int count = 0;
        for(RebalancePartitionsInfo info: infos) {
            count += info.getPartitionStoreCount();
        }
        return count;
    }


    public static int countTaskStores(List<RebalanceTaskInfo> infos) {
        int count = 0;
        for(RebalanceTaskInfo info: infos) {
            count += info.getPartitionStoreCount();
        }
        return count;
    }

    /**
     * Given a list of partition plans and a set of stores, copies the store
     * names to every individual plan and creates a new list
     * 
     * @param existingPlanList Existing partition plan list
     * @param storeDefs List of store names we are rebalancing
     * @return List of updated partition plan
     */
    public static List<RebalanceTaskInfo> filterTaskPlanWithStores(List<RebalanceTaskInfo> existingPlanList,
                                                                   List<StoreDefinition> storeDefs) {
        List<RebalanceTaskInfo> plans = Lists.newArrayList();
        List<String> storeNames = StoreDefinitionUtils.getStoreNames(storeDefs);

        for(RebalanceTaskInfo existingPlan: existingPlanList) {
            RebalanceTaskInfo info = RebalanceTaskInfo.create(existingPlan.toJsonString());

            // Filter the plans only for stores given
            HashMap<String, List<Integer>> storeToPartitions = info.getStoreToPartitionIds();

            HashMap<String, List<Integer>> newStoreToPartitions = Maps.newHashMap();
            for(String storeName: storeNames) {
                if(storeToPartitions.containsKey(storeName))
                    newStoreToPartitions.put(storeName, storeToPartitions.get(storeName));
            }
            info.setStoreToPartitionList(newStoreToPartitions);
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
    public static HashMap<Integer, List<RebalanceTaskInfo>> groupPartitionsTaskByNode(List<RebalanceTaskInfo> rebalanceTaskPlanList,
                                                                                      boolean groupByStealerNode) {
        HashMap<Integer, List<RebalanceTaskInfo>> nodeToTaskInfo = Maps.newHashMap();
        if(rebalanceTaskPlanList != null) {
            for(RebalanceTaskInfo partitionInfo: rebalanceTaskPlanList) {
                int nodeId = groupByStealerNode ? partitionInfo.getStealerId()
                                               : partitionInfo.getDonorId();
                List<RebalanceTaskInfo> taskInfos = nodeToTaskInfo.get(nodeId);
                if(taskInfos == null) {
                    taskInfos = Lists.newArrayList();
                    nodeToTaskInfo.put(nodeId, taskInfos);
                }
                taskInfos.add(partitionInfo);
            }
        }
        return nodeToTaskInfo;
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
}
