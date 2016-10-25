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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.rebalance.RebalancePlan;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.tools.PartitionBalance;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

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
    public final static String finalStoresFileName = "final-stores.xml";

    /**
     * Verify store definitions are congruent with cluster definition.
     * 
     * @param cluster
     * @param storeDefs
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
     * Given the current cluster and a zone id that needs to be dropped, this
     * method will remove all partitions from the zone that is being dropped and
     * move it to the existing zones. The partitions are moved intelligently so
     * as not to avoid any data movement in the existing zones.
     * 
     * This is achieved by moving the partitions to nodes in the surviving zones
     * that is zone-nry to that partition in the surviving zone.
     * 
     * @param currentCluster Current cluster metadata
     * @return Returns an interim cluster with empty partition lists on the
     *         nodes from the zone being dropped
     * 
     */
    public static Cluster vacateZone(Cluster currentCluster, int dropZoneId) {
        Cluster returnCluster = Cluster.cloneCluster(currentCluster);
        // Go over each node in the zone being dropped
        for(Integer nodeId: currentCluster.getNodeIdsInZone(dropZoneId)) {
            // For each node grab all the partitions it hosts
            for(Integer partitionId: currentCluster.getNodeById(nodeId).getPartitionIds()) {
                // Now for each partition find a new home..which would be a node
                // in one of the existing zones
                int finalZoneId = -1;
                int finalNodeId = -1;
                int adjacentPartitionId = partitionId;
                do {
                    adjacentPartitionId = (adjacentPartitionId + 1)
                                          % currentCluster.getNumberOfPartitions();
                    finalNodeId = currentCluster.getNodeForPartitionId(adjacentPartitionId).getId();
                    finalZoneId = currentCluster.getZoneForPartitionId(adjacentPartitionId).getId();
                    if(adjacentPartitionId == partitionId) {
                        logger.error("PartitionId " + partitionId + "stays unchanged \n");
                    } else {
                        logger.info("PartitionId " + partitionId
                                    + " goes together with partition " + adjacentPartitionId
                                    + " on node " + finalNodeId + " in zone " + finalZoneId);
                        returnCluster = UpdateClusterUtils.createUpdatedCluster(returnCluster,
                                                                                finalNodeId,
                                                                                Lists.newArrayList(partitionId));
                    }
                } while(finalZoneId == dropZoneId);
            }
        }
        return returnCluster;
    }

    /**
     * Given a interim cluster with a previously vacated zone, constructs a new
     * cluster object with the drop zone completely removed
     * 
     * @param intermediateCluster
     * @param dropZoneId
     * @return adjusted cluster with the zone dropped
     */
    public static Cluster dropZone(Cluster intermediateCluster, int dropZoneId) {
        // Filter out nodes that don't belong to the zone being dropped
        Set<Node> survivingNodes = new HashSet<Node>();
        for(int nodeId: intermediateCluster.getNodeIds()) {
            if(intermediateCluster.getNodeById(nodeId).getZoneId() != dropZoneId) {
                survivingNodes.add(intermediateCluster.getNodeById(nodeId));
            }
        }

        // Filter out dropZoneId from all zones
        Set<Zone> zones = new HashSet<Zone>();
        for(int zoneId: intermediateCluster.getZoneIds()) {
            if(zoneId == dropZoneId) {
                continue;
            }
            List<Integer> proximityList = intermediateCluster.getZoneById(zoneId)
                                                             .getProximityList();
            proximityList.remove(new Integer(dropZoneId));
            zones.add(new Zone(zoneId, proximityList));
        }

        return new Cluster(intermediateCluster.getName(),
                           Utils.asSortedList(survivingNodes),
                           Utils.asSortedList(zones));
    }

    /**
     * Similar to {@link RebalanceUtils#vacateZone(Cluster, int)}, takes the
     * current store definitions in the cluster and creates store definitions
     * with the specified zone effectively dropped.
     * 
     * In order to drop a zone, we adjust the total replication factor and
     * remove zone replication factor for the dropped zone
     * 
     * 
     * @param currentStoreDefs
     * @param dropZoneId
     * @return the adjusted list of store definitions
     */
    public static List<StoreDefinition> dropZone(List<StoreDefinition> currentStoreDefs,
                                                 int dropZoneId) {

        List<StoreDefinition> adjustedStoreDefList = new ArrayList<StoreDefinition>();
        for(StoreDefinition storeDef: currentStoreDefs) {
            HashMap<Integer, Integer> zoneRepFactorMap = storeDef.getZoneReplicationFactor();
            if(!zoneRepFactorMap.containsKey(dropZoneId)) {
                throw new VoldemortException("Store " + storeDef.getName()
                                             + " does not have replication factor for zone "
                                             + dropZoneId);
            }

            StoreDefinitionBuilder adjustedStoreDefBuilder = StoreDefinitionUtils.getBuilderForStoreDef(storeDef);

            if(!storeDef.hasPreferredReads()) {
                adjustedStoreDefBuilder.setPreferredReads(null);
            }
            if(!storeDef.hasPreferredWrites()) {
                adjustedStoreDefBuilder.setPreferredWrites(null);
            }

            // Copy all zone replication factor entries except for dropped zone
            HashMap<Integer, Integer> adjustedZoneRepFactorMap = new HashMap<Integer, Integer>();
            for(Integer zoneId: zoneRepFactorMap.keySet()) {
                if(zoneId != dropZoneId) {
                    adjustedZoneRepFactorMap.put(zoneId, zoneRepFactorMap.get(zoneId));
                }
            }
            adjustedStoreDefBuilder.setZoneReplicationFactor(adjustedZoneRepFactorMap);

            // adjust the replication factor
            int zoneRepFactor = zoneRepFactorMap.get(dropZoneId);
            adjustedStoreDefBuilder.setReplicationFactor(adjustedStoreDefBuilder.getReplicationFactor()
                                                         - zoneRepFactor);

            adjustedStoreDefList.add(adjustedStoreDefBuilder.build());
        }

        return adjustedStoreDefList;
    }

    /**
     * For a particular stealer node find all the primary partitions tuples it
     * will steal.
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
        if(currentCluster.hasNodeWithId(stealNodeId)) {
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

    /**
     * Given the initial and final cluster dumps it into the output directory
     * 
     * @param currentCluster Initial cluster metadata
     * @param finalCluster Final cluster metadata
     * @param outputDirName Output directory where to dump this file
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
     * @param outputDirName Output directory where to dump this file
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
     * Prints a stores xml to a file.
     * 
     * @param outputDirName
     * @param fileName
     * @param list of storeDefs
     */
    public static void dumpStoreDefsToFile(String outputDirName,
                                           String fileName,
                                           List<StoreDefinition> storeDefs) {

        if(outputDirName != null) {
            File outputDir = new File(outputDirName);
            if(!outputDir.exists()) {
                Utils.mkdirs(outputDir);
            }

            try {
                FileUtils.writeStringToFile(new File(outputDirName, fileName),
                                            new StoreDefinitionsMapper().writeStoreList(storeDefs));
            } catch(IOException e) {
                logger.error("IOException during dumpStoreDefsToFile: " + e);
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
     * @param rebalanceTaskPlanList Complete list of partition plans
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
