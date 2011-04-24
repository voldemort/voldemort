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

package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RebalanceController {

    private static final Logger logger = Logger.getLogger(RebalanceController.class);

    private final AdminClient adminClient;
    private final RebalanceClientConfig rebalanceConfig;

    public RebalanceController(String bootstrapUrl, RebalanceClientConfig rebalanceConfig) {
        this.adminClient = new AdminClient(bootstrapUrl, rebalanceConfig);
        this.rebalanceConfig = rebalanceConfig;
    }

    public RebalanceController(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new AdminClient(cluster, config);
        this.rebalanceConfig = config;
    }

    /**
     * Grabs the latest cluster definition
     * {@link #rebalance(voldemort.cluster.Cluster, voldemort.cluster.Cluster)}
     * 
     * @param targetCluster Target cluster metadata
     */
    public void rebalance(final Cluster targetCluster) {

        // Retrieve the latest cluster metadata from the existing nodes
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(RebalanceUtils.getNodeIds(Lists.newArrayList(adminClient.getAdminClientCluster()
                                                                                                                                             .getNodes())),
                                                                                     adminClient);
        Cluster currentCluster = currentVersionedCluster.getValue();

        // Start the rebalance with the current cluster + target cluster
        rebalance(currentCluster, targetCluster);
    }

    /**
     * Grabs the store definition and calls
     * {@link #rebalance(voldemort.cluster.Cluster, voldemort.cluster.Cluster, java.util.List)}
     * 
     * @param currentCluster Current cluster metadata
     * @param targetCluster Target cluster metadata
     */
    public void rebalance(Cluster currentCluster, final Cluster targetCluster) {

        // Make admin client point to this updated current cluster
        adminClient.setAdminClientCluster(targetCluster);

        // Retrieve list of stores + check for correctness
        List<StoreDefinition> storeDefs = RebalanceUtils.getStoreDefinition(targetCluster,
                                                                            adminClient);

        rebalance(currentCluster, targetCluster, storeDefs);
    }

    /**
     * Does basic verification of the metadata + server state. Finally starts
     * the rebalancing.
     * 
     * @param currentCluster The cluster currently used on each node
     * @param targetCluster The desired cluster after rebalance
     * @param storeDefs Stores to rebalance
     */
    public void rebalance(Cluster currentCluster,
                          final Cluster targetCluster,
                          List<StoreDefinition> storeDefs) {

        logger.info("Current Cluster configuration:" + currentCluster);
        logger.info("Target Cluster configuration:" + targetCluster);

        // Add all new nodes to current cluster
        currentCluster = RebalanceUtils.getClusterWithNewNodes(currentCluster, targetCluster);

        // Make admin client point to this updated current cluster
        adminClient.setAdminClientCluster(currentCluster);

        // Filter the store definitions to a set rebalancing can support
        storeDefs = RebalanceUtils.getRebalanceStores(storeDefs);

        // Do some verification
        if(!rebalanceConfig.isShowPlanEnabled()) {

            // Now validate that all the nodes ( new + old ) are in normal state
            RebalanceUtils.validateClusterState(currentCluster, adminClient);

            // Verify all old RO stores exist at version 2
            RebalanceUtils.validateReadOnlyStores(currentCluster, storeDefs, adminClient);

            // Propagate the updated cluster metadata to everyone
            List<Integer> nodeIds = RebalanceUtils.getNodeIds(Lists.newArrayList(currentCluster.getNodes()));
            VectorClock latestClock = (VectorClock) RebalanceUtils.getLatestCluster(nodeIds,
                                                                                    adminClient)
                                                                  .getVersion();
            RebalanceUtils.propagateCluster(adminClient,
                                            currentCluster,
                                            latestClock.incremented(0, System.currentTimeMillis()),
                                            nodeIds);
        }

        rebalancePerClusterTransition(currentCluster, targetCluster, storeDefs);
    }

    /**
     * Rebalance on a step-by-step transitions from cluster.xml to
     * target-cluster.xml
     * 
     * <br>
     * 
     * Each transition represents the migration of one primary partition (
     * {@link #rebalancePerPartitionTransition(int, OrderedClusterTransition)} )
     * along with all its side effect ( i.e. migration of replicas + deletions
     * ).
     * 
     * 
     * @param currentCluster The normalized cluster. This cluster contains new
     *        nodes with empty partitions as well
     * @param targetCluster The desired cluster after rebalance
     * @param storeDefs Stores to rebalance
     */
    private void rebalancePerClusterTransition(Cluster currentCluster,
                                               final Cluster targetCluster,
                                               final List<StoreDefinition> storeDefs) {
        final Map<Node, Set<Integer>> stealerToStolenPrimaryPartitions = new HashMap<Node, Set<Integer>>();

        for(Node stealerNode: targetCluster.getNodes()) {
            stealerToStolenPrimaryPartitions.put(stealerNode,
                                                 RebalanceUtils.getStolenPrimaries(currentCluster,
                                                                                   targetCluster,
                                                                                   stealerNode.getId()));
        }

        for(Node stealerNode: targetCluster.getNodes()) {
            // Checks if this node is stealing partition. If not, go to the next
            // node
            Set<Integer> stolenPrimaryPartitions = stealerToStolenPrimaryPartitions.get(stealerNode);
            if(stolenPrimaryPartitions == null || stolenPrimaryPartitions.isEmpty()) {
                RebalanceUtils.printLog(stealerNode.getId(), logger, "No partitions to steal");
                continue;
            }

            RebalanceUtils.printLog(stealerNode.getId(),
                                    logger,
                                    "Number of primary partitions being moved = "
                                            + stolenPrimaryPartitions.size() + " - "
                                            + stolenPrimaryPartitions);
            Node stealerNodeUpdated = currentCluster.getNodeById(stealerNode.getId());

            // Provision stolen primary partitions one by one.
            // Creates a transition cluster for each added partition.
            for(Integer donatedPrimaryPartition: stolenPrimaryPartitions) {
                Cluster transitionCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                                stealerNodeUpdated,
                                                                                donatedPrimaryPartition);
                stealerNodeUpdated = transitionCluster.getNodeById(stealerNodeUpdated.getId());

                final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                                                                                           transitionCluster,
                                                                                           storeDefs,
                                                                                           rebalanceConfig.isDeleteAfterRebalancingEnabled());

                final OrderedClusterTransition orderedClusterTransition = new OrderedClusterTransition(currentCluster,
                                                                                                       transitionCluster,
                                                                                                       storeDefs,
                                                                                                       rebalanceClusterPlan);

                RebalanceUtils.printLog(stealerNode.getId(),
                                        logger,
                                        orderedClusterTransition.toString());

                if(!rebalanceConfig.isShowPlanEnabled()) {
                    rebalancePerPartitionTransition(stealerNode.getId(), orderedClusterTransition);
                }

                currentCluster = transitionCluster;
            }
        }
    }

    /**
     * Rebalance per partition transition - This does the actual rebalancing
     * work required for a single primary partition move.
     * 
     * <br>
     * 
     * Each operation is split into individual tasks (
     * {@link #rebalancePerTaskTransition(int, List, List, boolean)}) depending
     * on read-only or read-write migration.
     * 
     * 
     * @param stealerNodeId The stealer node in picture
     * @param orderedClusterTransition The ordered cluster transition we are
     *        going to run
     */
    private void rebalancePerPartitionTransition(final int stealerNodeId,
                                                 final OrderedClusterTransition orderedClusterTransition) {
        try {
            // final Cluster transitionCluster =
            // orderedClusterTransition.getTargetCluster();
            final List<RebalanceNodePlan> rebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();

            if(rebalanceNodePlanList.isEmpty()) {
                RebalanceUtils.printLog(stealerNodeId, logger, "Skipping rebalance-task-id:"
                                                               + orderedClusterTransition.getId()
                                                               + " is empty");
                return;
            }

            RebalanceUtils.printLog(stealerNodeId, logger, "Starting rebalance-task-id: "
                                                           + orderedClusterTransition);

            // Flatten the node plans to partition plans
            final List<RebalancePartitionsInfo> rebalancePartitionPlanList = getPartitionPlans(rebalanceNodePlanList);

            // Get the nodes being affected
            // final Set<Integer> affectedNodes =
            // getAffectedNodes(rebalancePartitionPlanList);

            // Split the store definitions
            List<StoreDefinition> readOnlyStoreDefs = RebalanceUtils.filterStores(orderedClusterTransition.getStoreDefs(),
                                                                                  true);
            List<StoreDefinition> readWriteStoreDefs = RebalanceUtils.filterStores(orderedClusterTransition.getStoreDefs(),
                                                                                   false);
            boolean hasReadOnlyStores = readOnlyStoreDefs != null && readOnlyStoreDefs.size() > 0;
            boolean hasReadWriteStores = readWriteStoreDefs != null
                                         && readWriteStoreDefs.size() > 0;

            // Read-only store exists? Move it
            if(hasReadOnlyStores) {
                rebalancePerTaskTransition(stealerNodeId,
                                           rebalancePartitionPlanList,
                                           readOnlyStoreDefs,
                                           true);
            }

            // a) Swap ( if RO exists )
            // b) Move into rebalancing state ( if RW exists )
            // c) Update metadata ( RW + RO )

            // Read-write store exists? Move it
            if(hasReadWriteStores) {
                rebalancePerTaskTransition(stealerNodeId,
                                           rebalancePartitionPlanList,
                                           readWriteStoreDefs,
                                           false);
            }

            RebalanceUtils.printLog(stealerNodeId,
                                    logger,
                                    "Successfully terminated rebalance-task-id: "
                                            + orderedClusterTransition.getId());

        } catch(Exception e) {
            logger.error("Error in rebalance-task: " + orderedClusterTransition + " - "
                         + e.getMessage(), e);
            throw new VoldemortException("Rebalance failed on rebalance-task-id: "
                                         + orderedClusterTransition.getId(), e);
        }

    }

    /**
     * The smallest granularity of rebalancing where-in we move partitions for a
     * sub-set of stores
     * 
     * @param stealerNodeId The stealer node id
     * @param rebalancePartitionPlanList The list of rebalance partition plans
     * @param storeDefs List of store definitions being moved
     * @param isReadOnly Are these read-only stores?
     */
    private void rebalancePerTaskTransition(final int stealerNodeId,
                                            final List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                            final List<StoreDefinition> storeDefs,
                                            final boolean isReadOnly) {
        // Get an ExecutorService in place used for submitting our tasks
        ExecutorService service = RebalanceUtils.createExecutors(rebalanceConfig.getMaxParallelRebalancing());

        try {
            // List of tasks which will run asynchronously
            final List<RebalanceTask> tasks = new ArrayList<RebalanceTask>();

            // List of all exceptions
            List<Exception> failures = new ArrayList<Exception>();

            executeTasks(service, rebalancePartitionPlanList, storeDefs, tasks, isReadOnly);

            // All tasks submitted.
            RebalanceUtils.printLog(stealerNodeId,
                                    logger,
                                    "All rebalance tasks were submitted (shutting down in "
                                            + rebalanceConfig.getRebalancingClientTimeoutSeconds()
                                            + " sec)");

            // Wait and shutdown after timeout
            RebalanceUtils.executorShutDown(service,
                                            rebalanceConfig.getRebalancingClientTimeoutSeconds());

            RebalanceUtils.printLog(stealerNodeId, logger, "Finished waiting for executors");

            // Collects all failures from the rebalance tasks.
            failures = addFailures(failures, tasks);

            if(failures.size() > 0) {
                throw new VoldemortRebalancingException("Rebalance task terminated unsuccessfully",
                                                        failures);
            }
        } finally {
            if(!service.isShutdown()) {
                logger.error("Could not shutdown service cleanly for node " + stealerNodeId
                             + " with " + (isReadOnly ? "read-only" : "read-write") + " stores");
                service.shutdownNow();
            }
        }
    }

    private List<Exception> addFailures(final List<Exception> failures,
                                        final List<RebalanceTask> rebalanceTaskList) {
        for(RebalanceTask rebalanceTask: rebalanceTaskList) {
            if(rebalanceTask.hasErrors()) {
                failures.addAll(rebalanceTask.getExceptions());
            }
        }
        return failures;
    }

    private void executeTasks(final ExecutorService service,
                              List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                              List<StoreDefinition> storeDefs,
                              List<RebalanceTask> taskList,
                              boolean isReadOnly) {
        for(RebalancePartitionsInfo partitionsInfo: rebalancePartitionPlanList) {
            RebalanceTask rebalanceTask = new RebalanceTask(partitionsInfo,
                                                            storeDefs,
                                                            rebalanceConfig,
                                                            adminClient,
                                                            isReadOnly);
            taskList.add(rebalanceTask);
            service.execute(rebalanceTask);
        }
    }

    /**
     * Returns set of nodes which are taking part in this plan
     * 
     * @param partitionPlans List of partition plans
     * @return Returns a set of ids of nodes which are part of the plan
     */
    private Set<Integer> getAffectedNodes(List<RebalancePartitionsInfo> partitionPlans) {
        Set<Integer> affectedNodes = Sets.newTreeSet();
        for(RebalancePartitionsInfo partitionPlan: partitionPlans) {
            affectedNodes.add(partitionPlan.getStealerId());
            affectedNodes.add(partitionPlan.getDonorId());
        }
        return affectedNodes;
    }

    /**
     * Given a list of node plans flattens it into a list of partitions info
     * 
     * @param rebalanceNodePlanList Complete list of rebalance node plan
     * @return Flattened list of partition plans
     */
    private List<RebalancePartitionsInfo> getPartitionPlans(List<RebalanceNodePlan> rebalanceNodePlanList) {
        List<RebalancePartitionsInfo> list = new ArrayList<RebalancePartitionsInfo>();
        for(RebalanceNodePlan rebalanceNodePlan: rebalanceNodePlanList) {
            for(final RebalancePartitionsInfo stealInfo: rebalanceNodePlan.getRebalanceTaskList()) {
                list.add(stealInfo);
            }
        }
        return list;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }

}
