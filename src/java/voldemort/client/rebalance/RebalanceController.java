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

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class RebalanceController {

    private static final Logger logger = Logger.getLogger(RebalanceController.class);

    private static final DecimalFormat decimalFormatter = new DecimalFormat("#.##");

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

        // Retrieve list of stores + check for that all are consistent
        List<StoreDefinition> storeDefs = RebalanceUtils.getStoreDefinition(targetCluster,
                                                                            adminClient);

        rebalance(currentCluster, targetCluster, storeDefs);
    }

    /**
     * Does basic verification of the metadata + server state. Finally starts
     * the rebalancing.
     * 
     * @param newCurrentCluster The cluster currently used on each node
     * @param targetCluster The desired cluster after rebalance
     * @param storeDefs Stores to rebalance
     */
    public void rebalance(Cluster currentCluster,
                          final Cluster targetCluster,
                          List<StoreDefinition> storeDefs) {

        logger.info("Current cluster : " + currentCluster);
        logger.info("Final target cluster : " + targetCluster);

        // Filter the store definitions to a set rebalancing can support
        storeDefs = RebalanceUtils.validateRebalanceStore(storeDefs);

        // Add all new nodes to a 'new current cluster'
        Cluster newCurrentCluster = RebalanceUtils.getClusterWithNewNodes(currentCluster,
                                                                          targetCluster);

        // Make admin client point to this updated current cluster
        adminClient.setAdminClientCluster(newCurrentCluster);

        // Do some verification
        if(!rebalanceConfig.isShowPlanEnabled()) {

            // Now validate that all the nodes ( new + old ) are in normal state
            RebalanceUtils.validateClusterState(newCurrentCluster, adminClient);

            // Verify all old RO stores exist at version 2
            RebalanceUtils.validateReadOnlyStores(newCurrentCluster, storeDefs, adminClient);

            // Propagate the updated cluster metadata to everyone
            logger.info("Propagating new cluster " + newCurrentCluster + " to all nodes");
            RebalanceUtils.propagateCluster(adminClient, newCurrentCluster);

        }

        rebalancePerClusterTransition(newCurrentCluster, targetCluster, storeDefs);
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
        final Map<Node, List<Integer>> stealerToStolenPrimaryPartitions = new HashMap<Node, List<Integer>>();

        // Generate the number of tasks to compute percent completed
        int numTasks = 0;
        for(Node stealerNode: targetCluster.getNodes()) {
            List<Integer> stolenPrimaryPartitions = RebalanceUtils.getStolenPrimaryPartitions(currentCluster,
                                                                                              targetCluster,
                                                                                              stealerNode.getId());
            numTasks += stolenPrimaryPartitions.size();
            stealerToStolenPrimaryPartitions.put(stealerNode, stolenPrimaryPartitions);
        }

        int taskId = 0;
        for(Node stealerNode: targetCluster.getNodes()) {

            // If not stealing partition, ignore node
            List<Integer> stolenPrimaryPartitions = stealerToStolenPrimaryPartitions.get(stealerNode);

            if(stolenPrimaryPartitions == null || stolenPrimaryPartitions.isEmpty()) {
                RebalanceUtils.printLog(stealerNode.getId(),
                                        logger,
                                        "No primary partitions to steal");
                continue;
            }

            RebalanceUtils.printLog(stealerNode.getId(),
                                    logger,
                                    "All primary partitions to steal = " + stolenPrimaryPartitions);

            Node stealerNodeUpdated = currentCluster.getNodeById(stealerNode.getId());

            // Steal primary partitions one by one while creating transition
            // clusters
            for(Integer donatedPrimaryPartition: stolenPrimaryPartitions) {

                RebalanceUtils.printLog(stealerNode.getId(),
                                        logger,
                                        "Working on moving primary partition "
                                                + donatedPrimaryPartition);

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

                // Print the transition plan
                RebalanceUtils.printLog(stealerNode.getId(),
                                        logger,
                                        orderedClusterTransition.toString());

                rebalancePerPartitionTransition(stealerNode.getId(), orderedClusterTransition);

                currentCluster = transitionCluster;
                taskId++;

                RebalanceUtils.printLog(stealerNode.getId(),
                                        logger,
                                        "Completed tasks = "
                                                + taskId
                                                + ". Percent done = "
                                                + decimalFormatter.format(taskId * 100.0 / numTasks));
            }
        }
    }

    /**
     * Rebalance per partition transition - This does the actual rebalancing
     * work required for a single primary partition move.
     * 
     * <br>
     * 
     * Each operation is split into individual tasks depending on read-only or
     * read-write migration.
     * 
     * 
     * @param globalStealerNodeId The stealer node in picture
     * @param orderedClusterTransition The ordered cluster transition we are
     *        going to run
     */
    // TODO : Make this public and write some tests against it
    private void rebalancePerPartitionTransition(final int globalStealerNodeId,
                                                 final OrderedClusterTransition orderedClusterTransition) {
        try {
            final List<RebalanceNodePlan> rebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();

            if(rebalanceNodePlanList.isEmpty()) {
                RebalanceUtils.printLog(globalStealerNodeId,
                                        logger,
                                        "Skipping rebalance task id "
                                                + orderedClusterTransition.getId()
                                                + " since it is empty");
                return;
            }

            RebalanceUtils.printLog(globalStealerNodeId, logger, "Starting rebalance task id "
                                                                 + orderedClusterTransition.getId());

            // Flatten the node plans to partition plans
            List<RebalancePartitionsInfo> rebalancePartitionPlanList = RebalanceUtils.flattenNodePlans(rebalanceNodePlanList);

            // Split the store definitions
            List<StoreDefinition> readOnlyStoreDefs = RebalanceUtils.filterStores(orderedClusterTransition.getStoreDefs(),
                                                                                  true);
            List<StoreDefinition> readWriteStoreDefs = RebalanceUtils.filterStores(orderedClusterTransition.getStoreDefs(),
                                                                                   false);
            boolean hasReadOnlyStores = readOnlyStoreDefs != null && readOnlyStoreDefs.size() > 0;
            boolean hasReadWriteStores = readWriteStoreDefs != null
                                         && readWriteStoreDefs.size() > 0;

            // STEP 1 - Cluster state change
            boolean finishedReadOnlyPhase = false;
            rebalancePartitionPlanList = RebalanceUtils.updatePartitionPlanWithStores(rebalancePartitionPlanList,
                                                                                      readOnlyStoreDefs);

            if(!rebalanceConfig.isShowPlanEnabled()) {
                rebalanceStateChange(globalStealerNodeId,
                                     orderedClusterTransition.getCurrentCluster(),
                                     orderedClusterTransition.getTargetCluster(),
                                     rebalancePartitionPlanList,
                                     hasReadOnlyStores,
                                     hasReadWriteStores,
                                     finishedReadOnlyPhase);

                // STEP 2 - Move RO data
                if(hasReadOnlyStores) {
                    rebalancePerTaskTransition(globalStealerNodeId,
                                               orderedClusterTransition.getCurrentCluster(),
                                               rebalancePartitionPlanList,
                                               hasReadOnlyStores,
                                               hasReadWriteStores,
                                               finishedReadOnlyPhase);
                }
            }

            // STEP 3 - Cluster change state
            finishedReadOnlyPhase = true;
            rebalancePartitionPlanList = RebalanceUtils.updatePartitionPlanWithStores(rebalancePartitionPlanList,
                                                                                      readWriteStoreDefs);

            if(!rebalanceConfig.isShowPlanEnabled()) {
                rebalanceStateChange(globalStealerNodeId,
                                     orderedClusterTransition.getCurrentCluster(),
                                     orderedClusterTransition.getTargetCluster(),
                                     rebalancePartitionPlanList,
                                     hasReadOnlyStores,
                                     hasReadWriteStores,
                                     finishedReadOnlyPhase);

                // STEP 4 - Move RW data
                if(hasReadWriteStores) {
                    rebalancePerTaskTransition(globalStealerNodeId,
                                               orderedClusterTransition.getCurrentCluster(),
                                               rebalancePartitionPlanList,
                                               hasReadOnlyStores,
                                               hasReadWriteStores,
                                               finishedReadOnlyPhase);
                }
            }

            RebalanceUtils.printLog(globalStealerNodeId,
                                    logger,
                                    "Successfully terminated rebalance task id "
                                            + orderedClusterTransition.getId());

        } catch(Exception e) {
            RebalanceUtils.printErrorLog(globalStealerNodeId,
                                         logger,
                                         "Error in rebalance task id "
                                                 + orderedClusterTransition.getId() + " - "
                                                 + e.getMessage(),
                                         e);
            throw new VoldemortException("Rebalance failed on rebalance task id "
                                         + orderedClusterTransition.getId(), e);
        }

    }

    /**
     * 
     * Perform a group of state change actions. Also any errors + rollback
     * procedures are performed at this level itself.
     * 
     * <pre>
     * | Case | hasRO | hasRW | finishedRO | Action |
     * | 0 | t | t | t | 2nd one ( cluster change + swap + rebalance state change ) |
     * | 1 | t | t | f | 1st one ( rebalance state change ) |
     * | 2 | t | f | t | 2nd one ( cluster change + swap ) |
     * | 3 | t | f | f | 1st one ( rebalance state change ) |
     * | 4 | f | t | t | 2nd one ( cluster change + rebalance state change ) |
     * | 5 | f | t | f | ignore |
     * | 6 | f | f | t | no stores, exception | 
     * | 7 | f | f | f | no stores, exception |
     * </pre>
     * 
     * Truth table, FTW!
     * 
     * @param globalStealerNodeId Global stealer node id we're working on
     * @param currentCluster Current cluster
     * @param transitionCluster Transition cluster to propagate
     * @param rebalancePartitionPlanList List of partition plan list
     * @param hasReadOnlyStores Boolean indicating if read-only stores exist
     * @param hasReadWriteStores Boolean indicating if read-write stores exist
     * @param finishedReadOnlyStores Boolean indicating if we have finished RO
     *        store migration
     */
    private void rebalanceStateChange(final int globalStealerNodeId,
                                      Cluster currentCluster,
                                      Cluster transitionCluster,
                                      List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                      boolean hasReadOnlyStores,
                                      boolean hasReadWriteStores,
                                      boolean finishedReadOnlyStores) {
        try {
            if(!hasReadOnlyStores && !hasReadWriteStores) {
                // Case 6 / 7 - no stores, exception
                throw new VoldemortException("Cannot get this state since it means there are no stores");
            } else if(!hasReadOnlyStores && hasReadWriteStores && !finishedReadOnlyStores) {
                // Case 5 - ignore
                RebalanceUtils.printLog(globalStealerNodeId,
                                        logger,
                                        "Ignoring state change since there are no read-only stores");
            } else if(!hasReadOnlyStores && hasReadWriteStores && finishedReadOnlyStores) {
                // Case 4 - cluster change + rebalance state change
                RebalanceUtils.printLog(globalStealerNodeId,
                                        logger,
                                        "Cluster metadata change + rebalance state change");
                adminClient.rebalanceStateChange(currentCluster,
                                                 transitionCluster,
                                                 rebalancePartitionPlanList,
                                                 false,
                                                 true,
                                                 true);
            } else if(hasReadOnlyStores && !finishedReadOnlyStores) {
                // Case 1 / 3 - rebalance state change
                RebalanceUtils.printLog(globalStealerNodeId, logger, "Rebalance state change");
                adminClient.rebalanceStateChange(currentCluster,
                                                 transitionCluster,
                                                 rebalancePartitionPlanList,
                                                 false,
                                                 false,
                                                 true);
            } else if(hasReadOnlyStores && !hasReadWriteStores && finishedReadOnlyStores) {
                // Case 2 - swap + cluster change
                RebalanceUtils.printLog(globalStealerNodeId,
                                        logger,
                                        "Swap + Cluster metadata change");
                adminClient.rebalanceStateChange(currentCluster,
                                                 transitionCluster,
                                                 rebalancePartitionPlanList,
                                                 true,
                                                 true,
                                                 false);
            } else {
                // Case 0 - swap + cluster change + rebalance state change
                RebalanceUtils.printLog(globalStealerNodeId,
                                        logger,
                                        "Swap + Cluster metadata change + rebalance state change");
                adminClient.rebalanceStateChange(currentCluster,
                                                 transitionCluster,
                                                 rebalancePartitionPlanList,
                                                 true,
                                                 true,
                                                 true);
            }

        } catch(VoldemortRebalancingException e) {
            RebalanceUtils.printErrorLog(globalStealerNodeId,
                                         logger,
                                         "Failure while changing rebalancing state",
                                         e);
            throw e;
        }
    }

    /**
     * The smallest granularity of rebalancing where-in we move partitions for a
     * sub-set of stores. Finally at the end of the movement, the node is
     * removed out of rebalance state
     * 
     * <br>
     * 
     * Also any errors + rollback procedures are performed at this level itself.
     * 
     * <pre>
     * | Case | hasRO | hasRW | finishedRO | Action |
     * | 0 | t | t | t | rollback cluster change + swap + rebalance state change |
     * | 1 | t | t | f | rollback rebalance state change |
     * | 2 | t | f | t | not possible |
     * | 3 | t | f | f | rollback rebalance state change |
     * | 4 | f | t | t | rollback cluster change + rebalance state change |
     * | 5 | f | t | f | not possible |
     * | 6 | f | f | t | not possible | 
     * | 7 | f | f | f | not possible |
     * </pre>
     * 
     * @param globalStealerNodeId The stealer node id in picture
     * @param currentCluster Cluster to rollback to if we have a problem
     * @param rebalancePartitionPlanList The list of rebalance partition plans
     * @param hasReadOnlyStores Are we rebalancing any read-only stores?
     * @param hasReadWriteStores Are we rebalancing any read-write stores?
     * @param finishedReadOnlyStores Have we finished rebalancing of read-only
     *        stores?
     */
    private void rebalancePerTaskTransition(final int globalStealerNodeId,
                                            final Cluster currentCluster,
                                            final List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                            boolean hasReadOnlyStores,
                                            boolean hasReadWriteStores,
                                            boolean finishedReadOnlyStores) {
        // Get an ExecutorService in place used for submitting our tasks
        ExecutorService service = RebalanceUtils.createExecutors(rebalanceConfig.getMaxParallelRebalancing());

        // List of tasks which will run asynchronously
        final List<RebalanceTask> allTasks = Lists.newArrayList();

        // Sub-list of the above list
        final List<RebalanceTask> successfulTasks = Lists.newArrayList();
        final List<RebalanceTask> failedTasks = Lists.newArrayList();

        try {
            executeTasks(service, rebalancePartitionPlanList, allTasks);

            // All tasks submitted.
            RebalanceUtils.printLog(globalStealerNodeId,
                                    logger,
                                    "All rebalance tasks were submitted (shutting down in "
                                            + rebalanceConfig.getRebalancingClientTimeoutSeconds()
                                            + " sec)");

            // Wait and shutdown after timeout
            RebalanceUtils.executorShutDown(service,
                                            rebalanceConfig.getRebalancingClientTimeoutSeconds());

            RebalanceUtils.printLog(globalStealerNodeId, logger, "Finished waiting for executors");

            // Collects all failures from the rebalance tasks.
            List<Exception> failures = filterTasks(allTasks, successfulTasks, failedTasks);

            if(failedTasks.size() > 0) {
                throw new VoldemortRebalancingException("Rebalance task terminated unsuccessfully",
                                                        failures);
            }

        } catch(VoldemortRebalancingException e) {

            logger.error("Failure while migrating partitions for stealer node "
                         + globalStealerNodeId);

            // Rollback state for all successful tasks
            for(RebalanceTask task: successfulTasks) {
                try {
                    adminClient.individualStateChange(task.getRebalancePartitionsInfo()
                                                          .getStealerId(),
                                                      currentCluster,
                                                      Lists.newArrayList(task.getRebalancePartitionsInfo()),
                                                      hasReadWriteStores && hasReadOnlyStores
                                                              && finishedReadOnlyStores,
                                                      hasReadWriteStores && finishedReadOnlyStores,
                                                      (hasReadOnlyStores && hasReadWriteStores && finishedReadOnlyStores)
                                                              || (hasReadOnlyStores
                                                                  && hasReadWriteStores && !finishedReadOnlyStores)
                                                              || (hasReadOnlyStores
                                                                  && !hasReadWriteStores && !finishedReadOnlyStores)
                                                              || (!hasReadOnlyStores
                                                                  && hasReadWriteStores && finishedReadOnlyStores),
                                                      true,
                                                      true);
                } catch(Exception exception) {
                    RebalanceUtils.printErrorLog(globalStealerNodeId,
                                                 logger,
                                                 "Problem while reverting back successful tasks "
                                                         + task.getRebalancePartitionsInfo(),
                                                 exception);
                }
            }

            throw e;

        } finally {
            if(!service.isShutdown()) {
                RebalanceUtils.printErrorLog(globalStealerNodeId,
                                             logger,
                                             "Could not shutdown service cleanly for node "
                                                     + globalStealerNodeId,
                                             null);
                service.shutdownNow();
            }
        }
    }

    /**
     * Filters the rebalance tasks and groups them together + returns list of
     * errors
     * 
     * @param allTasks List of all rebalance tasks
     * @param successfulTasks List of all successful rebalance tasks
     * @param failedTasks List of all failed rebalance tasks
     * @return List of exceptions
     */
    private List<Exception> filterTasks(final List<RebalanceTask> allTasks,
                                        List<RebalanceTask> successfulTasks,
                                        List<RebalanceTask> failedTasks) {
        List<Exception> errors = Lists.newArrayList();
        for(RebalanceTask task: allTasks) {
            if(task.hasException()) {
                failedTasks.add(task);
                errors.add(task.getError());
            } else {
                successfulTasks.add(task);
            }
        }
        return errors;
    }

    private void executeTasks(final ExecutorService service,
                              List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                              List<RebalanceTask> taskList) {
        for(RebalancePartitionsInfo partitionsInfo: rebalancePartitionPlanList) {
            RebalanceTask rebalanceTask = new RebalanceTask(partitionsInfo,
                                                            rebalanceConfig,
                                                            adminClient);
            taskList.add(rebalanceTask);
            service.execute(rebalanceTask);
        }
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }

}
