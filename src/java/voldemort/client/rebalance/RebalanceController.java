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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.task.DonorBasedRebalanceTask;
import voldemort.client.rebalance.task.RebalanceTask;
import voldemort.client.rebalance.task.StealerBasedRebalanceTask;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.utils.NodeUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.TreeMultimap;

public class RebalanceController {

    private static final Logger logger = Logger.getLogger(RebalanceController.class);

    private static final DecimalFormat decimalFormatter = new DecimalFormat("#.##");

    private final AdminClient adminClient;
    private final RebalanceClientConfig rebalanceConfig;

    public RebalanceController(String bootstrapUrl, RebalanceClientConfig rebalanceConfig) {
        this.adminClient = new AdminClient(bootstrapUrl, rebalanceConfig, new ClientConfig());
        this.rebalanceConfig = rebalanceConfig;
    }

    public RebalanceController(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new AdminClient(cluster, config, new ClientConfig());
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
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(NodeUtils.getNodeIds(Lists.newArrayList(adminClient.getAdminClientCluster()
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
     * @param currentCluster The cluster currently used on each node
     * @param targetCluster The desired cluster after rebalance
     * @param storeDefs Stores to rebalance
     */
    public void rebalance(Cluster currentCluster,
                          final Cluster targetCluster,
                          List<StoreDefinition> storeDefs) {

        logger.info("Current cluster : " + currentCluster);
        logger.info("Final target cluster : " + targetCluster);
        logger.info("Show plan : " + rebalanceConfig.isShowPlanEnabled());
        logger.info("Delete post rebalancing : "
                    + rebalanceConfig.isDeleteAfterRebalancingEnabled());
        logger.info("Stealer based rebalancing : " + rebalanceConfig.isStealerBasedRebalancing());
        logger.info("Primary partition batch size : "
                    + rebalanceConfig.getPrimaryPartitionBatchSize());

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
        // Mapping of stealer node to list of primary partitions being moved
        final TreeMultimap<Integer, Integer> stealerToStolenPrimaryPartitions = TreeMultimap.create();

        // Creates the same mapping as above for dry run
        final TreeMultimap<Integer, Integer> stealerToStolenPrimaryPartitionsClone = TreeMultimap.create();

        // Various counts for progress bar
        int numTasks = 0;
        int numCrossZoneMoves = 0;
        int numPrimaryPartitionMoves = 0;

        // Used for creating clones
        ClusterMapper mapper = new ClusterMapper();

        // Output initial and final cluster
        if(rebalanceConfig.hasOutputDirectory())
            RebalanceUtils.dumpCluster(currentCluster,
                                       targetCluster,
                                       new File(rebalanceConfig.getOutputDirectory()));

        // Start first dry run to compute the stolen partitions
        for(Node stealerNode: targetCluster.getNodes()) {
            List<Integer> stolenPrimaryPartitions = RebalanceUtils.getStolenPrimaryPartitions(currentCluster,
                                                                                              targetCluster,
                                                                                              stealerNode.getId());

            if(stolenPrimaryPartitions.size() > 0) {
                numPrimaryPartitionMoves += stolenPrimaryPartitions.size();
                stealerToStolenPrimaryPartitions.putAll(stealerNode.getId(),
                                                        stolenPrimaryPartitions);
                stealerToStolenPrimaryPartitionsClone.putAll(stealerNode.getId(),
                                                             stolenPrimaryPartitions);
            }
        }

        // Create a clone for second dry run
        Cluster currentClusterClone = mapper.readCluster(new StringReader(mapper.writeCluster(currentCluster)));

        // Start second dry run to pre-compute other total statistics
        while(!stealerToStolenPrimaryPartitionsClone.isEmpty()) {

            // Generate a snapshot of current initial state
            Cluster startCluster = mapper.readCluster(new StringReader(mapper.writeCluster(currentClusterClone)));

            // Generate batches of partitions and move then over
            int batchCompleted = 0;
            List<Entry<Integer, Integer>> partitionsMoved = Lists.newArrayList();
            for(Entry<Integer, Integer> stealerToPartition: stealerToStolenPrimaryPartitionsClone.entries()) {
                partitionsMoved.add(stealerToPartition);
                currentClusterClone = RebalanceUtils.createUpdatedCluster(currentClusterClone,
                                                                          stealerToPartition.getKey(),
                                                                          Lists.newArrayList(stealerToPartition.getValue()));
                batchCompleted++;

                if(batchCompleted == rebalanceConfig.getPrimaryPartitionBatchSize())
                    break;
            }

            // Remove the partitions moved
            for(Iterator<Entry<Integer, Integer>> partitionMoved = partitionsMoved.iterator(); partitionMoved.hasNext();) {
                Entry<Integer, Integer> entry = partitionMoved.next();
                stealerToStolenPrimaryPartitionsClone.remove(entry.getKey(), entry.getValue());
            }

            // Generate a plan to compute the tasks
            final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(startCluster,
                                                                                       currentClusterClone,
                                                                                       storeDefs,
                                                                                       rebalanceConfig.isDeleteAfterRebalancingEnabled(),
                                                                                       rebalanceConfig.isStealerBasedRebalancing());

            numCrossZoneMoves += RebalanceUtils.getCrossZoneMoves(currentClusterClone,
                                                                  rebalanceClusterPlan);
            numTasks += RebalanceUtils.getTotalMoves(rebalanceClusterPlan);

        }

        logger.info("Total number of primary partition moves : " + numPrimaryPartitionMoves);
        logger.info("Total number of cross zone moves : " + numCrossZoneMoves);
        logger.info("Total number of tasks : " + numTasks);

        int tasksCompleted = 0;
        int batchCounter = 0;
        int primaryPartitionId = 0;
        double totalTimeMs = 0.0;

        // Starting the real run
        while(!stealerToStolenPrimaryPartitions.isEmpty()) {

            Cluster transitionCluster = mapper.readCluster(new StringReader(mapper.writeCluster(currentCluster)));

            // Generate batches of partitions and move then over
            int primaryPartitionBatchSize = 0;
            List<Entry<Integer, Integer>> partitionsMoved = Lists.newArrayList();
            for(Entry<Integer, Integer> stealerToPartition: stealerToStolenPrimaryPartitions.entries()) {
                partitionsMoved.add(stealerToPartition);
                transitionCluster = RebalanceUtils.createUpdatedCluster(transitionCluster,
                                                                        stealerToPartition.getKey(),
                                                                        Lists.newArrayList(stealerToPartition.getValue()));
                primaryPartitionBatchSize++;

                if(primaryPartitionBatchSize == rebalanceConfig.getPrimaryPartitionBatchSize())
                    break;
            }
            batchCounter++;

            // Remove the partitions moved + Prepare message to print
            StringBuffer buffer = new StringBuffer();
            buffer.append("Partitions being moved : ");
            for(Iterator<Entry<Integer, Integer>> partitionMoved = partitionsMoved.iterator(); partitionMoved.hasNext();) {
                Entry<Integer, Integer> entry = partitionMoved.next();
                buffer.append("[ partition " + entry.getValue() + " to stealer node "
                              + entry.getKey() + " ], ");
                stealerToStolenPrimaryPartitions.remove(entry.getKey(), entry.getValue());
            }
            final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                                                                                       transitionCluster,
                                                                                       storeDefs,
                                                                                       rebalanceConfig.isDeleteAfterRebalancingEnabled(),
                                                                                       rebalanceConfig.isStealerBasedRebalancing());

            final OrderedClusterTransition orderedClusterTransition = new OrderedClusterTransition(currentCluster,
                                                                                                   transitionCluster,
                                                                                                   storeDefs,
                                                                                                   rebalanceClusterPlan);

            // Print message about what is being moved
            logger.info("----------------");
            RebalanceUtils.printLog(orderedClusterTransition.getId(), logger, buffer.toString());

            // Print the transition plan
            RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                    logger,
                                    orderedClusterTransition.toString());

            // Output the transition plan to the output directory
            if(rebalanceConfig.hasOutputDirectory())
                RebalanceUtils.dumpCluster(currentCluster,
                                           transitionCluster,
                                           new File(rebalanceConfig.getOutputDirectory()),
                                           "batch-" + Integer.toString(batchCounter) + ".");

            long startTimeMs = System.currentTimeMillis();
            rebalancePerPartitionTransition(orderedClusterTransition);
            totalTimeMs += (System.currentTimeMillis() - startTimeMs);

            // Update the current cluster
            currentCluster = transitionCluster;

            // Bump up the statistics
            tasksCompleted += RebalanceUtils.getTotalMoves(rebalanceClusterPlan);
            primaryPartitionId += primaryPartitionBatchSize;

            // Calculate the estimated end time
            double estimatedTimeMs = (totalTimeMs / tasksCompleted) * (numTasks - tasksCompleted);

            RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                    logger,
                                    "Completed tasks - "
                                            + tasksCompleted
                                            + ". Percent done - "
                                            + decimalFormatter.format(tasksCompleted * 100.0
                                                                      / numTasks));

            RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                    logger,
                                    "Primary partitions left to move - "
                                            + (numPrimaryPartitionMoves - primaryPartitionId));

            RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                    logger,
                                    "Estimated time left for completion - " + estimatedTimeMs
                                            + " ms ( " + estimatedTimeMs / Time.MS_PER_HOUR
                                            + " hours )");

        }

    }

    /**
     * Rebalance per partition transition - This does the actual rebalancing
     * work required for a single primary partition move.
     * 
     * <br>
     * 
     * Each operation is split into individual tasks depending on read-only or
     * read-write migration. Read-only store migration is done first to avoid
     * the overhead of redirecting stores
     * 
     * @param orderedClusterTransition The ordered cluster transition we are
     *        going to run
     */
    private void rebalancePerPartitionTransition(final OrderedClusterTransition orderedClusterTransition) {
        try {
            final List<RebalancePartitionsInfo> rebalancePartitionsInfoList = orderedClusterTransition.getOrderedRebalancePartitionsInfoList();

            if(rebalancePartitionsInfoList.isEmpty()) {
                RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                        logger,
                                        "Skipping rebalance task id "
                                                + orderedClusterTransition.getId()
                                                + " since it is empty");
                return;
            }

            RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                    logger,
                                    "Starting rebalance task id "
                                            + orderedClusterTransition.getId());

            // Flatten the node plans to partition plans
            List<RebalancePartitionsInfo> rebalancePartitionPlanList = rebalancePartitionsInfoList;

            // Split the store definitions
            List<StoreDefinition> readOnlyStoreDefs = StoreDefinitionUtils.filterStores(orderedClusterTransition.getStoreDefs(),
                                                                                        true);
            List<StoreDefinition> readWriteStoreDefs = StoreDefinitionUtils.filterStores(orderedClusterTransition.getStoreDefs(),
                                                                                         false);
            boolean hasReadOnlyStores = readOnlyStoreDefs != null && readOnlyStoreDefs.size() > 0;
            boolean hasReadWriteStores = readWriteStoreDefs != null
                                         && readWriteStoreDefs.size() > 0;

            // STEP 1 - Cluster state change
            boolean finishedReadOnlyPhase = false;
            List<RebalancePartitionsInfo> filteredRebalancePartitionPlanList = RebalanceUtils.filterPartitionPlanWithStores(rebalancePartitionPlanList,
                                                                                                                            readOnlyStoreDefs);

            rebalanceStateChange(orderedClusterTransition.getId(),
                                 orderedClusterTransition.getCurrentCluster(),
                                 orderedClusterTransition.getTargetCluster(),
                                 filteredRebalancePartitionPlanList,
                                 hasReadOnlyStores,
                                 hasReadWriteStores,
                                 finishedReadOnlyPhase);

            // STEP 2 - Move RO data
            if(hasReadOnlyStores) {
                rebalancePerTaskTransition(orderedClusterTransition.getId(),
                                           orderedClusterTransition.getCurrentCluster(),
                                           filteredRebalancePartitionPlanList,
                                           hasReadOnlyStores,
                                           hasReadWriteStores,
                                           finishedReadOnlyPhase);
            }

            // STEP 3 - Cluster change state
            finishedReadOnlyPhase = true;
            filteredRebalancePartitionPlanList = RebalanceUtils.filterPartitionPlanWithStores(rebalancePartitionPlanList,
                                                                                              readWriteStoreDefs);

            rebalanceStateChange(orderedClusterTransition.getId(),
                                 orderedClusterTransition.getCurrentCluster(),
                                 orderedClusterTransition.getTargetCluster(),
                                 filteredRebalancePartitionPlanList,
                                 hasReadOnlyStores,
                                 hasReadWriteStores,
                                 finishedReadOnlyPhase);

            // STEP 4 - Move RW data
            if(hasReadWriteStores) {
                rebalancePerTaskTransition(orderedClusterTransition.getId(),
                                           orderedClusterTransition.getCurrentCluster(),
                                           filteredRebalancePartitionPlanList,
                                           hasReadOnlyStores,
                                           hasReadWriteStores,
                                           finishedReadOnlyPhase);
            }

            RebalanceUtils.printLog(orderedClusterTransition.getId(),
                                    logger,
                                    "Successfully terminated rebalance task id "
                                            + orderedClusterTransition.getId());

        } catch(Exception e) {
            RebalanceUtils.printErrorLog(orderedClusterTransition.getId(),
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
     * @param taskId Rebalancing task id
     * @param currentCluster Current cluster
     * @param transitionCluster Transition cluster to propagate
     * @param rebalancePartitionPlanList List of partition plan list
     * @param hasReadOnlyStores Boolean indicating if read-only stores exist
     * @param hasReadWriteStores Boolean indicating if read-write stores exist
     * @param finishedReadOnlyStores Boolean indicating if we have finished RO
     *        store migration
     */
    private void rebalanceStateChange(final int taskId,
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
                RebalanceUtils.printLog(taskId,
                                        logger,
                                        "Ignoring state change since there are no read-only stores");
            } else if(!hasReadOnlyStores && hasReadWriteStores && finishedReadOnlyStores) {
                // Case 4 - cluster change + rebalance state change
                RebalanceUtils.printLog(taskId,
                                        logger,
                                        "Cluster metadata change + rebalance state change");
                if(!rebalanceConfig.isShowPlanEnabled())
                    adminClient.rebalanceOps.rebalanceStateChange(currentCluster,
                                                                  transitionCluster,
                                                                  rebalancePartitionPlanList,
                                                                  false,
                                                                  true,
                                                                  true,
                                                                  true,
                                                                  true);
            } else if(hasReadOnlyStores && !finishedReadOnlyStores) {
                // Case 1 / 3 - rebalance state change
                RebalanceUtils.printLog(taskId, logger, "Rebalance state change");
                if(!rebalanceConfig.isShowPlanEnabled())
                    adminClient.rebalanceOps.rebalanceStateChange(currentCluster,
                                                                  transitionCluster,
                                                                  rebalancePartitionPlanList,
                                                                  false,
                                                                  false,
                                                                  true,
                                                                  true,
                                                                  true);
            } else if(hasReadOnlyStores && !hasReadWriteStores && finishedReadOnlyStores) {
                // Case 2 - swap + cluster change
                RebalanceUtils.printLog(taskId, logger, "Swap + Cluster metadata change");
                if(!rebalanceConfig.isShowPlanEnabled())
                    adminClient.rebalanceOps.rebalanceStateChange(currentCluster,
                                                                  transitionCluster,
                                                                  rebalancePartitionPlanList,
                                                                  true,
                                                                  true,
                                                                  false,
                                                                  true,
                                                                  true);
            } else {
                // Case 0 - swap + cluster change + rebalance state change
                RebalanceUtils.printLog(taskId,
                                        logger,
                                        "Swap + Cluster metadata change + rebalance state change");
                if(!rebalanceConfig.isShowPlanEnabled())
                    adminClient.rebalanceOps.rebalanceStateChange(currentCluster,
                                                                  transitionCluster,
                                                                  rebalancePartitionPlanList,
                                                                  true,
                                                                  true,
                                                                  true,
                                                                  true,
                                                                  true);
            }

        } catch(VoldemortRebalancingException e) {
            RebalanceUtils.printErrorLog(taskId,
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
     * | 0 | t | t | t | rollback cluster change + swap |
     * | 1 | t | t | f | nothing to do since "rebalance state change" should have removed everything |
     * | 2 | t | f | t | won't be triggered since hasRW is false |
     * | 3 | t | f | f | nothing to do since "rebalance state change" should have removed everything |
     * | 4 | f | t | t | rollback cluster change |
     * | 5 | f | t | f | won't be triggered |
     * | 6 | f | f | t | won't be triggered | 
     * | 7 | f | f | f | won't be triggered |
     * </pre>
     * 
     * @param taskId Rebalance task id
     * @param currentCluster Cluster to rollback to if we have a problem
     * @param rebalancePartitionPlanList The list of rebalance partition plans
     * @param hasReadOnlyStores Are we rebalancing any read-only stores?
     * @param hasReadWriteStores Are we rebalancing any read-write stores?
     * @param finishedReadOnlyStores Have we finished rebalancing of read-only
     *        stores?
     */
    private void rebalancePerTaskTransition(final int taskId,
                                            final Cluster currentCluster,
                                            final List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                            boolean hasReadOnlyStores,
                                            boolean hasReadWriteStores,
                                            boolean finishedReadOnlyStores) {
        RebalanceUtils.printLog(taskId, logger, "Submitting rebalance tasks ");

        // If only show plan, done!
        if(rebalanceConfig.isShowPlanEnabled()) {
            return;
        }

        // Get an ExecutorService in place used for submitting our tasks
        ExecutorService service = RebalanceUtils.createExecutors(rebalanceConfig.getMaxParallelRebalancing());

        // Sub-list of the above list
        final List<RebalanceTask> failedTasks = Lists.newArrayList();
        final List<RebalanceTask> incompleteTasks = Lists.newArrayList();

        // Semaphores for donor nodes - To avoid multiple disk sweeps
        Semaphore[] donorPermits = new Semaphore[currentCluster.getNumberOfNodes()];
        for(Node node: currentCluster.getNodes()) {
            donorPermits[node.getId()] = new Semaphore(1);
        }

        try {
            // List of tasks which will run asynchronously
            List<RebalanceTask> allTasks = executeTasks(taskId,
                                                        service,
                                                        rebalancePartitionPlanList,
                                                        donorPermits);

            // All tasks submitted.
            RebalanceUtils.printLog(taskId,
                                    logger,
                                    "All rebalance tasks were submitted ( shutting down in "
                                            + rebalanceConfig.getRebalancingClientTimeoutSeconds()
                                            + " sec )");

            // Wait and shutdown after timeout
            RebalanceUtils.executorShutDown(service,
                                            rebalanceConfig.getRebalancingClientTimeoutSeconds());

            RebalanceUtils.printLog(taskId, logger, "Finished waiting for executors");

            // Collects all failures + incomplete tasks from the rebalance
            // tasks.
            List<Exception> failures = Lists.newArrayList();
            for(RebalanceTask task: allTasks) {
                if(task.hasException()) {
                    failedTasks.add(task);
                    failures.add(task.getError());
                } else if(!task.isComplete()) {
                    incompleteTasks.add(task);
                }
            }

            if(failedTasks.size() > 0) {
                throw new VoldemortRebalancingException("Rebalance task terminated unsuccessfully on tasks "
                                                                + failedTasks,
                                                        failures);
            }

            // If there were no failures, then we could have had a genuine
            // timeout ( Rebalancing took longer than the operator expected ).
            // We should throw a VoldemortException and not a
            // VoldemortRebalancingException ( which will start reverting
            // metadata ). The operator may want to manually then resume the
            // process.
            if(incompleteTasks.size() > 0) {
                throw new VoldemortException("Rebalance tasks are still incomplete / running "
                                             + incompleteTasks);
            }

        } catch(VoldemortRebalancingException e) {

            logger.error("Failure while migrating partitions for rebalance task " + taskId);

            if(hasReadOnlyStores && hasReadWriteStores && finishedReadOnlyStores) {
                // Case 0
                adminClient.rebalanceOps.rebalanceStateChange(null,
                                                              currentCluster,
                                                              null,
                                                              true,
                                                              true,
                                                              false,
                                                              false,
                                                              false);
            } else if(hasReadWriteStores && finishedReadOnlyStores) {
                // Case 4
                adminClient.rebalanceOps.rebalanceStateChange(null,
                                                              currentCluster,
                                                              null,
                                                              false,
                                                              true,
                                                              false,
                                                              false,
                                                              false);
            }

            throw e;

        } finally {
            if(!service.isShutdown()) {
                RebalanceUtils.printErrorLog(taskId,
                                             logger,
                                             "Could not shutdown service cleanly for rebalance task "
                                                     + taskId,
                                             null);
                service.shutdownNow();
            }
        }
    }

    private List<RebalanceTask> executeTasks(final int taskId,
                                             final ExecutorService service,
                                             List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                             Semaphore[] donorPermits) {
        List<RebalanceTask> taskList = Lists.newArrayList();
        if(rebalanceConfig.isStealerBasedRebalancing()) {
            for(RebalancePartitionsInfo partitionsInfo: rebalancePartitionPlanList) {
                StealerBasedRebalanceTask rebalanceTask = new StealerBasedRebalanceTask(taskId,
                                                                                        partitionsInfo,
                                                                                        rebalanceConfig,
                                                                                        donorPermits[partitionsInfo.getDonorId()],
                                                                                        adminClient);
                taskList.add(rebalanceTask);
                service.execute(rebalanceTask);
            }
        } else {
            // Group by donor nodes
            HashMap<Integer, List<RebalancePartitionsInfo>> donorNodeBasedPartitionsInfo = RebalanceUtils.groupPartitionsInfoByNode(rebalancePartitionPlanList,
                                                                                                                                    false);
            for(Entry<Integer, List<RebalancePartitionsInfo>> entries: donorNodeBasedPartitionsInfo.entrySet()) {
                try {
                    Thread.sleep(10000);
                } catch(InterruptedException e) {}
                DonorBasedRebalanceTask rebalanceTask = new DonorBasedRebalanceTask(taskId,
                                                                                    entries.getValue(),
                                                                                    rebalanceConfig,
                                                                                    donorPermits[entries.getValue()
                                                                                                        .get(0)
                                                                                                        .getDonorId()],
                                                                                    adminClient);
                taskList.add(rebalanceTask);
                service.execute(rebalanceTask);
            }
        }
        return taskList;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.close();
    }

}
