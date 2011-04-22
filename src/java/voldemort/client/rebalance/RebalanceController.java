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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
        storeDefs = RebalanceUtils.getWritableStores(storeDefs);

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
     * Each transition represents the migration of one primary partition along
     * with all its side effect ( i.e. migration of replicas + deletions )
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
                printLog(stealerNode.getId(), "No partitions to steal");
                continue;
            }

            printLog(stealerNode.getId(), "Number of primary partitions being moved = "
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
                                                                                                       rebalanceClusterPlan);

                printLog(stealerNode.getId(), orderedClusterTransition.toString());

                if(!rebalanceConfig.isShowPlanEnabled()) {
                    rebalance(stealerNode.getId(), orderedClusterTransition);
                }

                currentCluster = transitionCluster;
            }
        }
    }

    public void printLog(int nodeId, String message) {
        logger.info("Stealer node " + Integer.toString(nodeId) + "] " + message);
    }

    private void rebalance(final int stealerNodeId,
                           final OrderedClusterTransition orderedClusterTransition) {
        try {
            final Cluster transitionCluster = orderedClusterTransition.getTargetCluster();
            final List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();

            if(orderedRebalanceNodePlanList.isEmpty()) {
                printLog(stealerNodeId, "Skipping rebalance-task-id:"
                                        + orderedClusterTransition.getId() + " is empty");
                return;
            }

            printLog(stealerNodeId, "Starting rebalance-task-id: " + orderedClusterTransition);

            // Executing the rebalance tasks now.
            rebalance(stealerNodeId, transitionCluster, orderedRebalanceNodePlanList);

            printLog(stealerNodeId, "Successfully terminated rebalance-task-id: "
                                    + orderedClusterTransition.getId());

        } catch(Exception e) {
            logger.error("Error in rebalance-task: " + orderedClusterTransition + " - "
                         + e.getMessage(), e);
            throw new VoldemortException("Rebalance failed on rebalance-task ID#"
                                         + orderedClusterTransition.getId(), e);
        }

    }

    private void rebalance(final int stealerNodeId,
                           final Cluster transitionCluster,
                           final List<RebalanceNodePlan> rebalanceNodePlanList) throws Exception {

        final List<RebalanceTask> primaryTasks = new ArrayList<RebalanceTask>();
        final List<RebalanceTask> replicaTasks = new ArrayList<RebalanceTask>();

        // List of all exceptions
        List<Exception> failures = new ArrayList<Exception>();

        // All stealer and donor nodes
        final Set<Integer> participatingNodesId = Collections.synchronizedSet(new HashSet<Integer>());

        // Tasks with migration of primary partitions will be executed first
        final List<RebalancePartitionsInfo> onlyPrimaries = getTasks(rebalanceNodePlanList, true);
        final List<RebalancePartitionsInfo> onlyReplicas = getTasks(rebalanceNodePlanList, false);

        final CountDownLatch gate = new CountDownLatch(onlyPrimaries.size());
        ExecutorService execPrimaries = executeTasks(participatingNodesId,
                                                     onlyPrimaries,
                                                     rebalanceConfig.getMaxParallelRebalancing(),
                                                     gate,
                                                     primaryTasks);

        // Wait for all the primary tasks to propagate the cluster but not wait
        // for them to complete.
        //
        // In order to guarantee that a partition contains ALL keys sent while
        // rebalancing is in progress you have to guaranteed that any
        // RebalancePartitionsInfo that migrated a "primary"
        // partition happens first than another RebalancePartitionsInfo that
        // just copy "replicas".
        //
        // This is due to the fact that propagation of new cluster is triggered
        // only when a "primary" is moved. If a "replica" is moved first then
        // you will effectively copied the information that was at that moment
        // in the partition but not new incoming PUTs.
        // Only a routing-strategy will make ALL the PUTs goes to the right
        // partitions (primary and replicas) after (an not before) a new
        // cluster.xml is propagated to all nodes.
        try {
            logger.debug("Waiting on the gate");
            gate.await();
            logger.debug("Gate opened");
        } catch(InterruptedException e) {
            failures.add(e);
        }

        final ExecutorService execReplicas = executeTasks(participatingNodesId,
                                                          onlyReplicas,
                                                          rebalanceConfig.getMaxParallelRebalancing(),
                                                          null,
                                                          replicaTasks);

        // All tasks submitted.
        printLog(stealerNodeId, "All rebalance tasks were submitted (shutting down in "
                                + rebalanceConfig.getRebalancingClientTimeoutSeconds() + " sec");

        // Waits and then shutdown primary-executor.
        RebalanceUtils.executorShutDown(execPrimaries,
                                        rebalanceConfig.getRebalancingClientTimeoutSeconds());

        // Waits and then shutdown replicas-executor.
        RebalanceUtils.executorShutDown(execReplicas,
                                        rebalanceConfig.getRebalancingClientTimeoutSeconds());

        if(logger.isInfoEnabled()) {
            logger.info("Finished waiting for executors ");
        }

        // Collects all failures from the rebalance tasks.
        failures = addFailures(failures, primaryTasks);
        failures = addFailures(failures, replicaTasks);

        // If everything successful, swap the read-only stores
        if(failures.size() == 0 && readOnlyStores.size() > 0) {
            logger.info("Swapping stores " + readOnlyStores + " on " + participatingNodesId);
            ExecutorService swapExecutors = RebalanceUtils.createExecutors(transitionCluster.getNumberOfNodes());
            for(final Integer nodeId: participatingNodesId) {
                swapExecutors.submit(new Runnable() {

                    public void run() {
                        Map<String, String> storeDirs = currentROStoreVersionsDirs.get(nodeId);

                        try {
                            logger.info("Swapping read-only stores on node " + nodeId);
                            adminClient.swapStoresAndCleanState(nodeId, storeDirs);
                            logger.info("Successfully swapped on node " + nodeId);
                        } catch(Exception e) {
                            logger.error("Failed swapping on node " + nodeId, e);
                        }

                    }
                });
            }

            try {
                RebalanceUtils.executorShutDown(swapExecutors,
                                                rebalanceConfig.getRebalancingClientTimeoutSeconds());
            } catch(Exception e) {
                logger.error("Interrupted swapping executor ", e);
            }
        }

        if(failures.size() > 0) {
            throw new VoldemortRebalancingException("Rebalance task terminated unsuccessfully");
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

    private ExecutorService executeTasks(final Set<Integer> participatingNodesId,
                                         List<RebalancePartitionsInfo> rebalancePartitionsInfoList,
                                         int numThreads,
                                         CountDownLatch gate,
                                         List<RebalanceTask> taskList) {
        ExecutorService exec = RebalanceUtils.createExecutors(numThreads);
        for(RebalancePartitionsInfo pinfo: rebalancePartitionsInfoList) {
            RebalanceTask myRebalanceTask = new RebalanceTask(pinfo, participatingNodesId, gate);
            taskList.add(myRebalanceTask);
            exec.execute(myRebalanceTask);
        }
        return exec;
    }

    /**
     * Given the list of node plans and a boolean, returns a sub-list which
     * contains either plans with primary movements or plans with only replica
     * movements
     * 
     * @param rebalanceNodePlanList Complete list of rebalance node plan
     * @param primaryOnly Boolean to get only primary based plans
     * @return Sub-list depending on the primaryOnly flag
     */
    private List<RebalancePartitionsInfo> getTasks(List<RebalanceNodePlan> rebalanceNodePlanList,
                                                   boolean primaryOnly) {
        List<RebalancePartitionsInfo> list = new ArrayList<RebalancePartitionsInfo>();
        for(RebalanceNodePlan rebalanceNodePlan: rebalanceNodePlanList) {
            for(final RebalancePartitionsInfo stealInfo: rebalanceNodePlan.getRebalanceTaskList()) {
                List<Integer> stealMasterPartitions = stealInfo.getStealMasterPartitions();
                if(stealMasterPartitions == null) {
                    continue;
                }

                if(!stealMasterPartitions.isEmpty() == primaryOnly) {
                    list.add(stealInfo);
                }
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
