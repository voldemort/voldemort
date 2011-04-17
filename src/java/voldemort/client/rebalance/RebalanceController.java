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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalanceController {

    private static final int MAX_TRIES = 2;
    private static final String NL = System.getProperty("line.separator");
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
     * Grabs the latest cluster definition and calls
     * {@link #rebalance(voldemort.cluster.Cluster, voldemort.cluster.Cluster)}
     * 
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final Cluster targetCluster) {

        // Retrieve the latest cluster metadata from the existing nodes
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(RebalanceUtils.getNodeIds(Lists.newArrayList(adminClient.getAdminClientCluster()
                                                                                                                                             .getNodes())),
                                                                                     adminClient);
        Cluster currentCluster = currentVersionedCluster.getValue();
        validateClusterState(currentCluster);
        rebalance(currentCluster, targetCluster);
    }

    /**
     * Check the execution state of the server by checking the
     * {@link MetadataStore}. Rebalance logic requires that for any
     * rebalancing-task to be executed it is first written into the metadata for
     * tracking purposes. This is because in case of a server crash the
     * information can be read back from the metadata store and resumed.
     * 
     * <br>
     * 
     * This function checks if the nodes are all in normal state (
     * {@link VoldemortState#NORMAL_SERVER}).
     * 
     * <br>
     * All possible states are :
     * <ol>
     * 
     * <li> {@link VoldemortState#NORMAL_SERVER} = Server in normal execution
     * mode.
     * 
     * <li> {@link VoldemortState#REBALANCING_MASTER_SERVER} = Server in
     * rebalance mode.
     * 
     * <li> {@link VoldemortState#GRANDFATHERING_SERVER} = Server in
     * grandfathering mode.
     * 
     * </ol>
     * 
     * @param cluster Cluster metadata whose nodes we are checking
     * @throws VoldemortRebalancingException if any node is not in normal state
     */
    private void validateClusterState(final Cluster cluster) {
        for(Node node: cluster.getNodes()) {
            Versioned<String> versioned = adminClient.getRemoteMetadata(node.getId(),
                                                                        MetadataStore.SERVER_STATE_KEY);

            if(!VoldemortState.NORMAL_SERVER.name().equals(versioned.getValue())) {
                throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                        + node.getId() + " (" + node.getHost()
                                                        + ") is not in normal state, but in "
                                                        + versioned.getValue());
            } else {
                if(logger.isInfoEnabled()) {
                    logger.info("Node " + node.getId() + " (" + node.getHost()
                                + ") is ready for rebalance.");
                }
            }
        }
    }

    /**
     * Voldemort dynamic cluster membership rebalancing mechanism. <br>
     * Migrate partitions across nodes to manage changes in cluster membership. <br>
     * Takes target cluster as parameter, fetches the current cluster
     * configuration from the cluster, compares and makes a list of partitions
     * that eed to be transferred.<br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}
     * 
     * @param currentCluster: current cluster configuration
     * @param targetCluster: target cluster configuration
     */
    public void rebalance(Cluster currentCluster, final Cluster targetCluster) {
        if(logger.isDebugEnabled()) {
            logger.debug("Current Cluster configuration:" + currentCluster);
            logger.debug("Target Cluster configuration:" + targetCluster);
        }

        adminClient.setAdminClientCluster(currentCluster);

        // Retrieve list of stores
        List<StoreDefinition> storesList = RebalanceUtils.getStoreNameList(currentCluster,
                                                                           adminClient);

        // Add all new nodes to currentCluster
        currentCluster = getClusterWithNewNodes(currentCluster, targetCluster);
        adminClient.setAdminClientCluster(currentCluster);

        // Maintain nodeId to map of RO store name to current version dirs
        final Map<Integer, Map<String, String>> currentROStoreVersionsDirs = Maps.newHashMapWithExpectedSize(storesList.size());

        // Retrieve list of read-only stores
        List<String> readOnlyStores = Lists.newArrayList();
        for(StoreDefinition store: storesList) {
            if(store.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
                readOnlyStores.add(store.getName());
            }
        }

        // Retrieve current versions dirs for all nodes (old + new), required
        // for swapping at end
        if(readOnlyStores.size() > 0) {
            for(Node node: currentCluster.getNodes()) {
                currentROStoreVersionsDirs.put(node.getId(),
                                               adminClient.getROCurrentVersionDir(node.getId(),
                                                                                  readOnlyStores));
            }
        }

        rebalancePerClusterTransition(currentCluster,
                                      targetCluster,
                                      storesList,
                                      currentROStoreVersionsDirs,
                                      readOnlyStores);
    }

    /**
     * Rebalance on a step-by-step transitions from cluster.xml to
     * target-cluster.xml. Each transition represents the migration of one
     * primary partition at the time with all its side effect meaning migration
     * of replicas and deleting handling.
     * 
     * @param currentCluster the cluster currently running on each node.
     * @param targetCluster the desired cluster after rebalance.
     * @param storesList stores to rebalance.
     * @param currentROStoreVersionsDirs
     * @param readOnlyStores
     */
    private void rebalancePerClusterTransition(Cluster currentCluster,
                                               final Cluster targetCluster,
                                               final List<StoreDefinition> storesList,
                                               final Map<Integer, Map<String, String>> currentROStoreVersionsDirs,
                                               final List<String> readOnlyStores) {

        final Map<Node, Set<Integer>> stealerToStolenPrimaryPartitions = new HashMap<Node, Set<Integer>>();

        for(Node stealerNode: targetCluster.getNodes()) {
            stealerToStolenPrimaryPartitions.put(stealerNode, getStolenPrimaries(currentCluster,
                                                                                 targetCluster,
                                                                                 stealerNode));
        }

        for(Node stealerNode: targetCluster.getNodes()) {
            // Checks if this stealer is stealing partition. If not then nothing
            // to do
            Set<Integer> stolenPrimaryPartitions = stealerToStolenPrimaryPartitions.get(stealerNode);
            if(stolenPrimaryPartitions == null || stolenPrimaryPartitions.isEmpty())
                continue;

            // If the node has existing partitions then remove
            // the stolen partition from the list because they are going to be
            // added one by one.
            Node stealerNodeUpdated = RebalanceUtils.removePartitionToNode(stealerNode,
                                                                           stolenPrimaryPartitions);

            // Provision stolen primary partitions one by one.
            // Creates a transition cluster for each added partition.
            for(Integer donatedPrimaryPartition: stolenPrimaryPartitions) {
                Cluster targetTransition = createTransitionTargetCluster(currentCluster,
                                                                         stealerNodeUpdated,
                                                                         donatedPrimaryPartition);
                stealerNodeUpdated = targetTransition.getNodeById(stealerNodeUpdated.getId());

                final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                                                                                           targetTransition,
                                                                                           storesList,
                                                                                           rebalanceConfig.isDeleteAfterRebalancingEnabled(),
                                                                                           currentROStoreVersionsDirs);

                final OrderedClusterTransition orderedClusterTransition = new OrderedClusterTransition(currentCluster,
                                                                                                       targetTransition,
                                                                                                       rebalanceClusterPlan);

                // Only shows the plan without doing rebalancing.
                if(rebalanceConfig.isShowPlanEnabled()) {
                    System.out.print(orderedClusterTransition);
                    if(logger.isDebugEnabled()) {
                        logger.debug(orderedClusterTransition);
                    }
                } else {
                    rebalance(orderedClusterTransition, currentROStoreVersionsDirs, readOnlyStores);
                }

                currentCluster = targetTransition;
            }
        }
    }

    private void rebalance(final OrderedClusterTransition orderedClusterTransition,
                           final Map<Integer, Map<String, String>> currentROStoreVersionsDirs,
                           final List<String> readOnlyStores) {
        try {
            final Cluster transitionTarget = orderedClusterTransition.getTargetCluster();
            final List<RebalanceNodePlan> orderedRebalanceNodePlanList = orderedClusterTransition.getOrderedRebalanceNodePlanList();

            if(orderedRebalanceNodePlanList.isEmpty()) {
                if(logger.isInfoEnabled()) {
                    logger.info("Skipping rebalance-task ID:" + orderedClusterTransition.getId()
                                + " is empty");
                }
                return;
            }

            if(logger.isInfoEnabled()) {
                logger.info("Starting rebalance-task: " + orderedClusterTransition);
            }

            // Executing the rebalance tasks now.
            rebalance(currentROStoreVersionsDirs,
                      readOnlyStores,
                      transitionTarget,
                      orderedRebalanceNodePlanList);

            if(logger.isInfoEnabled()) {
                logger.info("Successfully terminated rebalance-task: "
                            + orderedClusterTransition.getId());
            }

        } catch(Exception e) {
            logger.error("Error - rebalance-task: " + orderedClusterTransition + " - "
                         + e.getMessage(), e);
            throw new VoldemortException("Rebalance failed on rebalance-task ID#"
                                         + orderedClusterTransition.getId(), e);
        }

    }

    private void rebalance(final Map<Integer, Map<String, String>> currentROStoreVersionsDirs,
                           final List<String> readOnlyStores,
                           final Cluster transitionTarget,
                           final List<RebalanceNodePlan> rebalanceNodePlanList) throws Exception {

        final List<RebalanceTask> primaryTasks = new ArrayList<RebalanceTask>();
        final List<RebalanceTask> replicaTasks = new ArrayList<RebalanceTask>();

        // List of collective exception.
        List<Exception> failures = new ArrayList<Exception>();

        // All stealer and donor nodes
        final Set<Integer> participatingNodesId = Collections.synchronizedSet(new HashSet<Integer>());

        // Tasks that contain migration of primary partitions will be executed
        // first.
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
            if(logger.isDebugEnabled()) {
                logger.debug("Waiting on the gate");
            }
            gate.await();
            if(logger.isDebugEnabled()) {
                logger.debug("Gate opened");
            }
        } catch(InterruptedException e) {
            failures.add(e);
        }

        final ExecutorService execReplicas = executeTasks(participatingNodesId,
                                                          onlyReplicas,
                                                          rebalanceConfig.getMaxParallelDonors(),
                                                          null,
                                                          replicaTasks);

        // All tasks submitted.
        if(logger.isInfoEnabled()) {
            logger.info("All rebalance tasks were submitted (shutting down in "
                        + rebalanceConfig.getRebalancingClientTimeoutSeconds() + " sec");
        }

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
            ExecutorService swapExecutors = RebalanceUtils.createExecutors(transitionTarget.getNumberOfNodes());
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
                                        final List<RebalanceTask> myRebalanceTaskList) {
        for(RebalanceTask task: myRebalanceTaskList) {
            if(task.hasErrors()) {
                failures.addAll(task.getExceptions());
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

    private List<RebalancePartitionsInfo> getTasks(List<RebalanceNodePlan> rebalanceNodePlanList,
                                                   boolean primaryOnly) {
        List<RebalancePartitionsInfo> list = new ArrayList<RebalancePartitionsInfo>();
        for(RebalanceNodePlan rebalanceNodePlan: rebalanceNodePlanList) {
            if(null == rebalanceNodePlan)
                continue;

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

    private int startNodeRebalancing(RebalancePartitionsInfo rebalanceSubTask) {
        int nTries = 0;
        AlreadyRebalancingException exception = null;

        while(nTries < MAX_TRIES) {
            nTries++;
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug("rebalancing node for: " + rebalanceSubTask);
                }
                int asyncOperationId = adminClient.rebalanceNode(rebalanceSubTask);
                if(logger.isDebugEnabled()) {
                    logger.debug("asyncOperationId: " + asyncOperationId + "for : "
                                 + rebalanceSubTask);
                }

                return asyncOperationId;
            } catch(AlreadyRebalancingException e) {
                logger.info("Node " + rebalanceSubTask.getStealerId()
                            + " is currently rebalancing will wait till it finish.");
                adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                              MetadataStore.SERVER_STATE_KEY,
                                              VoldemortState.NORMAL_SERVER.toString(),
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                exception = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing at node "
                                     + rebalanceSubTask.getStealerId() + " with rebalanceInfo:"
                                     + rebalanceSubTask, exception);
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }

    /**
     * Does an atomic commit or revert for the intended partitions ownership
     * changes and modifies adminClient with the updatedCluster.<br>
     * Creates new cluster metadata by moving partitions list passed in as
     * parameter rebalanceStealInfo and propagates it to all nodes.<br>
     * Revert all changes if failed to copy on required nodes (stealer and
     * donor).<br>
     * Holds a lock untill the commit/revert finishes.
     * 
     * @param stealerNode Node copy data from
     * @param rebalanceStealInfo Current rebalance sub task
     * @throws Exception If we are unable to propagate the cluster definition to
     *         donor and stealer
     */
    void commitClusterChanges(Node stealerNode, RebalancePartitionsInfo rebalanceStealInfo)
            throws Exception {
        synchronized(adminClient) {
            Cluster currentCluster = adminClient.getAdminClientCluster();
            Node donorNode = currentCluster.getNodeById(rebalanceStealInfo.getDonorId());

            Versioned<Cluster> latestCluster = RebalanceUtils.getLatestCluster(Arrays.asList(donorNode.getId(),
                                                                                             rebalanceStealInfo.getStealerId()),
                                                                               adminClient);
            VectorClock latestClock = (VectorClock) latestCluster.getVersion();

            // apply changes and create new updated cluster.
            // use steal master partitions to update cluster increment clock
            // version on stealerNodeId
            Cluster updatedCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                         stealerNode,
                                                                         donorNode,
                                                                         rebalanceStealInfo.getStealMasterPartitions());
            latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
            try {
                // propagates changes to all nodes.
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latestClock,
                                                Arrays.asList(stealerNode.getId(),
                                                              rebalanceStealInfo.getDonorId()));

                // set new cluster in adminClient
                adminClient.setAdminClientCluster(updatedCluster);
            } catch(Exception e) {
                // revert cluster changes.
                updatedCluster = currentCluster;
                latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latestClock,
                                                new ArrayList<Integer>());
                logger.error("Exception during comming changes in the cluster.xml - "
                             + e.getMessage(), e);
                throw e;
            }

            adminClient.setAdminClientCluster(updatedCluster);
        }
    }

    private Cluster getClusterWithNewNodes(Cluster currentCluster, Cluster targetCluster) {
        ArrayList<Node> newNodes = new ArrayList<Node>();
        for(Node node: targetCluster.getNodes()) {
            if(!RebalanceUtils.containsNode(currentCluster, node.getId())) {
                // add stealerNode with empty partitions list
                newNodes.add(RebalanceUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        return RebalanceUtils.updateCluster(currentCluster, newNodes);
    }

    /**
     * For a particular stealer node find all the partitions it will steal
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param targetCluster The target cluster definition
     * @param stealerNode The stealer node
     * @return Returns a list of partitions which this stealer node will get
     */
    private Set<Integer> getStolenPrimaries(final Cluster currentCluster,
                                            final Cluster targetCluster,
                                            final Node stealerNode) {
        int stealNodeId = stealerNode.getId();
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return new TreeSet<Integer>(targetList);
    }

    /**
     * Creates a new cluster by adding a donated partition to a new or existing
     * node.
     * 
     * @param currentCluster current cluster used to copy from.
     * @param stealerNode now or existing node being updated.
     * @param donatedPartition partition donated to the <code>stealerNode</code>
     * @return
     */
    private Cluster createTransitionTargetCluster(final Cluster currentCluster,
                                                  Node stealerNode,
                                                  final Integer donatedPartition) {
        // Gets the donor Node that owns this donated partition
        Node donorNode = getNodeByPartition(currentCluster, donatedPartition);

        // Removes the node from the list,
        final List<Node> nodes = new ArrayList<Node>(currentCluster.getNodes());
        nodes.remove(donorNode);
        nodes.remove(stealerNode);

        // Update the list of partitions for this node
        donorNode = RebalanceUtils.removePartitionToNode(donorNode, donatedPartition);
        stealerNode = RebalanceUtils.addPartitionToNode(stealerNode, donatedPartition);

        // Add the updated nodes (donor and stealer).
        nodes.add(donorNode);
        nodes.add(stealerNode);

        // After the stealer & donor were fixed recreate the cluster.
        // Sort the nodes so they will appear in the same order all the time.
        Collections.sort(nodes);
        return new Cluster(currentCluster.getName(), nodes);
    }

    /**
     * Returns the Node associated to the provided partition.
     * 
     * @param currentCluster
     * @param donatedPartition
     * @return Node that owns <code>donatedPartition</code>
     */
    private Node getNodeByPartition(Cluster currentCluster, Integer donatedPartition) {
        Map<Integer, Node> partitionToNode = new HashMap<Integer, Node>();
        for(Node node: currentCluster.getNodes()) {
            for(Integer partition: node.getPartitionIds()) {
                partitionToNode.put(partition, node);
            }
        }
        return partitionToNode.get(donatedPartition);
    }

    /**
     * Immutable representation of a cluster transition that guarantees the
     * order of the rebalance task and subtask ({@link RebalanceNodePlan},
     * {@link RebalancePartitionsInfo}) respectively.
     * 
     * The order of the task/subtaks is needed to make sure that migration of
     * primary partitions are before any other ( {@link RebalanceNodePlan} ,
     * {@link RebalancePartitionsInfo}) instances.
     * 
     * Propagation of a cluster is triggered by detecting that a stealer node is
     * given a "primary" partition only. In this case if we execute a
     * task/subtask that moves only "replicas" will only have the side effect of
     * "copying" the data saved at this point in time in the replica but not new
     * operations that enter the system afterward. On the other hand, by
     * guaranteeing that all task/subtask that migrate primary partitions happen
     * first then afterward any task/subtask that copy replicas will have all
     * the data inclusive new incoming transactions entered on the system.
     * 
     */
    public static class OrderedClusterTransition {

        private static final AtomicInteger idGen = new AtomicInteger(0);
        private final Cluster origenCluster;
        private final Cluster targetCluster;
        private final RebalanceClusterPlan rebalanceClusterPlan;
        private final List<RebalanceNodePlan> orderedRebalanceNodePlanList;
        private String printedContent;
        private final int id;

        public OrderedClusterTransition(final Cluster currentCluster,
                                        final Cluster targetCluster,
                                        final RebalanceClusterPlan rebalanceClusterPlan) {
            this.id = idGen.incrementAndGet();
            this.origenCluster = currentCluster;
            this.targetCluster = targetCluster;
            this.rebalanceClusterPlan = rebalanceClusterPlan;
            this.orderedRebalanceNodePlanList = orderedClusterPlan(rebalanceClusterPlan);
        }

        public int getId() {
            return id;
        }

        public Cluster getTargetCluster() {
            return targetCluster;
        }

        public List<RebalanceNodePlan> getOrderedRebalanceNodePlanList() {
            return orderedRebalanceNodePlanList;
        }

        /**
         * Lazy initialization. Thanks to the immutable characteristic of this
         * class, only one creations of this printable content of this class is
         * sufficient.
         */
        @Override
        public String toString() {
            if(printedContent == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("Id:").append(getId()).append(NL);
                sb.append("Cluster transition:").append(NL).append(getCurrentCluster()).append(NL);
                sb.append("Target transition:").append(NL).append(getTargetCluster()).append(NL);
                sb.append("Partition distribution:")
                  .append(NL)
                  .append(getRebalanceClusterPlan().printPartitionDistribution())
                  .append(NL);
                sb.append("Ordered rebalance node plan:")
                  .append(NL)
                  .append(printRebalanceNodePlan(getOrderedRebalanceNodePlanList()))
                  .append(NL);
                printedContent = sb.toString();
            }
            return printedContent;
        }

        private RebalanceClusterPlan getRebalanceClusterPlan() {
            return rebalanceClusterPlan;
        }

        private Cluster getCurrentCluster() {
            return origenCluster;
        }

        private List<RebalanceNodePlan> orderedClusterPlan(final RebalanceClusterPlan rebalanceClusterPlan) {
            Queue<RebalanceNodePlan> rebalancingTaskQueue = rebalanceClusterPlan.getRebalancingTaskQueue();
            RebalanceNodePlan[] array = new RebalanceNodePlan[rebalancingTaskQueue.size()];
            rebalancingTaskQueue.toArray(array);

            List<RebalanceNodePlan> plans = new ArrayList<RebalanceNodePlan>();
            for(RebalanceNodePlan rebalanceNodePlan: array) {
                List<RebalancePartitionsInfo> orderedRebalancePartitionsInfos = orderedPartitionInfos(rebalanceNodePlan);
                plans.add(new RebalanceNodePlan(rebalanceNodePlan.getStealerNode(),
                                                orderedRebalancePartitionsInfos));
            }

            return orderedNodePlans(plans);
        }

        /**
         * Ordering {@link RebalancePartitionsInfo} for a single stealer in such
         * a way that guarantees that primary (master) partitions will be before
         * any other instance of {@link RebalancePartitionsInfo} that only
         * moves/migrates replica partitions only.
         * 
         * @param rebalanceNodePlan the plan for a single stealer.
         * @return list of ordered {@link RebalancePartitionsInfo}.
         */
        private List<RebalancePartitionsInfo> orderedPartitionInfos(final RebalanceNodePlan rebalanceNodePlan) {
            List<RebalancePartitionsInfo> listPrimaries = new ArrayList<RebalancePartitionsInfo>();
            List<RebalancePartitionsInfo> listReplicas = new ArrayList<RebalancePartitionsInfo>();

            List<RebalancePartitionsInfo> pinfos = rebalanceNodePlan.getRebalanceTaskList();

            for(RebalancePartitionsInfo pinfo: pinfos) {
                List<Integer> stealMasterPartitions = pinfo.getStealMasterPartitions();
                if(stealMasterPartitions != null && !stealMasterPartitions.isEmpty()) {
                    listPrimaries.add(pinfo);
                } else {
                    listReplicas.add(pinfo);
                }
            }

            listPrimaries.addAll(listReplicas);
            return listPrimaries;
        }

        private List<RebalanceNodePlan> orderedNodePlans(List<RebalanceNodePlan> plans) {
            List<RebalanceNodePlan> first = new ArrayList<RebalanceNodePlan>();
            List<RebalanceNodePlan> second = new ArrayList<RebalanceNodePlan>();

            for(RebalanceNodePlan plan: plans) {
                boolean found = false;
                for(RebalancePartitionsInfo pinfo: plan.getRebalanceTaskList()) {
                    List<Integer> stealMasterPartitions = pinfo.getStealMasterPartitions();
                    if(stealMasterPartitions != null && !stealMasterPartitions.isEmpty()) {
                        found = true;
                        break;
                    }
                }

                if(found) {
                    first.add(plan);
                } else {
                    second.add(plan);
                }
            }
            first.addAll(second);
            return first;
        }

        private String printRebalanceNodePlan(List<RebalanceNodePlan> rebalanceNodePlanList) {
            StringBuilder sb = new StringBuilder();
            for(RebalanceNodePlan plan: rebalanceNodePlanList) {
                for(RebalancePartitionsInfo pinfo: plan.getRebalanceTaskList()) {
                    sb.append(pinfo).append(NL);
                }
            }
            return sb.toString();
        }

    }

    /**
     * Immutable class that executes a {@link RebalancePartitionsInfo} instance.
     * 
     */
    private class RebalanceTask implements Runnable {

        private final static int INVALID_REBALANCE_ID = -1;
        private final RebalancePartitionsInfo stealInfo;

        private final List<Exception> exceptions = new ArrayList<Exception>();
        private final Set<Integer> participatingNodesId;
        private final CountDownLatch gate;

        public RebalanceTask(final RebalancePartitionsInfo stealInfo,
                             final Set<Integer> participatingNodesId,
                             CountDownLatch gate) {
            this.stealInfo = deepCopy(stealInfo);
            this.participatingNodesId = participatingNodesId;
            this.gate = gate;
        }

        private RebalancePartitionsInfo deepCopy(final RebalancePartitionsInfo stealInfo) {
            String jsonString = stealInfo.toJsonString();
            return RebalancePartitionsInfo.create(jsonString);
        }

        public boolean hasErrors() {
            return !exceptions.isEmpty();
        }

        public List<Exception> getExceptions() {
            return exceptions;
        }

        public void run() {
            boolean countDownSuccessful = false;
            int rebalanceAsyncId = INVALID_REBALANCE_ID;
            final int stealerNodeId = stealInfo.getStealerId();
            participatingNodesId.add(stealerNodeId);
            participatingNodesId.add(stealInfo.getDonorId());

            try {
                rebalanceAsyncId = startNodeRebalancing(stealInfo);

                if(logger.isInfoEnabled()) {
                    logger.info("Commiting cluster changes, Async ID: " + rebalanceAsyncId
                                + ", rebalancing for stealerNode: " + stealerNodeId
                                + " with rebalanceInfo: " + stealInfo);
                }

                try {
                    commitClusterChanges(adminClient.getAdminClientCluster()
                                                    .getNodeById(stealerNodeId), stealInfo);
                } catch(Exception e) {
                    // Only when the commit fails do this.
                    if(INVALID_REBALANCE_ID != rebalanceAsyncId) {
                        adminClient.stopAsyncRequest(stealInfo.getStealerId(), rebalanceAsyncId);
                        logger.error("Commiting the cluster has failed. Async ID:"
                                     + rebalanceAsyncId);
                    }
                    throw new VoldemortRebalancingException("Impossible to commit cluster for rebalanceAsyncId: "
                                                            + rebalanceAsyncId);
                }

                // Right after committing the cluster, do a count-down, so the
                // other threads can run without waiting for this to finish.
                // Remember that "adminClient.waitForCompletion" is a blocking
                // call.
                if(gate != null) {
                    gate.countDown();
                    countDownSuccessful = true;
                }

                if(logger.isInfoEnabled()) {
                    logger.info("Waitting ForCompletion for rebalanceAsyncId:" + rebalanceAsyncId);
                }

                adminClient.waitForCompletion(stealInfo.getStealerId(),
                                              rebalanceAsyncId,
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                if(logger.isInfoEnabled()) {
                    logger.info("Succesfully finished rebalance for rebalanceAsyncId:"
                                + rebalanceAsyncId);
                }

            } catch(UnreachableStoreException e) {
                exceptions.add(e);
                logger.error("StealerNode " + stealerNodeId
                             + " is unreachable, please make sure it is up and running. - "
                             + e.getMessage(), e);
            } catch(VoldemortRebalancingException e) {
                exceptions.add(e);
                logger.error("Rebalance failed - " + e.getMessage(), e);
            } catch(Exception e) {
                exceptions.add(e);
                logger.error("Rebalance failed - " + e.getMessage(), e);
            } finally {
                // If the any exception happened before the call to count-down
                // then put it down here.
                if(gate != null && !countDownSuccessful) {
                    gate.countDown();
                }
            }
        }
    }
}
