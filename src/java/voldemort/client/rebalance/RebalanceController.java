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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

public class RebalanceController {

    private static final int MAX_TRIES = 2;
    private static Logger logger = Logger.getLogger(RebalanceController.class);

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

    private ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    /**
     * Grabs the latest cluster definition and calls
     * {@link #rebalance(voldemort.cluster.Cluster, voldemort.cluster.Cluster)}
     * 
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final Cluster targetCluster) {
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                                     adminClient);
        rebalance(currentVersionedCluster.getValue(), targetCluster);
    }

    /**
     * Splits the rebalance node plan of a single stealer node to return a map
     * of per donor node plan
     * 
     * @param rebalanceNodePlan The complete rebalance plan
     * @return MultiMap with key being donor node id
     */
    private SetMultimap<Integer, RebalancePartitionsInfo> divideRebalanceNodePlan(RebalanceNodePlan rebalanceNodePlan) {
        SetMultimap<Integer, RebalancePartitionsInfo> plan = HashMultimap.create();
        List<RebalancePartitionsInfo> rebalanceSubTaskList = rebalanceNodePlan.getRebalanceTaskList();

        for(RebalancePartitionsInfo rebalanceSubTask: rebalanceSubTaskList) {
            plan.put(rebalanceSubTask.getDonorId(), rebalanceSubTask);
        }

        return plan;
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
        logger.debug("Current Cluster configuration:" + currentCluster);
        logger.debug("Target Cluster configuration:" + targetCluster);

        adminClient.setAdminClientCluster(currentCluster);

        Cluster oldCluster = currentCluster;
        // Retrieve list of stores
        List<StoreDefinition> storesList = RebalanceUtils.getStoreNameList(currentCluster,
                                                                           adminClient);

        // Add all new nodes to currentCluster
        currentCluster = getClusterWithNewNodes(currentCluster, targetCluster);
        adminClient.setAdminClientCluster(currentCluster);

        // Maintain nodeId to map of read-only store name to current version
        // dirs
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

        final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(oldCluster,
                                                                                   targetCluster,
                                                                                   storesList,
                                                                                   rebalanceConfig.isDeleteAfterRebalancingEnabled());
        logger.info(rebalanceClusterPlan);

        // propagate new cluster information to all
        Node firstNode = currentCluster.getNodes().iterator().next();
        VectorClock latestClock = (VectorClock) RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                                adminClient)
                                                              .getVersion();
        RebalanceUtils.propagateCluster(adminClient,
                                        currentCluster,
                                        latestClock.incremented(firstNode.getId(),
                                                                System.currentTimeMillis()),
                                        new ArrayList<Integer>());

        ExecutorService executor = createExecutors(rebalanceConfig.getMaxParallelRebalancing());

        // start all threads
        for(int nThreads = 0; nThreads < this.rebalanceConfig.getMaxParallelRebalancing(); nThreads++) {
            executor.execute(new Runnable() {

                public void run() {
                    // pick one node to rebalance from queue
                    while(!rebalanceClusterPlan.getRebalancingTaskQueue().isEmpty()) {

                        RebalanceNodePlan rebalanceTask = rebalanceClusterPlan.getRebalancingTaskQueue()
                                                                              .poll();

                        if(null != rebalanceTask) {
                            final int stealerNodeId = rebalanceTask.getStealerNode();
                            final SetMultimap<Integer, RebalancePartitionsInfo> rebalanceSubTaskMap = divideRebalanceNodePlan(rebalanceTask);
                            final Set<Integer> parallelDonors = rebalanceSubTaskMap.keySet();
                            ExecutorService parallelDonorExecutor = createExecutors(rebalanceConfig.getMaxParallelDonors());

                            for(final int donorNodeId: parallelDonors) {
                                parallelDonorExecutor.execute(new Runnable() {

                                    public void run() {
                                        Set<RebalancePartitionsInfo> tasksForDonor = rebalanceSubTaskMap.get(donorNodeId);

                                        for(RebalancePartitionsInfo stealInfo: tasksForDonor) {
                                            logger.info("Starting rebalancing for stealerNode: "
                                                        + stealerNodeId + " with rebalanceInfo: "
                                                        + stealInfo);

                                            try {
                                                int rebalanceAsyncId = startNodeRebalancing(stealInfo);

                                                try {
                                                    commitClusterChanges(adminClient.getAdminClientCluster()
                                                                                    .getNodeById(stealerNodeId),
                                                                         stealInfo);
                                                } catch(Exception e) {
                                                    if(-1 != rebalanceAsyncId) {
                                                        adminClient.stopAsyncRequest(stealInfo.getStealerId(),
                                                                                     rebalanceAsyncId);
                                                    }
                                                    throw e;
                                                }

                                                adminClient.waitForCompletion(stealInfo.getStealerId(),
                                                                              rebalanceAsyncId,
                                                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                                                              TimeUnit.SECONDS);

                                                logger.info("Succesfully finished rebalance attempt: "
                                                            + stealInfo);
                                            } catch(UnreachableStoreException e) {
                                                logger.error("StealerNode "
                                                                     + stealerNodeId
                                                                     + " is unreachable, please make sure it is up and running.",
                                                             e);
                                            } catch(VoldemortRebalancingException e) {
                                                logger.error(e);
                                                for(Exception cause: e.getCauses()) {
                                                    logger.error(cause);
                                                }
                                            } catch(Exception e) {
                                                logger.error("Rebalancing task failed with exception",
                                                             e);
                                            }
                                        }
                                    }
                                });
                            }

                            try {
                                executorShutDown(parallelDonorExecutor);
                            } catch(Exception e) {
                                logger.error("Interrupted", e);
                            }
                        }
                    }
                    logger.info("Thread run() finished:\n");
                }

            });
        }// for (nThreads ..

        try {
            executorShutDown(executor);
        } catch(Exception e) {
            logger.error("Interrupted rebalance executor ", e);
            return;
        }

        // If all successful, swap the read-only stores
        if(readOnlyStores.size() > 0) {
            ExecutorService swapExecutors = createExecutors(targetCluster.getNumberOfNodes());
            for(final Node node: targetCluster.getNodes()) {
                swapExecutors.submit(new Runnable() {

                    public void run() {
                        Map<String, String> storeDirs = currentROStoreVersionsDirs.get(node.getId());
                        for(String storeName: storeDirs.keySet()) {
                            logger.info("Swapping " + storeName + " on node " + node.getId());
                            adminClient.swapStore(node.getId(), storeName, storeDirs.get(storeName));
                            logger.info("Successfully swapped " + storeName + " on node "
                                        + node.getId());
                        }
                    }
                });
            }

            try {
                executorShutDown(swapExecutors);
            } catch(Exception e) {
                logger.error("Interrupted swapping executor ", e);
                return;
            }
        }

    }

    private int startNodeRebalancing(RebalancePartitionsInfo rebalanceSubTask) {
        int nTries = 0;
        AlreadyRebalancingException exception = null;

        while(nTries < MAX_TRIES) {
            nTries++;
            try {
                return adminClient.rebalanceNode(rebalanceSubTask);
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

    private void executorShutDown(ExecutorService executorService) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                             TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service.", e);
        }
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }

    /* package level function to ease of unit testing */

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
}
