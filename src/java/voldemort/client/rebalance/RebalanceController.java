package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class RebalanceController {

    private static final int MAX_TRIES = 2;
    private static final long SEED = 5276239082346L;
    private static Logger logger = Logger.getLogger(RebalanceController.class);

    private final AdminClient adminClient;
    private final Random random = new Random(SEED);
    RebalanceClientConfig rebalanceConfig;

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
     * Voldemort dynamic cluster membership rebalancing mechanism. <br>
     * Migrate partitions across nodes to managed changes in cluster
     * memberships. <br>
     * Takes targetCluster as parameters, fetches the current cluster
     * configuration from the cluster compares and makes a list of partitions
     * need to be transferred.<br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}<br>
     * 
     * 
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final Cluster targetCluster) {
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                                     adminClient);
        rebalance(currentVersionedCluster.getValue(), targetCluster);
    }

    /**
     * Voldemort dynamic cluster membership rebalancing mechanism. <br>
     * Migrate partitions across nodes to managed changes in cluster
     * memberships. <br>
     * Takes targetCluster as parameters, fetches the current cluster
     * configuration from the cluster compares and makes a list of partitions
     * need to be transferred.<br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}<br>
     * 
     * 
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(Cluster currentCluster, final Cluster targetCluster) {
        logger.debug("Current Cluster configuration:" + currentCluster);
        logger.debug("Target Cluster configuration:" + targetCluster);

        adminClient.setAdminClientCluster(currentCluster);

        final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                                                                                   targetCluster,
                                                                                   RebalanceUtils.getStoreNameList(currentCluster,
                                                                                                                   adminClient),
                                                                                   rebalanceConfig.isDeleteAfterRebalancingEnabled());
        logger.info(rebalanceClusterPlan);

        // add all new nodes to currentCluster and propagate to all
        currentCluster = getClusterWithNewNodes(currentCluster, targetCluster);
        adminClient.setAdminClientCluster(currentCluster);
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
                            int stealerNodeId = rebalanceTask.getStealerNode();
                            List<RebalancePartitionsInfo> rebalanceSubTaskList = rebalanceTask.getRebalanceTaskList();

                            while(rebalanceSubTaskList.size() > 0) {
                                int index = (int) (random.nextDouble() * rebalanceSubTaskList.size());
                                RebalancePartitionsInfo rebalanceSubTask = rebalanceSubTaskList.remove(index);
                                logger.info("Starting rebalancing for stealerNode:" + stealerNodeId
                                            + " with rebalanceInfo:" + rebalanceSubTask);

                                try {
                                    int rebalanceAsyncId = startNodeRebalancing(rebalanceSubTask);

                                    try {
                                        commitClusterChanges(adminClient.getAdminClientCluster()
                                                                        .getNodeById(stealerNodeId),
                                                             rebalanceSubTask);
                                    } catch(Exception e) {
                                        if(-1 != rebalanceAsyncId) {
                                            adminClient.stopAsyncRequest(rebalanceSubTask.getStealerId(),
                                                                         rebalanceAsyncId);
                                        }
                                        throw e;
                                    }

                                    adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                                                  rebalanceAsyncId,
                                                                  rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                                                  TimeUnit.SECONDS);

                                    logger.info("Successfully finished rebalance attempt:"
                                                + rebalanceSubTask);
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
                                    logger.error("Rebalancing task failed with exception", e);
                                }
                            }
                        }
                    }
                    logger.info("Thread run() finished:\n");
                }

            });
        }// for (nThreads ..
        executorShutDown(executor);
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
     * changes and modify adminClient with the updatedCluster.<br>
     * creates a new cluster metadata by moving partitions list passed in
     * parameter rebalanceStealInfo and propagates it to all nodes.<br>
     * Revert all changes if failed to copy on required copies (stealerNode and
     * donorNode).<br>
     * holds a lock untill the commit/revert finishes.
     * 
     * @param stealPartitionsMap
     * @param stealerNodeId
     * @param rebalanceStealInfo
     * @throws Exception
     */
    void commitClusterChanges(Node stealerNode, RebalancePartitionsInfo rebalanceStealInfo)
            throws Exception {
        synchronized(adminClient) {
            Cluster currentCluster = adminClient.getAdminClientCluster();
            Node donorNode = currentCluster.getNodeById(rebalanceStealInfo.getDonorId());

            VectorClock latestClock = (VectorClock) RebalanceUtils.getLatestCluster(Arrays.asList(stealerNode.getId(),
                                                                                                  rebalanceStealInfo.getDonorId()),
                                                                                    adminClient)
                                                                  .getVersion();

            // apply changes and create new updated cluster.
            Cluster updatedCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                         stealerNode,
                                                                         donorNode,
                                                                         rebalanceStealInfo.getStealMasterPartitions());// use steal master partitions to update cluster
            // increment clock version on stealerNodeId
            latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
            try {
                // propogates changes to all nodes.
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
