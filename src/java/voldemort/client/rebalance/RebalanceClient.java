package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

public class RebalanceClient {

    private static Logger logger = Logger.getLogger(RebalanceClient.class);

    private final ExecutorService executor;
    private final AdminClient adminClient;
    RebalanceClientConfig rebalanceConfig;

    public RebalanceClient(String bootstrapUrl, RebalanceClientConfig rebalanceConfig) {
        this.adminClient = new AdminClient(bootstrapUrl, rebalanceConfig);
        this.rebalanceConfig = rebalanceConfig;
        this.executor = createExecutors(rebalanceConfig.getMaxParallelRebalancing());
    }

    public RebalanceClient(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new AdminClient(cluster, config);
        this.rebalanceConfig = config;
        this.executor = createExecutors(rebalanceConfig.getMaxParallelRebalancing());
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
        logger.info("Current Cluster configuration:" + currentVersionedCluster);
        logger.info("Target Cluster configuration:" + targetCluster);

        Cluster currentCluster = currentVersionedCluster.getValue();
        adminClient.setAdminClientCluster(currentCluster);

        List<String> storeList = RebalanceUtils.getStoreNameList(currentCluster, adminClient);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                                                                                   targetCluster,
                                                                                   storeList);
        logger.info(rebalanceClusterPlan);

        // start all threads
        for(int nThreads = 0; nThreads < this.rebalanceConfig.getMaxParallelRebalancing(); nThreads++) {
            this.executor.execute(new Runnable() {

                public void run() {
                    // pick one node to rebalance from queue
                    while(!rebalanceClusterPlan.getRebalancingTaskQueue().isEmpty()) {

                        RebalanceNodePlan rebalanceNodePlan = rebalanceClusterPlan.getRebalancingTaskQueue()
                                                                                  .poll();
                        if(null != rebalanceNodePlan) {
                            int stealerNodeId = rebalanceNodePlan.getStealerNode();
                            List<RebalancePartitionsInfo> rebalanceSubTaskList = rebalanceNodePlan.getRebalanceTaskList();

                            while(rebalanceSubTaskList.size() > 0) {
                                int index = (int) Math.random() * rebalanceSubTaskList.size();
                                RebalancePartitionsInfo rebalanceSubTask = rebalanceSubTaskList.remove(index);
                                logger.info("Starting rebalancing for stealerNode:" + stealerNodeId
                                            + " rebalanceInfo:" + rebalanceSubTask);

                                try {

                                    // commit cluster changes to all nodes
                                    commitClusterChanges(stealerNodeId,
                                                         targetCluster,
                                                         rebalanceSubTask);

                                    // attempt rebalancing.
                                    attemptRebalanceSubTask(rebalanceSubTask);

                                    logger.info("Successfully finished RebalanceSubTask attempt:"
                                                + rebalanceSubTask);
                                } catch(Exception e) {
                                    logger.warn("rebalancing task (" + rebalanceSubTask
                                                + ") failed with exception:", e);
                                }
                            }
                        }
                    }
                    logger.debug("Thread run() finished:\n");
                }
            });
        }// for (nThreads ..

        executorShutDown(executor);
    }

    private void executorShutDown(ExecutorService executorService) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                             TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service .. ", e);
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
    void commitClusterChanges(int stealerNodeId,
                              Cluster targetCluster,
                              RebalancePartitionsInfo rebalanceStealInfo) throws Exception {
        synchronized(adminClient) {
            Cluster currentCluster = adminClient.getAdminClientCluster();
            Node donorNode = currentCluster.getNodeById(rebalanceStealInfo.getDonorId());

            // Add stealerNode to currentCluster if not present.
            Cluster updatedCluster = getClusterWithStealerNode(currentCluster,
                                                               targetCluster,
                                                               stealerNodeId);
            Node stealerNode = updatedCluster.getNodeById(stealerNodeId);
            adminClient.setAdminClientCluster(updatedCluster);

            VectorClock latestClock = (VectorClock) RebalanceUtils.getLatestCluster(Arrays.asList(stealerNode.getId(),
                                                                                                  rebalanceStealInfo.getDonorId()),
                                                                                    adminClient)
                                                                  .getVersion();

            // apply changes and create new updated cluster.
            updatedCluster = RebalanceUtils.createUpdatedCluster(updatedCluster,
                                                                 stealerNode,
                                                                 donorNode,
                                                                 rebalanceStealInfo.getPartitionList());
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

    /**
     * Attempt the data transfer on the stealerNode through the
     * {@link AdminClient#rebalanceNode()} api for all stores in
     * rebalanceSubTask.<br>
     * Blocks untill the AsyncStatus is set to success or exception <br>
     * 
     * @param stealerNodeId
     * @param stealPartitionsMap
     */
    void attemptRebalanceSubTask(RebalancePartitionsInfo rebalanceSubTask) {
        List<String> rebalanceStoreList = ImmutableList.copyOf(rebalanceSubTask.getUnbalancedStoreList());
        List<String> unbalancedStoreList = new ArrayList<String>(rebalanceStoreList);
        for(String storeName: rebalanceStoreList) {
            try {
                int rebalanceAsyncId = adminClient.rebalanceNode(storeName, rebalanceSubTask);

                adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                              rebalanceAsyncId,
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                // remove store from rebalance list
                unbalancedStoreList.remove(storeName);
                rebalanceSubTask.setUnbalancedStoreList(unbalancedStoreList);
            } catch(Exception e) {
                throw new VoldemortException("rebalanceSubTask:" + rebalanceSubTask
                                             + " failed for store:" + storeName, e);
            }
        }
    }

    private Cluster getClusterWithStealerNode(Cluster currentCluster,
                                              Cluster targetCluster,
                                              int stealerNodeId) {
        if(!RebalanceUtils.containsNode(currentCluster, stealerNodeId)) {
            // add stealerNode with empty partitions list
            Node stealerNode = RebalanceUtils.updateNode(targetCluster.getNodeById(stealerNodeId),
                                                         new ArrayList<Integer>());
            return RebalanceUtils.updateCluster(currentCluster, Arrays.asList(stealerNode));
        }

        return currentCluster;
    }
}
