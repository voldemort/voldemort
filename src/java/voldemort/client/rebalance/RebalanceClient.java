package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.Pair;
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
        this.executor = Executors.newFixedThreadPool(rebalanceConfig.getMaxParallelRebalancing());
    }

    public RebalanceClient(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new AdminClient(cluster, config);
        this.rebalanceConfig = rebalanceConfig;
        this.executor = Executors.newFixedThreadPool(rebalanceConfig.getMaxParallelRebalancing());
    }

    /**
     * Voldemort dynamic cluster membership rebalancing mechanism. <br>
     * Migrate partitions across nodes to managed changes in cluster
     * memberships. <br>
     * Takes two cluster configuration currentCluster and targetCluster as
     * parameters compares and makes a list of partitions need to be
     * transferred.<br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}<br>
     * 
     * 
     * @param targetCluster: target Cluster configuration
     * @param storeList: All stores which should be rebalanced.
     */
    public void rebalance(final Cluster targetCluster, final List<String> storeList) {
        Cluster currentCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                 adminClient).getValue();
        adminClient.setAdminClientCluster(currentCluster);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        final Queue<Pair<Integer, List<RebalanceStealInfo>>> rebalanceTaskQueue = RebalanceUtils.getRebalanceTaskQueue(currentCluster,
                                                                                                                       targetCluster,
                                                                                                                       storeList);
        logRebalancingPlan(rebalanceTaskQueue);

        // start all threads
        for(int nThreads = 0; nThreads < this.rebalanceConfig.getMaxParallelRebalancing(); nThreads++) {
            this.executor.execute(new Runnable() {

                public void run() {
                    // pick one node to rebalance from queue
                    while(!rebalanceTaskQueue.isEmpty()) {
                        logger.debug("rebalanceTaskQueue size:" + rebalanceTaskQueue.size());

                        Pair<Integer, List<RebalanceStealInfo>> rebalanceTask = rebalanceTaskQueue.poll();
                        if(null != rebalanceTask) {
                            int stealerNodeId = rebalanceTask.getFirst();
                            addNodeIfnotPresent(targetCluster, stealerNodeId);
                            Node stealerNode = adminClient.getAdminClientCluster()
                                                          .getNodeById(stealerNodeId);
                            List<RebalanceStealInfo> rebalanceSubTaskList = rebalanceTask.getSecond();

                            while(rebalanceSubTaskList.size() > 0) {
                                int index = (int) Math.random() * rebalanceSubTaskList.size();
                                RebalanceStealInfo rebalanceSubTask = rebalanceSubTaskList.remove(index);
                                logger.info("Starting rebalancing for stealerNode:" + stealerNode
                                            + " rebalanceInfo:" + rebalanceSubTask);

                                try {

                                    // first commit cluster changes on nodes.
                                    commitClusterChanges(stealerNode, rebalanceSubTask);
                                    // attempt to rebalance for all stores.
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

    private void logRebalancingPlan(Queue<Pair<Integer, List<RebalanceStealInfo>>> rebalanceTaskQueue) {

        StringBuilder builder = new StringBuilder();
        builder.append("Rebalancing Plan:\n");
        for(Pair<Integer, List<RebalanceStealInfo>> pair: rebalanceTaskQueue) {
            builder.append("StealerNode:" + pair.getFirst() + "\n");
            for(RebalanceStealInfo stealInfo: pair.getSecond()) {
                builder.append("\t" + stealInfo + "\n");
            }
        }

        logger.info(builder.toString());
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
     * changes.<br>
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
    void commitClusterChanges(Node stealerNode, RebalanceStealInfo rebalanceStealInfo)
            throws Exception {
        synchronized(adminClient) {
            Versioned<Cluster> latestVersionedCluster = RebalanceUtils.getLatestCluster(Arrays.asList(stealerNode.getId(),
                                                                                                      rebalanceStealInfo.getDonorId()),
                                                                                        adminClient);
            Cluster latestCluster = latestVersionedCluster.getValue();
            VectorClock latestClock = (VectorClock) latestVersionedCluster.getVersion();
            try {
                Cluster newCluster = RebalanceUtils.createUpdatedCluster(latestCluster,
                                                                         stealerNode,
                                                                         latestCluster.getNodeById(rebalanceStealInfo.getDonorId()),
                                                                         rebalanceStealInfo.getPartitionList());
                // increment clock version on stealerNodeId
                latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());

                // propogates changes to all nodes.
                RebalanceUtils.propagateCluster(adminClient,
                                                newCluster,
                                                latestClock,
                                                Arrays.asList(stealerNode.getId(),
                                                              rebalanceStealInfo.getDonorId()));

                // set new cluster in adminClient
                adminClient.setAdminClientCluster(newCluster);
            } catch(Exception e) {
                // revert cluster changes.
                latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
                RebalanceUtils.propagateCluster(adminClient,
                                                latestCluster,
                                                latestClock,
                                                new ArrayList<Integer>());

                throw e;
            }
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
    void attemptRebalanceSubTask(RebalanceStealInfo rebalanceSubTask) {
        boolean success = true;
        List<String> rebalanceStoreList = ImmutableList.copyOf(rebalanceSubTask.getUnbalancedStoreList());
        List<String> unbalancedStoreList = new ArrayList<String>(rebalanceStoreList);
        for(String storeName: rebalanceStoreList) {
            try {
                int rebalanceAsyncId = adminClient.rebalanceNode(storeName, rebalanceSubTask);

                adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                              rebalanceAsyncId,
                                              24 * 60 * 60,
                                              TimeUnit.SECONDS);
                // remove store from rebalance list
                unbalancedStoreList.remove(storeName);
                rebalanceSubTask.setUnbalancedStoreList(unbalancedStoreList);
            } catch(Exception e) {
                logger.warn("rebalanceSubTask:" + rebalanceSubTask + " failed for store:"
                            + storeName, e);
                success = false;
            }
        }

        if(!success)
            throw new VoldemortException("rebalanceSubTask:" + rebalanceSubTask
                                         + " failed incomplete.");
    }

    private void addNodeIfnotPresent(Cluster targetCluster, int stealerNodeId) {
        if(!RebalanceUtils.containsNode(adminClient.getAdminClientCluster(), stealerNodeId)) {
            // add stealerNode from targertCluster here
            adminClient.setAdminClientCluster(RebalanceUtils.updateCluster(adminClient.getAdminClientCluster(),
                                                                           Arrays.asList(targetCluster.getNodeById(stealerNodeId))));
        }
    }
}
