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
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;

import com.google.common.collect.ImmutableList;

public class RebalanceClient {

    private static Logger logger = Logger.getLogger(RebalanceClient.class);

    private final ExecutorService executor;
    private final AdminClient adminClient;
    private final RebalanceClientConfig config;

    public RebalanceClient(String bootstrapUrl, RebalanceClientConfig config) {
        this.adminClient = new ProtoBuffAdminClientRequestFormat(bootstrapUrl, config);
        this.executor = Executors.newFixedThreadPool(config.getMaxParallelRebalancingNodes());
        this.config = config;
    }

    public RebalanceClient(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new ProtoBuffAdminClientRequestFormat(cluster, config);
        this.executor = Executors.newFixedThreadPool(config.getMaxParallelRebalancingNodes());
        this.config = config;
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
     * @param currentCluster: currentCluster configuration.
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final Cluster currentCluster,
                          final Cluster targetCluster,
                          final List<String> storeList) {
        // update adminClient with currentCluster
        adminClient.setCluster(currentCluster);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        final Queue<Pair<Integer, List<RebalanceStealInfo>>> rebalanceTaskQueue = RebalanceUtils.getRebalanceTaskQueue(currentCluster,
                                                                                                                       targetCluster,
                                                                                                                       storeList);
        logger.info("Rebalancing Task Queue(first:StealerNodeId, second:RebalanceStealInfo):\n"
                    + rebalanceTaskQueue);

        // start all threads
        for(int nThreads = 0; nThreads < config.getMaxParallelRebalancingNodes(); nThreads++) {
            this.executor.execute(new Runnable() {

                public void run() {
                    // pick one node to rebalance from queue
                    while(!rebalanceTaskQueue.isEmpty()) {
                        Pair<Integer, List<RebalanceStealInfo>> rebalanceTask = rebalanceTaskQueue.poll();
                        if(null != rebalanceTask) {
                            Node stealerNode = getStealerNode(currentCluster,
                                                              targetCluster,
                                                              rebalanceTask.getFirst());
                            List<RebalanceStealInfo> rebalanceSubTaskList = rebalanceTask.getSecond();

                            logger.info("Starting rebalancing for stealerNode:" + stealerNode
                                        + " with rebalanceSubTaskList:" + rebalanceSubTaskList);

                            while(rebalanceSubTaskList.size() > 0) {
                                RebalanceStealInfo rebalanceSubTask = rebalanceSubTaskList.remove(0);
                                try {
                                    logger.info("Starting RebalanceSubTask attempt:"
                                                + rebalanceSubTask);

                                    // first commit cluster changes on nodes.
                                    commitClusterChanges(stealerNode, rebalanceSubTask);
                                    // attempt to rebalance for all stores.
                                    attemptRebalanceSubTask(rebalanceSubTask);

                                    logger.info("Successfully finished RebalanceSubTask attempt:"
                                                + rebalanceSubTask);
                                } catch(Exception e) {
                                    logger.warn("rebalancing task failed" + rebalanceSubTask);

                                    rebalanceSubTask.setAttempt(rebalanceSubTask.getAttempt() + 1);
                                    if(rebalanceSubTask.getAttempt() < config.getMaxRebalancingAttempt())
                                        rebalanceSubTaskList.add(rebalanceSubTask);
                                    else
                                        logger.error("rebalancing task "
                                                     + rebalanceSubTask
                                                     + "  failed max attempts .. Aborting more trials !!");
                                }
                            }
                        }
                    }
                }

                private Node getStealerNode(Cluster currentCluster,
                                            Cluster targetCluster,
                                            int stealerNodeId) {
                    if(RebalanceUtils.containsNode(currentCluster, stealerNodeId))
                        return currentCluster.getNodeById(stealerNodeId);
                    else
                        return RebalanceUtils.updateNode(targetCluster.getNodeById(stealerNodeId),
                                                         new ArrayList<Integer>());
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

                    for(String storeName: rebalanceStoreList) {
                        try {
                            int rebalanceAsyncId = adminClient.rebalanceNode(storeName,
                                                                             rebalanceSubTask);

                            adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                                          rebalanceAsyncId,
                                                          24 * 60 * 60,
                                                          TimeUnit.SECONDS);
                            // remove store from rebalance list
                            rebalanceSubTask.getUnbalancedStoreList().remove(storeName);
                        } catch(Exception e) {
                            logger.warn("rebalanceSubTask:" + rebalanceSubTask
                                        + " failed for store:" + storeName, e);
                            success = false;
                        }
                    }

                    if(!success)
                        throw new VoldemortException("rebalanceSubTask:" + rebalanceSubTask
                                                     + " failed incomplete.");
                }

                /**
                 * Does an atomic commit or revert for the intended partitions
                 * ownership changes.<br>
                 * creates a new cluster metadata by moving partitions list
                 * passed in parameter rebalanceStealInfo and propagates it to
                 * all nodes.<br>
                 * Revert all changes if failed to copy on required copies
                 * (stealerNode and donorNode).<br>
                 * holds a lock untill the commit/revert finishes.
                 * 
                 * @param stealPartitionsMap
                 * @param stealerNodeId
                 * @param rebalanceStealInfo
                 */
                void commitClusterChanges(Node stealerNode, RebalanceStealInfo rebalanceStealInfo) {
                    synchronized(adminClient) {
                        VectorClock clock = (VectorClock) adminClient.getRemoteCluster(rebalanceStealInfo.getDonorId())
                                                                     .getVersion();
                        Cluster oldCluster = adminClient.getCluster();

                        try {
                            Cluster newCluster = RebalanceUtils.createUpdatedCluster(oldCluster,
                                                                                     stealerNode,
                                                                                     oldCluster.getNodeById(rebalanceStealInfo.getDonorId()),
                                                                                     rebalanceStealInfo.getPartitionList());
                            // increment clock version on stealerNodeId
                            clock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());

                            // propogates changes to all nodes.
                            RebalanceUtils.propagateCluster(adminClient,
                                                            newCluster,
                                                            clock,
                                                            Arrays.asList(stealerNode.getId(),
                                                                          rebalanceStealInfo.getDonorId()));

                            // set new cluster in adminClient
                            adminClient.setCluster(newCluster);
                        } catch(Exception e) {
                            // revert cluster changes.
                            clock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
                            RebalanceUtils.propagateCluster(adminClient,
                                                            oldCluster,
                                                            clock,
                                                            new ArrayList<Integer>());

                            throw new VoldemortException("Failed to commit rebalance on node:"
                                                         + stealerNode.getId(), e);
                        }
                    }
                }
            });
        }

    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }
}
