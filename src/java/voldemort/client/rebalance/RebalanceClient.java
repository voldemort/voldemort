package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;

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
     * @param storeName : store to be rebalanced
     * @param currentCluster: currentCluster configuration.
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final String storeName,
                          final Cluster currentCluster,
                          final Cluster targetCluster) {
        // update adminClient with currentCluster
        adminClient.setCluster(currentCluster);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        final Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap = RebalanceUtils.getStealPartitionsMap(storeName,
                                                                                                               currentCluster,
                                                                                                               targetCluster);
        logger.info("Rebalancing plan:\n"
                    + RebalanceUtils.getStealPartitionsMapAsString(stealPartitionsMap));

        final Map<Integer, AtomicBoolean> nodeRebalancingLock = createRebalancingLocks(stealPartitionsMap);
        final Semaphore semaphore = new Semaphore(config.getMaxParallelRebalancingNodes());

        while(!stealPartitionsMap.isEmpty()) {
            this.executor.execute(new Runnable() {

                public void run() {
                    if(acquireSemaphore(semaphore)) {
                        try {
                            // get one target stealer(destination) node
                            int stealerNodeId = RebalanceUtils.getRandomStealerNodeId(stealPartitionsMap);
                            Node stealerNode = getStealerNode(currentCluster,
                                                              targetCluster,
                                                              stealerNodeId);

                            if(nodeRebalancingLock.get(stealerNodeId).compareAndSet(false, true)) {
                                RebalanceStealInfo rebalanceStealInfo = RebalanceUtils.getOneStealInfoAndUpdateStealMap(stealerNodeId,
                                                                                                                        stealPartitionsMap);
                                // commit or revert on rebalance transfer
                                if(RebalanceUtils.rebalanceCommitOrRevert(stealPartitionsMap,
                                                                          stealerNode,
                                                                          rebalanceStealInfo,
                                                                          getAdminClient())) {
                                    boolean success = RebalanceUtils.attemptRebalanceTransfer(stealerNode,
                                                                                              rebalanceStealInfo,
                                                                                              getAdminClient());

                                    if(!success) {
                                        if(rebalanceStealInfo.getAttempt() < config.getMaxRebalancingAttempt()) {
                                            // increment attempt and add back.
                                            rebalanceStealInfo.setAttempt(rebalanceStealInfo.getAttempt() + 1);
                                            RebalanceUtils.revertStealPartitionsMap(stealPartitionsMap,
                                                                                    stealerNodeId,
                                                                                    rebalanceStealInfo);
                                        } else {
                                            logger.error("Rebalance attempt for node:"
                                                         + stealerNodeId + " failed max times.");
                                        }

                                    }
                                }

                                // free rebalancing lock for this node.
                                nodeRebalancingLock.get(stealerNodeId).set(false);
                            }
                        } catch(Exception e) {
                            logger.warn("Rebalance step failed", e);
                        } finally {
                            semaphore.release();
                            logger.debug("rebalancing semaphore released.");
                        }
                    } else {
                        logger.warn(new VoldemortException("Failed to get rebalance task permit."));
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
            });
        }

    }

    private Map<Integer, AtomicBoolean> createRebalancingLocks(Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
        Map<Integer, AtomicBoolean> map = new HashMap<Integer, AtomicBoolean>();
        for(int stealerNode: stealPartitionsMap.keySet()) {
            map.put(stealerNode, new AtomicBoolean(false));
        }
        return map;
    }

    private boolean acquireSemaphore(Semaphore semaphore) {
        try {
            logger.debug("Request to acquire rebalancing semaphore.");
            semaphore.acquire();
            logger.debug("rebalancing semaphore acquired.");
            return true;
        } catch(InterruptedException e) {
            // ignore
        }

        return false;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }
}
