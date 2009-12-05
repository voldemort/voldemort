package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.protocol.admin.AsyncOperationRunner;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.Pair;
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

        // instantiate and intialize rebalancing locks per node.
        final Map<Integer, AtomicBoolean> nodeRebalancingLock = new HashMap<Integer, AtomicBoolean>();
        for(int stealerNode: stealPartitionsMap.keySet()) {
            nodeRebalancingLock.put(stealerNode, new AtomicBoolean(false));
        }

        final Semaphore semaphore = new Semaphore(config.getMaxParallelRebalancingNodes());
        final List<Exception> failures = new ArrayList<Exception>();

        while(!stealPartitionsMap.isEmpty()) {
            this.executor.execute(new Runnable() {

                public void run() {
                    if(acquireSemaphore(semaphore)) {
                        try {
                            // get one target stealer(destination) node
                            int stealerNodeId = RebalanceUtils.getRandomStealerNodeId(stealPartitionsMap);

                            if(nodeRebalancingLock.get(stealerNodeId).compareAndSet(false, true)) {
                                Pair<Integer, RebalanceStealInfo> rebalanceNodeInfo = RebalanceUtils.getOneStealInfoAndUpdateStealMap(stealerNodeId,
                                                                                                                                      stealPartitionsMap);
                                // update cluster.xml and tell all Nodes
                                adminClient.setCluster(RebalanceUtils.updateAndPropagateCluster(adminClient,
                                                                                                getStealerNode(currentCluster,
                                                                                                               targetCluster,
                                                                                                               rebalanceNodeInfo.getFirst()),
                                                                                                rebalanceNodeInfo.getSecond()));
                                // attempt data transfer untill succeed or fail
                                attemptOneRebalanceTransfer(rebalanceNodeInfo.getFirst(),
                                                            rebalanceNodeInfo.getSecond());

                                // free rebalancing lock for this node.
                                nodeRebalancingLock.get(stealerNodeId).set(false);
                            }
                        } catch(Exception e) {
                            failures.add(e);
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

                /**
                 * Given a stealerNode and stealInfo {@link RebalanceStealInfo}
                 * tries stealing unless succeed or failed max number of allowed
                 * tries.
                 * 
                 * @param stealPartitionsMap
                 */
                private void attemptOneRebalanceTransfer(int stealerNodeId,
                                                         RebalanceStealInfo stealInfo) {

                    while(stealInfo.getAttempt() < config.getMaxRebalancingAttempt()) {
                        try {
                            stealInfo.setAttempt(stealInfo.getAttempt() + 1);
                            int rebalanceAsyncId = getAdminClient().rebalanceNode(stealerNodeId,
                                                                                  stealInfo);
                            AsyncOperationStatus status = getAdminClient().getAsyncRequestStatus(stealerNodeId,
                                                                                                 rebalanceAsyncId);
                            while(!status.isComplete()) {
                                logger.info("Rebalance transfer " + stealerNodeId + " status:"
                                            + status.getStatus());
                                try {
                                    Thread.sleep(60 * 1000);
                                } catch(InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }

                                status = getAdminClient().getAsyncRequestStatus(stealerNodeId,
                                                                                rebalanceAsyncId);
                            }
                            return;
                        } catch(Exception e) {
                            logger.warn("Failed Attempt number " + stealInfo.getAttempt()
                                        + " Rebalance transfer " + stealerNodeId + " <== "
                                        + stealInfo.getDonorId() + " "
                                        + stealInfo.getPartitionList() + " with exception:", e);
                        }
                    }

                    throw new VoldemortException("Failed rebalance transfer to:" + stealerNodeId
                                                 + " <== from:" + stealInfo.getDonorId()
                                                 + " partitions:" + stealInfo.getPartitionList()
                                                 + ")");
                }
            });
        }

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

    /**
     * Rebalance logic at single node level.<br>
     * <imp> should be called by the rebalancing node itself</imp><br>
     * Attempt to rebalance from node {@link RebalanceStealInfo#getDonorId()}
     * for partitionList {@link RebalanceStealInfo#getPartitionList()}
     * <p>
     * Force Sets serverState to rebalancing, Sets stealInfo in MetadataStore,
     * fetch keys from remote node and upsert them locally.<br>
     * On success clean all states it changed
     * 
     * @param metadataStore
     * @param stealInfo
     * @return taskId for asynchronous task.
     */
    public int rebalancePartitionAtNode(final MetadataStore metadataStore,
                                        final RebalanceStealInfo stealInfo,
                                        AsyncOperationRunner asyncRunner) {
        int requestId = asyncRunner.getUniqueRequestId();
        asyncRunner.submitOperation(requestId,
                                    new AsyncOperation(requestId, "rebalanceNode:"
                                                                  + stealInfo.toString()) {

                                        private int fetchAndUpdateAsyncId = -1;

                                        @Override
                                        public void operate() throws Exception {
                                            synchronized(metadataStore) {
                                                checkCurrentState(metadataStore, stealInfo);
                                                setRebalancingState(metadataStore, stealInfo);
                                            }
                                            fetchAndUpdateAsyncId = startAsyncPartitionFetch(metadataStore,
                                                                                             stealInfo);
                                            getAdminClient().waitForCompletion(metadataStore.getNodeId(),
                                                                               fetchAndUpdateAsyncId,
                                                                               24 * 60 * 60,
                                                                               TimeUnit.SECONDS);

                                            metadataStore.cleanAllRebalancingState();
                                        }

                                        @Override
                                        @JmxGetter(name = "asyncTaskStatus")
                                        public AsyncOperationStatus getStatus() {
                                            return getAdminClient().getAsyncRequestStatus(metadataStore.getNodeId(),
                                                                                          fetchAndUpdateAsyncId);
                                        }

                                        private int startAsyncPartitionFetch(MetadataStore metadataStore,
                                                                             RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            return getAdminClient().fetchAndUpdateStreams(metadataStore.getNodeId(),
                                                                                          stealInfo.getDonorId(),
                                                                                          stealInfo.getStoreName(),
                                                                                          stealInfo.getPartitionList(),
                                                                                          null);
                                        }

                                        private void setRebalancingState(MetadataStore metadataStore,
                                                                         RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            metadataStore.put(MetadataStore.SERVER_STATE_KEY,
                                                              VoldemortState.REBALANCING_MASTER_SERVER);
                                            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO,
                                                              stealInfo);
                                        }

                                        private void checkCurrentState(MetadataStore metadataStore,
                                                                       RebalanceStealInfo stealInfo)
                                                throws Exception {
                                            if(metadataStore.getServerState()
                                                            .equals(VoldemortState.REBALANCING_MASTER_SERVER)
                                               && metadataStore.getRebalancingStealInfo()
                                                               .getDonorId() != stealInfo.getDonorId())
                                                throw new VoldemortException("Server "
                                                                             + metadataStore.getNodeId()
                                                                             + " is already rebalancing from:"
                                                                             + metadataStore.getRebalancingStealInfo()
                                                                             + " rejecting rebalance request:"
                                                                             + stealInfo);
                                        }

                                    });

        return requestId;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }
}
