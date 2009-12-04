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
import voldemort.annotations.jmx.JmxGetter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
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
     * Voldemort online rebalancing mechanism. <br>
     * Compares the provided currentCluster and targetCluster and makes a list
     * of partitions need to be transferred <br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}
     * 
     * @param storeName : store to be rebalanced
     * @param currentCluster: currentCluster configuration.
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final String storeName,
                          final Cluster currentCluster,
                          final Cluster targetCluster) {
        // update cluster info for adminClient
        adminClient.setCluster(currentCluster);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        final Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap = RebalanceUtils.getStealPartitionsMap(storeName,
                                                                                                               currentCluster,
                                                                                                               targetCluster);
        logger.info("Rebalancing plan:\n"
                    + RebalanceUtils.getStealPartitionsMapAsString(stealPartitionsMap));

        final Map<Integer, AtomicBoolean> nodeLock = new HashMap<Integer, AtomicBoolean>();
        final List<Exception> failures = new ArrayList<Exception>();
        final Semaphore semaphore = new Semaphore(config.getMaxParallelRebalancingNodes());

        // initialize nodeLocks
        for(int stealerNode: stealPartitionsMap.keySet()) {
            nodeLock.put(stealerNode, new AtomicBoolean(false));
        }

        while(!stealPartitionsMap.isEmpty()) {
            this.executor.execute(new Runnable() {

                public void run() {
                    if(acquireSemaphore(semaphore)) {
                        try {
                            int stealerNodeId = getRandomStealerNodeId(stealPartitionsMap);
                            if(nodeLock.get(stealerNodeId).compareAndSet(false, true)) {
                                Pair<Integer, RebalanceStealInfo> rebalanceNodeInfo = getOneStealInfoAndupdateStealMap(stealerNodeId,
                                                                                                                       stealPartitionsMap);

                                attemptOneRebalanceTransfer(rebalanceNodeInfo.getFirst(),
                                                            rebalanceNodeInfo.getSecond());
                                nodeLock.get(stealerNodeId).set(false);
                            }
                        } catch(Exception e) {
                            failures.add(e);
                        } finally {
                            semaphore.release();
                        }
                    } else {
                        failures.add(new VoldemortException("Failed to get rebalance task permit."));
                    }
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
                            getAdminClient().rebalanceNode(stealerNodeId, stealInfo);
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

                /**
                 * Pick and remove the RebalanceStealInfo from the
                 * List<RebalanceStealInfo> tail for the given stealerNodeId<br>
                 * Deletes the entire key from map if the resultant list becomes
                 * empty.
                 * 
                 * @param stealPartitionsMap
                 * @return
                 */
                private Pair<Integer, RebalanceStealInfo> getOneStealInfoAndupdateStealMap(int stealerNodeId,
                                                                                           Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
                    synchronized(stealPartitionsMap) {
                        List<RebalanceStealInfo> stealInfoList = stealPartitionsMap.get(stealerNodeId);
                        int stealInfoIndex = stealInfoList.size() - 1;
                        RebalanceStealInfo stealInfo = stealInfoList.get(stealInfoIndex);
                        stealInfoList.remove(stealInfoIndex);

                        if(stealInfoList.isEmpty()) {
                            stealPartitionsMap.remove(stealerNodeId);
                        }

                        return new Pair<Integer, RebalanceStealInfo>(stealerNodeId, stealInfo);
                    }
                }

                private int getRandomStealerNodeId(Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap) {
                    int size = stealPartitionsMap.keySet().size();
                    int randomIndex = (int) (Math.random() * size);
                    return stealPartitionsMap.keySet().toArray(new Integer[0])[randomIndex];
                }
            });
        }

    }

    private boolean acquireSemaphore(Semaphore semaphore) {
        try {
            semaphore.acquire();
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
