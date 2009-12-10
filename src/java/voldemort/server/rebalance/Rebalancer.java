package voldemort.server.rebalance;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.protocol.admin.AsyncOperationRunner;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.ImmutableList;

public class Rebalancer implements Runnable {

    private final static Logger logger = Logger.getLogger(Rebalancer.class);

    private final AtomicBoolean rebalancePermit = new AtomicBoolean(false);
    private final MetadataStore metadataStore;
    private final AdminClient adminClient;
    private final AsyncOperationRunner asyncRunner;
    private final VoldemortConfig config;

    public Rebalancer(MetadataStore metadataStore,
                      VoldemortConfig config,
                      AsyncOperationRunner asyncRunner) {
        this.metadataStore = metadataStore;
        this.asyncRunner = asyncRunner;
        this.config = config;
        this.adminClient = RebalanceUtils.createTempAdminClient(config, metadataStore.getCluster());
    }

    public void start() {
    // add startup time stuff here.
    }

    /**
     * After the current operation finishes, no longer gossip.
     */
    public void stop() {
        try {
            adminClient.stop();
        } catch(Exception e) {
            logger.error("Error while closing adminClient.", e);
        }
    }

    public boolean acquireRebalancingPermit() {
        if(rebalancePermit.compareAndSet(false, true))
            return true;

        return false;
    }

    public void releaseRebalancingPermit() {
        if(!rebalancePermit.compareAndSet(true, false)) {
            throw new VoldemortException("Invalid state rebalancePermit must be true here.");
        }
    }

    public void run() {
        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(metadataStore.getServerState())
           && acquireRebalancingPermit()) {
            RebalanceStealInfo stealInfo = metadataStore.getRebalancingStealInfo();
            logger.warn("Rebalance server found incomplete rebalancing attempt restarting "
                        + stealInfo);

            if(stealInfo.getAttempt() < config.getMaxRebalancingAttempt()) {
                attemptRebalance(stealInfo);
            } else {
                logger.warn("Rebalancing for rebalancing task:" + stealInfo
                            + " failed multiple times, Aborting more trials...");
            }

            // clean all rebalancing state
            metadataStore.cleanAllRebalancingState();
        }
    }

    private void attemptRebalance(RebalanceStealInfo stealInfo) {
        stealInfo.setAttempt(stealInfo.getAttempt() + 1);
        List<String> unbalanceStoreList = ImmutableList.copyOf(stealInfo.getUnbalancedStoreList());

        for(String storeName: unbalanceStoreList) {
            try {
                int rebalanceAsyncId = rebalanceLocalNode(storeName, stealInfo);

                adminClient.waitForCompletion(stealInfo.getStealerId(),
                                              rebalanceAsyncId,
                                              24 * 60 * 60,
                                              TimeUnit.SECONDS);
                // remove store from rebalance list
                stealInfo.getUnbalancedStoreList().remove(storeName);
            } catch(Exception e) {
                logger.warn("rebalanceSubTask:" + stealInfo + " failed for store:" + storeName, e);
            }
        }
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
    public int rebalanceLocalNode(final String storeName, final RebalanceStealInfo stealInfo) {
        int requestId = asyncRunner.getUniqueRequestId();

        asyncRunner.submitOperation(requestId, new AsyncOperation(requestId, stealInfo.toString()) {

            private int fetchAndUpdateAsyncId = -1;

            @Override
            public void operate() throws Exception {
                try {

                    checkCurrentState(metadataStore, stealInfo);
                    setRebalancingState(metadataStore, stealInfo);

                    fetchAndUpdateAsyncId = adminClient.fetchAndUpdateStreams(stealInfo.getDonorId(),
                                                                              metadataStore.getNodeId(),
                                                                              storeName,
                                                                              stealInfo.getPartitionList(),
                                                                              null);
                    logger.debug("rebalance internal async Id:" + fetchAndUpdateAsyncId);
                    adminClient.waitForCompletion(metadataStore.getNodeId(),
                                                  fetchAndUpdateAsyncId,
                                                  24 * 60 * 60,
                                                  TimeUnit.SECONDS);
                    logger.info("rebalance " + stealInfo + " completed successfully.");

                    // clean state only if successfull.
                    metadataStore.cleanAllRebalancingState();

                } finally {

                }
            }

            @Override
            @JmxGetter(name = "asyncTaskStatus")
            public AsyncOperationStatus getStatus() {
                if(-1 != fetchAndUpdateAsyncId && !asyncRunner.isComplete(fetchAndUpdateAsyncId))
                    updateStatus(asyncRunner.getStatus(fetchAndUpdateAsyncId));

                return super.getStatus();
            }
        });

        logger.debug("rebalance node request_id:" + requestId);
        return requestId;
    }

    private void setRebalancingState(MetadataStore metadataStore, RebalanceStealInfo stealInfo)
            throws Exception {
        metadataStore.put(MetadataStore.SERVER_STATE_KEY, VoldemortState.REBALANCING_MASTER_SERVER);
        metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, stealInfo);
    }

    private void checkCurrentState(MetadataStore metadataStore, RebalanceStealInfo stealInfo)
            throws Exception {
        if(metadataStore.getServerState().equals(VoldemortState.REBALANCING_MASTER_SERVER)
           && metadataStore.getRebalancingStealInfo().getDonorId() != stealInfo.getDonorId())
            throw new VoldemortException("Server " + metadataStore.getNodeId()
                                         + " is already rebalancing from:"
                                         + metadataStore.getRebalancingStealInfo()
                                         + " rejecting rebalance request:" + stealInfo);
    }
}