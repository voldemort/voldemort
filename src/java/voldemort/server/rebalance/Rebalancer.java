package voldemort.server.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
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
    private final AsyncOperationRunner asyncRunner;
    private final VoldemortConfig voldemortConfig;

    public Rebalancer(MetadataStore metadataStore,
                      VoldemortConfig voldemortConfig,
                      AsyncOperationRunner asyncRunner) {
        this.metadataStore = metadataStore;
        this.asyncRunner = asyncRunner;
        this.voldemortConfig = voldemortConfig;
    }

    public void start() {
    // add startup time stuff here.
    }

    public void stop() {}

    private boolean acquireRebalancingPermit() {
        if(rebalancePermit.compareAndSet(false, true))
            return true;

        return false;
    }

    private void releaseRebalancingPermit() {
        if(!rebalancePermit.compareAndSet(true, false)) {
            throw new VoldemortException("Invalid state rebalancePermit must be true here.");
        }
    }

    public void run() {
        logger.debug("rebalancer run() called.");
        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(metadataStore.getServerState())
           && acquireRebalancingPermit()) {

            // free permit here for rebalanceLocalNode to acquire.
            releaseRebalancingPermit();

            RebalancePartitionsInfo stealInfo = metadataStore.getRebalancingStealInfo();

            try {
                logger.warn("Rebalance server found incomplete rebalancing attempt " + stealInfo
                            + " restarting ...");

                if(stealInfo.getAttempt() < voldemortConfig.getMaxRebalancingAttempt()) {
                    attemptRebalance(stealInfo);
                } else {
                    logger.warn("Rebalancing for rebalancing task:" + stealInfo
                                + " failed multiple times, Aborting more trials...");
                    metadataStore.cleanAllRebalancingState();
                }
            } catch(Exception e) {
                logger.error("RebalanceService rebalancing attempt " + stealInfo
                             + " failed with exception", e);
            }
        }
    }

    private void attemptRebalance(RebalancePartitionsInfo stealInfo) {
        stealInfo.setAttempt(stealInfo.getAttempt() + 1);

        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadataStore.getCluster());
        int rebalanceAsyncId = rebalanceLocalNode(stealInfo);

        adminClient.waitForCompletion(stealInfo.getStealerId(),
                                      rebalanceAsyncId,
                                      voldemortConfig.getAdminSocketTimeout(),
                                      TimeUnit.SECONDS);
    }

    /**
     * Rebalance logic at single node level.<br>
     * <imp> should be called by the rebalancing node itself</imp><br>
     * Attempt to rebalance from node
     * {@link RebalancePartitionsInfo#getDonorId()} for partitionList
     * {@link RebalancePartitionsInfo#getPartitionList()}
     * <p>
     * Force Sets serverState to rebalancing, Sets stealInfo in MetadataStore,
     * fetch keys from remote node and upsert them locally.<br>
     * On success clean all states it changed
     * 
     * @param metadataStore
     * @param stealInfo
     * @return taskId for asynchronous task.
     */
    public int rebalanceLocalNode(final RebalancePartitionsInfo stealInfo) {

        if(!acquireRebalancingPermit()) {
            RebalancePartitionsInfo info = metadataStore.getRebalancingStealInfo();
            throw new VoldemortException("Node "
                                         + metadataStore.getCluster()
                                                        .getNodeById(info.getStealerId())
                                         + " is already rebalancing from " + info.getDonorId()
                                         + " rebalanceInfo:" + info);
        }

        // check and set State
        checkCurrentState(metadataStore, stealInfo);
        setRebalancingState(metadataStore, stealInfo);

        int requestId = asyncRunner.getUniqueRequestId();

        asyncRunner.submitOperation(requestId, new AsyncOperation(requestId, stealInfo.toString()) {

            @Override
            public void stop() {
                // TODO: verify if this is correct
                Thread.currentThread().interrupt();
            }

            private int migratePartitionsAsyncId = -1;
            private String currentStore = null;

            @Override
            public void operate() throws Exception {
                AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                               metadataStore.getCluster());
                try {
                    logger.info("Rebalancer: rebalance " + stealInfo + " starting.");
                    List<String> tempUnbalancedStoreList = new ArrayList<String>(stealInfo.getUnbalancedStoreList());
                    for(String storeName: ImmutableList.copyOf(stealInfo.getUnbalancedStoreList())) {
                        try {
                            rebalanceStore(storeName, adminClient, stealInfo);

                            // remove store from stealInfo unbalanced list.
                            tempUnbalancedStoreList.remove(storeName);
                            stealInfo.setUnbalancedStoreList(tempUnbalancedStoreList);
                        } catch(Exception e) {
                            logger.warn("rebalanceSubTask:" + stealInfo + " failed for store:"
                                        + storeName, e);
                        }
                    }

                    if(stealInfo.getUnbalancedStoreList().isEmpty()) {
                        logger.info("Rebalancer: rebalance " + stealInfo
                                    + " completed successfully.");
                        // clean state only if successfull.
                        metadataStore.cleanAllRebalancingState();
                    } else {
                        throw new VoldemortException("Rebalancer: Failed to rebalance all stores, unbalanced stores:"
                                                     + stealInfo.getUnbalancedStoreList()
                                                     + " rebalanceInfo:" + stealInfo);
                    }

                } finally {
                    // free the permit in all cases.
                    releaseRebalancingPermit();
                    adminClient.stop();
                    migratePartitionsAsyncId = -1;
                }
            }

            private void rebalanceStore(String storeName,
                                        AdminClient adminClient,
                                        RebalancePartitionsInfo stealInfo) throws Exception {
                updateStatus("starting partitions migration for store:" + storeName);
                currentStore = storeName;

                migratePartitionsAsyncId = adminClient.migratePartitions(stealInfo.getDonorId(),
                                                                         metadataStore.getNodeId(),
                                                                         storeName,
                                                                         stealInfo.getPartitionList(),
                                                                         null);
                adminClient.waitForCompletion(metadataStore.getNodeId(),
                                              migratePartitionsAsyncId,
                                              voldemortConfig.getAdminSocketTimeout(),
                                              TimeUnit.SECONDS);

                if(stealInfo.isDeleteDonorPartitions()) {
                    logger.warn("Deleting data from donorNode after rebalancing !!");
                    adminClient.deletePartitions(stealInfo.getDonorId(),
                                                 storeName,
                                                 stealInfo.getPartitionList(),
                                                 null);
                    logger.info("Deleted partitions " + stealInfo.getPartitionList()
                                + " from donorNode:" + stealInfo.getDonorId());
                }

                updateStatus("partitions migration for store:" + storeName + " completed.");

                // reset asyncId
                migratePartitionsAsyncId = -1;
                currentStore = null;
            }

            @Override
            @JmxGetter(name = "asyncTaskStatus")
            public AsyncOperationStatus getStatus() {
                if(-1 != migratePartitionsAsyncId && null != currentStore)
                    try {
                        updateStatus("partitions migration for store:" + currentStore + " status:"
                                     + asyncRunner.getStatus(migratePartitionsAsyncId));
                    } catch(Exception e) {
                        // ignore
                    }

                return super.getStatus();
            }
        });

        return requestId;
    }

    private void setRebalancingState(MetadataStore metadataStore, RebalancePartitionsInfo stealInfo) {
        metadataStore.put(MetadataStore.SERVER_STATE_KEY, VoldemortState.REBALANCING_MASTER_SERVER);
        metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, stealInfo);
    }

    private void checkCurrentState(MetadataStore metadataStore, RebalancePartitionsInfo stealInfo) {
        if(metadataStore.getServerState().equals(VoldemortState.REBALANCING_MASTER_SERVER)
           && metadataStore.getRebalancingStealInfo().getDonorId() != stealInfo.getDonorId())
            throw new VoldemortException("Server " + metadataStore.getNodeId()
                                         + " is already rebalancing from:"
                                         + metadataStore.getRebalancingStealInfo()
                                         + " rejecting rebalance request:" + stealInfo);
    }
}