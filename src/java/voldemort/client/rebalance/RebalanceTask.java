package voldemort.client.rebalance;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

/**
 * Immutable class that executes a {@link RebalancePartitionsInfo} instance on
 * the rebalance client side
 * 
 */
class RebalanceTask implements Runnable {

    private static final Logger logger = Logger.getLogger(RebalanceTask.class);

    private final static int INVALID_REBALANCE_ID = -1;

    private final RebalancePartitionsInfo stealInfo;

    private Exception exception;
    private final RebalanceClientConfig config;
    private final AdminClient adminClient;

    public RebalanceTask(final RebalancePartitionsInfo stealInfo,
                         final RebalanceClientConfig config,
                         final AdminClient adminClient) {
        this.stealInfo = stealInfo;
        this.config = config;
        this.adminClient = adminClient;
        this.exception = null;
    }

    public boolean hasException() {
        return exception != null;
    }

    public Exception getError() {
        return exception;
    }

    public RebalancePartitionsInfo getRebalancePartitionsInfo() {
        return this.stealInfo;
    }

    private int startNodeRebalancing(RebalancePartitionsInfo stealInfo) {
        int nTries = 0;
        AlreadyRebalancingException rebalanceException = null;

        while(nTries < config.getMaxTriesRebalancing()) {
            nTries++;
            try {

                logger.debug("Rebalancing node for plan: " + stealInfo);
                int asyncOperationId = adminClient.rebalanceNode(stealInfo);
                logger.debug("Async operation id: " + asyncOperationId + " for plan: " + stealInfo);
                return asyncOperationId;

            } catch(AlreadyRebalancingException e) {
                RebalanceUtils.printLog(stealInfo.getStealerId(),
                                        logger,
                                        "Node is currently rebalancing. Waiting till completion");
                adminClient.waitForCompletion(stealInfo.getStealerId(),
                                              MetadataStore.SERVER_STATE_KEY,
                                              VoldemortState.NORMAL_SERVER.toString(),
                                              config.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                rebalanceException = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing with plan: " + stealInfo,
                                     rebalanceException);
    }

    public void run() {
        int rebalanceAsyncId = INVALID_REBALANCE_ID;
        final int stealerNodeId = stealInfo.getStealerId();

        try {
            rebalanceAsyncId = startNodeRebalancing(stealInfo);

            // Wait for the task to get over
            adminClient.waitForCompletion(stealInfo.getStealerId(),
                                          rebalanceAsyncId,
                                          config.getRebalancingClientTimeoutSeconds(),
                                          TimeUnit.SECONDS);
            RebalanceUtils.printLog(stealInfo.getStealerId(),
                                    logger,
                                    "Succesfully finished rebalance for async operation id: "
                                            + rebalanceAsyncId);

        } catch(UnreachableStoreException e) {
            exception = e;
            logger.error("StealerNode " + stealerNodeId
                         + " is unreachable, please make sure it is up and running - "
                         + e.getMessage(), e);
        } catch(Exception e) {
            exception = e;
            logger.error("Rebalance failed: " + e.getMessage(), e);
        }
    }
}