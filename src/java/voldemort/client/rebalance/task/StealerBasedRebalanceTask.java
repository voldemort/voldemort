package voldemort.client.rebalance.task;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.Lists;

/**
 * Immutable class that executes a {@link RebalancePartitionsInfo} instance on
 * the rebalance client side.
 * 
 * This is run from the stealer nodes perspective
 */
public class StealerBasedRebalanceTask extends RebalanceTask {

    private static final Logger logger = Logger.getLogger(StealerBasedRebalanceTask.class);

    private final int stealerNodeId;

    public StealerBasedRebalanceTask(final int taskId,
                                     final RebalancePartitionsInfo stealInfo,
                                     final RebalanceClientConfig config,
                                     final Semaphore donorPermit,
                                     final AdminClient adminClient) {
        super(taskId, Lists.newArrayList(stealInfo), config, donorPermit, adminClient);
        this.stealerNodeId = stealInfo.getStealerId();
    }

    private int startNodeRebalancing() {
        int nTries = 0;
        AlreadyRebalancingException rebalanceException = null;

        while(nTries < config.getMaxTriesRebalancing()) {
            nTries++;
            try {

                RebalanceUtils.printLog(taskId, logger, "Starting on node " + stealerNodeId
                                                        + " rebalancing task " + stealInfos.get(0));

                int asyncOperationId = adminClient.rebalanceOps.rebalanceNode(stealInfos.get(0));
                return asyncOperationId;

            } catch(AlreadyRebalancingException e) {
                RebalanceUtils.printLog(taskId,
                                        logger,
                                        "Node "
                                                + stealerNodeId
                                                + " is currently rebalancing. Waiting till completion");
                adminClient.rpcOps.waitForCompletion(stealerNodeId,
                                                     MetadataStore.SERVER_STATE_KEY,
                                                     VoldemortState.NORMAL_SERVER.toString(),
                                                     config.getRebalancingClientTimeoutSeconds(),
                                                     TimeUnit.SECONDS);
                rebalanceException = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing with plan: " + getStealInfos(),
                                     rebalanceException);
    }

    public void run() {
        int rebalanceAsyncId = INVALID_REBALANCE_ID;

        try {
            RebalanceUtils.printLog(taskId, logger, "Acquiring donor permit for node "
                                                    + stealInfos.get(0).getDonorId() + " for "
                                                    + stealInfos);
            donorPermit.acquire();

            rebalanceAsyncId = startNodeRebalancing();

            // Wait for the task to get over
            adminClient.rpcOps.waitForCompletion(stealerNodeId,
                                                 rebalanceAsyncId,
                                                 config.getRebalancingClientTimeoutSeconds(),
                                                 TimeUnit.SECONDS);
            RebalanceUtils.printLog(taskId,
                                    logger,
                                    "Succesfully finished rebalance for async operation id "
                                            + rebalanceAsyncId);

        } catch(UnreachableStoreException e) {
            exception = e;
            logger.error("Stealer node " + stealerNodeId
                                 + " is unreachable, please make sure it is up and running : "
                                 + e.getMessage(),
                         e);
        } catch(Exception e) {
            exception = e;
            logger.error("Rebalance failed : " + e.getMessage(), e);
        } finally {
            donorPermit.release();
            isComplete.set(true);
        }
    }

    @Override
    public String toString() {
        return "Stealer based rebalance task on stealer node " + stealerNodeId + " : "
               + getStealInfos();
    }
}