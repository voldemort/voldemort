package voldemort.client.rebalance.task;

import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.RebalanceUtils;

/**
 * Immutable class that executes a {@link RebalancePartitionsInfo} instance on
 * the rebalance client side
 * 
 * This is run from the donor nodes perspective
 */
public class DonorBasedRebalanceTask extends RebalanceTask {

    protected static final Logger logger = Logger.getLogger(DonorBasedRebalanceTask.class);

    private final int donorNodeId;

    public DonorBasedRebalanceTask(final int taskId,
                                   final List<RebalancePartitionsInfo> stealInfos,
                                   final Semaphore donorPermit,
                                   final AdminClient adminClient) {
        super(taskId, stealInfos, donorPermit, adminClient);
        RebalanceUtils.assertSameDonor(stealInfos, -1);
        this.donorNodeId = stealInfos.get(0).getDonorId();
    }

    @Override
    public void run() {
        int rebalanceAsyncId = INVALID_REBALANCE_ID;

        try {
            RebalanceUtils.printLog(taskId, logger, "Acquiring donor permit for node "
                                                    + donorNodeId + " for " + stealInfos);
            donorPermit.acquire();

            RebalanceUtils.printLog(taskId, logger, "Starting on node " + donorNodeId
                                                    + " rebalancing task " + stealInfos);
            rebalanceAsyncId = adminClient.rebalanceOps.rebalanceNode(stealInfos);

            adminClient.rpcOps.waitForCompletion(donorNodeId, rebalanceAsyncId);
            RebalanceUtils.printLog(taskId,
                                    logger,
                                    "Succesfully finished rebalance for async operation id "
                                            + rebalanceAsyncId);

        } catch(UnreachableStoreException e) {
            exception = e;
            logger.error("Donor node " + donorNodeId
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
        return "Donor based rebalance task on donor node " + donorNodeId + " : " + getStealInfos();
    }
}