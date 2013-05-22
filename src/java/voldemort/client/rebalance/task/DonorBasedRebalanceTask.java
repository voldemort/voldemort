/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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