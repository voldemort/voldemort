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

import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceBatchPlanProgressBar;
import voldemort.client.rebalance.RebalanceScheduler;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.store.UnreachableStoreException;

import com.google.common.collect.Lists;

/**
 * Immutable class that executes a {@link RebalanceTaskInfo} instance on
 * the rebalance client side.
 * 
 * This is run from the stealer nodes perspective
 */
public class StealerBasedRebalanceTask extends RebalanceTask {

    private static final Logger logger = Logger.getLogger(StealerBasedRebalanceTask.class);

    private final int stealerNodeId;
    private final int donorNodeId;

    private final RebalanceScheduler scheduler;

    public StealerBasedRebalanceTask(final int batchId,
                                     final int taskId,
                                     final RebalanceTaskInfo stealInfo,
                                     final Semaphore donorPermit,
                                     final AdminClient adminClient,
                                     final RebalanceBatchPlanProgressBar progressBar,
                                     final RebalanceScheduler scheduler) {
        super(batchId,
              taskId,
              Lists.newArrayList(stealInfo),
              donorPermit,
              adminClient,
              progressBar,
              logger);

        this.stealerNodeId = stealInfo.getStealerId();
        this.donorNodeId = stealInfo.getDonorId();
        this.scheduler = scheduler;

        taskLog(toString());
    }

    private int startNodeRebalancing() {
        try {
            taskLog("Trying to start async rebalance task on stealer node " + stealerNodeId);
            int asyncOperationId = adminClient.rebalanceOps.rebalanceNode(stealInfos.get(0));
            taskLog("Started async rebalance task on stealer node " + stealerNodeId);

            return asyncOperationId;

        } catch(AlreadyRebalancingException e) {
            String errorMessage = "Node "
                                  + stealerNodeId
                                  + " is currently rebalancing. Should not have tried to start new task on stealer node!";
            taskLog(errorMessage);
            throw new VoldemortException(errorMessage + " Failed to start rebalancing with plan: "
                                         + getStealInfos(), e);
        }
    }

    @Override
    public void run() {
        int rebalanceAsyncId = INVALID_REBALANCE_ID;

        try {
            acquirePermit(stealInfos.get(0).getDonorId());

            // Start rebalance task and then wait.
            rebalanceAsyncId = startNodeRebalancing();
            taskStart(rebalanceAsyncId);

            adminClient.rpcOps.waitForCompletion(stealerNodeId, rebalanceAsyncId);
            taskDone(rebalanceAsyncId);

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
            scheduler.doneTask(stealerNodeId, donorNodeId);
        }
    }

    @Override
    public String toString() {
        return "Stealer based rebalance task on stealer node " + stealerNodeId
               + " from donor node " + donorNodeId + " : " + getStealInfos();
    }
}
