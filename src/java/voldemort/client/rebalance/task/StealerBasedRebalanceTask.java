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
import voldemort.client.rebalance.RebalanceController;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;

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
    private final int donorNodeId;
    // TODO: What is the use of maxTries for stealer-based tasks? Need to
    // validate reason for existence or remove.
    // NOTES FROM VINOTH:
    // I traced the code down and it seems like this is basically used to
    // reissue StealerBasedRebalanceTask when it encounters an
    // AlreadyRebalancingException (which is tied to obtaining a rebalance
    // permit for the donor node) .. In general, I vote for removing this
    // parameter.. I think we should have the controller wait/block with a
    // decent log message if it truly blocked on other tasks to complete... But,
    // we need to check how likely this retry is saving us grief today and
    // probably stick to it for sometime, as we stabliize the code base with the
    // new planner/controller et al...Right way to do this.. Controller simply
    // submits "work" to the server and servers are mature enough to throttle
    // and process them as fast as they can. Since that looks like changing all
    // the server execution frameworks, let's stick with this for now..
    // TODO: Decide fate of maxTries argument after some integration tests are
    // done.
    private final int maxTries;

    private final RebalanceController.Scheduler scheduler;

    public StealerBasedRebalanceTask(final int batchId,
                                     final int taskId,
                                     final RebalancePartitionsInfo stealInfo,
                                     final int maxTries,
                                     final Semaphore donorPermit,
                                     final AdminClient adminClient,
                                     final RebalanceBatchPlanProgressBar progressBar,
                                     final RebalanceController.Scheduler scheduler) {
        super(batchId,
              taskId,
              Lists.newArrayList(stealInfo),
              donorPermit,
              adminClient,
              progressBar,
              logger);

        this.maxTries = maxTries;
        this.stealerNodeId = stealInfo.getStealerId();
        this.donorNodeId = stealInfo.getDonorId();

        this.scheduler = scheduler;

        taskLog(toString());
    }

    private int startNodeRebalancing() {
        int nTries = 0;
        AlreadyRebalancingException rebalanceException = null;

        while(nTries < maxTries) {
            nTries++;
            try {

                taskLog("Trying to start async rebalance task on stealer node " + stealerNodeId);
                int asyncOperationId = adminClient.rebalanceOps.rebalanceNode(stealInfos.get(0));
                taskLog("Started async rebalance task on stealer node " + stealerNodeId);

                return asyncOperationId;

            } catch(AlreadyRebalancingException e) {
                taskLog("Node " + stealerNodeId
                        + " is currently rebalancing. Waiting till completion");
                adminClient.rpcOps.waitForCompletion(stealerNodeId,
                                                     MetadataStore.SERVER_STATE_KEY,
                                                     VoldemortState.NORMAL_SERVER.toString());
                rebalanceException = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing with plan: " + getStealInfos(),
                                     rebalanceException);
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
        return "Stealer based rebalance task on stealer node " + stealerNodeId + " : "
               + getStealInfos();
    }
}