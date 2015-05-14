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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceBatchPlanProgressBar;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.utils.RebalanceUtils;

public abstract class RebalanceTask implements Runnable {

    protected final int batchId;
    protected final int taskId;
    protected final List<RebalanceTaskInfo> stealInfos;
    protected final Semaphore donorPermit;
    protected final AdminClient adminClient;
    protected final RebalanceBatchPlanProgressBar progressBar;
    protected final Logger loggerToUse;

    protected Exception exception;
    protected final AtomicBoolean isComplete;

    protected final int partitionStoreCount;
    protected long permitAcquisitionTimeMs;
    protected long taskCompletionTimeMs;

    protected final static int INVALID_REBALANCE_ID = -1;

    public RebalanceTask(final int batchId,
                         final int taskId,
                         final List<RebalanceTaskInfo> stealInfos,
                         final Semaphore donorPermit,
                         final AdminClient adminClient,
                         final RebalanceBatchPlanProgressBar progressBar,
                         final Logger logger) {
        this.batchId = batchId;
        this.taskId = taskId;
        this.stealInfos = stealInfos;
        this.donorPermit = donorPermit;
        this.adminClient = adminClient;
        this.progressBar = progressBar;
        this.loggerToUse = logger;

        this.exception = null;
        this.isComplete = new AtomicBoolean(false);

        this.partitionStoreCount = RebalanceUtils.countTaskStores(stealInfos);
        this.permitAcquisitionTimeMs = -1;
        this.taskCompletionTimeMs = -1;
    }

    public List<RebalanceTaskInfo> getStealInfos() {
        return this.stealInfos;
    }

    public boolean isComplete() {
        return this.isComplete.get();
    }

    public boolean hasException() {
        return exception != null;
    }

    public Exception getError() {
        return exception;
    }

    protected void acquirePermit(int nodeId) throws InterruptedException {
        permitStart(nodeId);
        boolean isPermitAcquired = false;
        while(isPermitAcquired == false) {
            isPermitAcquired = donorPermit.tryAcquire(5, TimeUnit.MINUTES);
            if(isPermitAcquired == false) {
                logPermitStatus(nodeId, "Waiting to acquire log ");
            }
        }
        logPermitStatus(nodeId, "Acquired donor permit for node ");
    }

    @Override
    public String toString() {
        return "Rebalance task " + taskId + " from batch " + batchId + " : " + getStealInfos();
    }

    /**
     * Helper method to log updates in uniform manner that includes batch & task
     * ID.
     * 
     * @param message
     */
    protected void taskLog(String message) {
        RebalanceUtils.printBatchTaskLog(batchId, taskId, loggerToUse, message);
    }

    /**
     * Helper method to pretty print progress and timing info.
     * 
     * @param nodeId node ID for which donor permit is required
     */
    protected void permitStart(int nodeId) {
        permitAcquisitionTimeMs = System.currentTimeMillis();
        taskLog("Acquiring donor permit for node " + nodeId + ".");
    }

    /**
     * Helper method to pretty print progress and timing info.
     * 
     * @param nodeId node ID for which donor permit is required
     */
    protected void logPermitStatus(int nodeId, String prefix) {
        String durationString = "";
        if(permitAcquisitionTimeMs >= 0) {
            long durationMs = System.currentTimeMillis() - permitAcquisitionTimeMs;
            permitAcquisitionTimeMs = -1;
            durationString = " in " + TimeUnit.MILLISECONDS.toSeconds(durationMs) + " seconds.";
        }
        taskLog(prefix + nodeId + durationString);
    }

    /**
     * Helper method to pretty print progress and timing info.
     * 
     * @param rebalanceAsyncId ID of the async rebalancing task
     */
    protected void taskStart(int taskId, int rebalanceAsyncId) {
        taskCompletionTimeMs = System.currentTimeMillis();
        taskLog("[TaskId : " + taskId + " ] Starting rebalance of " + partitionStoreCount
                + " partition-stores for async operation id " + rebalanceAsyncId + ".");
        progressBar.beginTask(taskId);
    }

    /**
     * Helper method to pretty print progress and timing info.
     * 
     * @param rebalanceAsyncId ID of the async rebalancing task
     */
    protected void taskDone(int taskId, int rebalanceAsyncId) {
        String durationString = "";
        if(taskCompletionTimeMs >= 0) {
            long durationMs = System.currentTimeMillis() - taskCompletionTimeMs;
            taskCompletionTimeMs = -1;
            durationString = " in " + TimeUnit.MILLISECONDS.toSeconds(durationMs) + " seconds.";
        }
        taskLog("[TaskId : " + taskId + " ] Successfully finished rebalance of "
                + partitionStoreCount
                + " for async operation id " + rebalanceAsyncId + durationString);

        progressBar.completeTask(taskId, partitionStoreCount);
    }
}
