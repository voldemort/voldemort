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
import java.util.concurrent.atomic.AtomicBoolean;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;

public abstract class RebalanceTask implements Runnable {

    protected final int taskId;
    protected Exception exception;
    protected final AdminClient adminClient;
    protected final Semaphore donorPermit;
    protected final AtomicBoolean isComplete;
    protected final List<RebalancePartitionsInfo> stealInfos;

    protected final static int INVALID_REBALANCE_ID = -1;

    public RebalanceTask(final int taskId,
                         final List<RebalancePartitionsInfo> stealInfos,
                         final Semaphore donorPermit,
                         final AdminClient adminClient) {
        this.stealInfos = stealInfos;
        this.taskId = taskId;
        this.adminClient = adminClient;
        this.donorPermit = donorPermit;
        this.exception = null;
        this.isComplete = new AtomicBoolean(false);
    }

    public List<RebalancePartitionsInfo> getStealInfos() {
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

    @Override
    public String toString() {
        return "Rebalance task : " + getStealInfos();
    }

}
