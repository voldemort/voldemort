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
package voldemort.client.rebalance;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.utils.Utils;

// TODO: Better name? ExecutableBatchPlan?
public abstract class RebalanceTypedBatchPlan {

    protected final List<RebalancePartitionsInfo> batchPlan;
    protected final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    public RebalanceTypedBatchPlan(final RebalanceClusterPlan rebalanceClusterPlan) {
        this.batchPlan = rebalanceClusterPlan.getBatchPlan();

        // TODO: Why does this data structure need to be concurrent!? I have
        // cut-and-paste this construction from prior code. But, if this needs
        // to be concurrent-safe, then the declaration of the getRebaalncePlan
        // methods ought to indicate that a concurrent safe struct is being
        // returned. [if no value in concurrent, want to remove so that there is
        // no confusion about usage of this member.]

        // Sub-classes populate this data member in their constructor
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
    }

    /**
     * Returns the rebalancing task queue to be executed.
     * 
     * @return
     */
    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        return rebalanceTaskQueue;
    }

    @Override
    public String toString() {
        if(rebalanceTaskQueue.isEmpty()) {
            return "No rebalancing required since rebalance task queue is empty";
        }

        StringBuilder builder = new StringBuilder();
        for(RebalanceNodePlan nodePlan: rebalanceTaskQueue) {
            builder.append((nodePlan.isStealer() ? "Stealer " : "Donor ") + "Node "
                           + nodePlan.getNodeId());
            for(RebalancePartitionsInfo rebalancePartitionsInfo: nodePlan.getRebalanceTaskList()) {
                builder.append(rebalancePartitionsInfo).append(Utils.NEWLINE);
            }
        }

        return builder.toString();
    }
}
