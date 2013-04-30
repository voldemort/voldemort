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

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;

public abstract class RebalanceTypedBatchPlan extends RebalanceClusterPlan {

    protected final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    public RebalanceTypedBatchPlan(final Cluster currentCluster,
                                   final Cluster targetCluster,
                                   final List<StoreDefinition> storeDefs,
                                   final boolean enabledDeletePartition) {
        super(currentCluster, targetCluster, storeDefs, enabledDeletePartition, true);

        // TODO: Why does this data structure need to be concurrent!? I have
        // cut-and-paste this construction from prior code. But, if this needs
        // to be concurrent-safe, then the declaration of the getRebaalncePlan
        // methods ought to indicate that a concurrent safe struct is being
        // returned.
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        // Sub-classes populate this data member in their constructor
    }

    // TODO: drop the "TWO" suffix after this method has been removed from
    // parent class RebalanceClusterPlan. This will happen as part of switching
    // the rebalance controller over to use RebalancePlanner.
    public Queue<RebalanceNodePlan> getRebalancingTaskQueueTWO() {
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
