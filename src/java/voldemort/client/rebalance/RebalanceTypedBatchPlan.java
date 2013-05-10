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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;

// TODO: Rename to ExecutableRebalanceBatch
public abstract class RebalanceTypedBatchPlan {

    protected final RebalanceClusterPlan rebalanceClusterPlan;
    // Construct store routing plans so that task order can be optimized.
    private final Map<String, StoreRoutingPlan> storeToRoutingPlan;

    protected final Queue<RebalanceNodePlan> rebalanceTaskQueue;

    public RebalanceTypedBatchPlan(final RebalanceClusterPlan rebalanceClusterPlan) {
        this.rebalanceClusterPlan = rebalanceClusterPlan;
        this.storeToRoutingPlan = new HashMap<String, StoreRoutingPlan>();
        for(StoreDefinition storeDef: rebalanceClusterPlan.getStoreDefs()) {
            this.storeToRoutingPlan.put(storeDef.getName(),
                                        new StoreRoutingPlan(rebalanceClusterPlan.getFinalCluster(),
                                                             storeDef));
        }

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

    // TODO: Change method name once types have better names.
    // TODO: add javadoc
    // Take an unorderd list of tasks and order them by zone n-ary for sake of
    // prioritizing zone primaries ahead of zone secondaries ahead of ...
    protected List<RebalancePartitionsInfo> sortTasks(int nodeId,
                                                      List<RebalancePartitionsInfo> tasks) {
        int zoneId = rebalanceClusterPlan.getFinalCluster().getNodeById(nodeId).getZoneId();
        for(RebalancePartitionsInfo task: tasks) {
            Map<Integer, List<RebalancePartitionsInfo>> zoneNaryToTasks = new HashMap<Integer, List<RebalancePartitionsInfo>>();
            for(String storeName: storeToRoutingPlan.keySet()) {
                StoreRoutingPlan storeRoutingPlan = storeToRoutingPlan.get(storeName);
                List<Integer> partitionIds = task.getPartitionIds(storeName);
                for(Integer partitionId: partitionIds) {
                    int zoneNaryType = storeRoutingPlan.getZoneReplicaType(zoneId,
                                                                           nodeId,
                                                                           partitionId);
                    if(!zoneNaryToTasks.containsKey(zoneNaryType)) {
                        zoneNaryToTasks.put(zoneNaryType, new ArrayList<RebalancePartitionsInfo>());
                    }
                    List<RebalancePartitionsInfo> naryTasks = zoneNaryToTasks.get(zoneNaryType);
                    // naryTasks. .add(partitionId);
                }
            }
        }
        return null;
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
