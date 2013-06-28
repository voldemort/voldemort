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

package voldemort.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;

/**
 * This class wraps up a Cluster object and a StoreDefinition. The methods are
 * effectively helper or util style methods for querying the routing plan that
 * will be generated for a given routing strategy upon store and cluster
 * topology information.
 */
public class StoreRoutingPlan extends BaseStoreRoutingPlan {

    private final Map<Integer, List<Integer>> nodeIdToNaryPartitionMap;
    private final Map<Integer, List<Integer>> nodeIdToZonePrimaryMap;

    public StoreRoutingPlan(Cluster cluster, StoreDefinition storeDefinition) {
        super(cluster, storeDefinition);
        this.nodeIdToNaryPartitionMap = new HashMap<Integer, List<Integer>>();
        this.nodeIdToZonePrimaryMap = new HashMap<Integer, List<Integer>>();
        for (int nodeId : cluster.getNodeIds()) {
            this.nodeIdToNaryPartitionMap.put(nodeId, new ArrayList<Integer>());
            this.nodeIdToZonePrimaryMap.put(nodeId, new ArrayList<Integer>());
        }
        for (int masterPartitionId = 0; masterPartitionId < cluster.getNumberOfPartitions(); ++masterPartitionId) {
            List<Integer> naryPartitionIds = getReplicatingPartitionList(masterPartitionId);
            for (int naryPartitionId : naryPartitionIds) {
                int naryNodeId = getNodeIdForPartitionId(naryPartitionId);
                this.nodeIdToNaryPartitionMap.get(naryNodeId)
                                             .add(masterPartitionId);
            }
        }
        for (int nodeId : cluster.getNodeIds()) {
            int naryZoneId = cluster.getNodeById(nodeId).getZoneId();
            List<Integer> naryPartitionIds = this.nodeIdToNaryPartitionMap.get(nodeId);
            List<Integer> zoneNAries = this.nodeIdToZonePrimaryMap.get(nodeId);
            for (int naryPartitionId : naryPartitionIds) {
                if (getZoneNaryForNodesPartition(naryZoneId,
                                                 nodeId,
                                                 naryPartitionId) == 0) {
                    zoneNAries.add(naryPartitionId);
                }
            }
        }
    }

    /**
     * Returns all (zone n-ary) partition IDs hosted on the node.
     * 
     * @param nodeId
     * @return all zone n-ary partition IDs hosted on the node in an unordered
     *         list.
     */
    public List<Integer> getZoneNAryPartitionIds(int nodeId) {
        return nodeIdToNaryPartitionMap.get(nodeId);
    }

    /**
     * Returns all zone-primary partition IDs on node. A zone-primary means zone
     * n-ary==0. Zone-primary nodes are generally pseudo-masters in the zone and
     * receive get traffic for some partition Id.
     * 
     * @param nodeId
     * @return all primary partition IDs (zone n-ary == 0) hosted on the node.
     */
    public List<Integer> getZonePrimaryPartitionIds(int nodeId) {
        return nodeIdToZonePrimaryMap.get(nodeId);
    }
}
