/*
 * Copyright 2008-2009 LinkedIn, Inc
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import voldemort.cluster.Node;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;

/**
 * A Zone Routing strategy that sits on top of the Consistent Routing strategy
 * such that each request goes to zone specific replicas
 * 
 */
public class ZoneRoutingStrategy extends ConsistentRoutingStrategy {

    private HashMap<Integer, Integer> zoneReplicationFactor;

    public ZoneRoutingStrategy(Collection<Node> nodes,
                               HashMap<Integer, Integer> zoneReplicationFactor,
                               int numReplicas) {
        this(new FnvHashFunction(), nodes, zoneReplicationFactor, numReplicas);
    }

    public ZoneRoutingStrategy(HashFunction hash,
                               Collection<Node> nodes,
                               HashMap<Integer, Integer> zoneReplicationFactor,
                               int numReplicas) {
        super(hash, nodes, numReplicas);
        this.zoneReplicationFactor = zoneReplicationFactor;
    }

    /**
     * Get the replication partitions list for the given partition.
     * 
     * @param index Partition id for which we are generating the preference list
     * @return The List of partitionId where this partition is replicated.
     */
    @Override
    public List<Integer> getReplicatingPartitionList(int index) {
        List<Node> preferenceNodesList = new ArrayList<Node>(getNumReplicas());
        List<Integer> replicationPartitionsList = new ArrayList<Integer>(getNumReplicas());

        // Copy Zone based Replication Factor
        HashMap<Integer, Integer> requiredRepFactor = new HashMap<Integer, Integer>();
        requiredRepFactor.putAll(zoneReplicationFactor);

        // Cross-check if individual zone replication factor equals global
        int sum = 0;
        for(Integer zoneRepFactor: requiredRepFactor.values()) {
            sum += zoneRepFactor;
        }

        if(sum != getNumReplicas())
            throw new IllegalArgumentException("Number of zone replicas is not equal to the total replication factor");

        if(getPartitionToNode().length == 0) {
            return new ArrayList<Integer>(0);
        }

        for(int i = 0; i < getPartitionToNode().length; i++) {
            // add this one if we haven't already, and it can satisfy some zone
            // replicationFactor
            Node currentNode = getNodeByPartition(index);
            if(!preferenceNodesList.contains(currentNode)) {
                preferenceNodesList.add(currentNode);
                if(checkZoneRequirement(requiredRepFactor, currentNode.getZoneId()))
                    replicationPartitionsList.add(index);
            }

            // if we have enough, go home
            if(replicationPartitionsList.size() >= getNumReplicas())
                return replicationPartitionsList;
            // move to next clockwise slot on the ring
            index = (index + 1) % getPartitionToNode().length;
        }

        // we don't have enough, but that may be okay
        return replicationPartitionsList;
    }

    /**
     * Check if we still need more nodes from the given zone and reduce the
     * zoneReplicationFactor count accordingly.
     * 
     * @param requiredRepFactor
     * @param zoneId
     * @return
     */
    private boolean checkZoneRequirement(HashMap<Integer, Integer> requiredRepFactor, int zoneId) {
        if(requiredRepFactor.containsKey(zoneId)) {
            if(requiredRepFactor.get(zoneId) == 0) {
                return false;
            } else {
                requiredRepFactor.put(zoneId, requiredRepFactor.get(zoneId) - 1);
                return true;
            }
        }
        return false;

    }

    @Override
    public String getType() {
        return RoutingStrategyType.ZONE_STRATEGY;
    }
}
