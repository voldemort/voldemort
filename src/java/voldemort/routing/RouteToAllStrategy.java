/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import voldemort.cluster.Node;

/**
 * A routing strategy which just routes each request to all the nodes given.
 * 
 * Pretend that there is one partition ID "0" that all keys map to and that is
 * replicated on all nodes.
 * 
 */
public class RouteToAllStrategy implements RoutingStrategy {

    final private Collection<Node> nodes;
    final private ArrayList<Integer> partitionIds;

    // Use partition ID 0 for all keys to implement route to all strategy.
    final static private int ROUTE_TO_ALL_PARTITION_ID = 0;

    public RouteToAllStrategy(Collection<Node> nodes) {
        this.nodes = nodes;
        this.partitionIds = new ArrayList<Integer>(1);
        this.partitionIds.add(ROUTE_TO_ALL_PARTITION_ID);
    }

    @Override
    public int getNumReplicas() {
        return nodes.size();
    }

    @Override
    public List<Node> routeRequest(byte[] key) {
        return new ArrayList<Node>(nodes);
    }

    @Override
    public Set<Node> getNodes() {
        return new HashSet<Node>(nodes);
    }

    @Override
    public List<Integer> getPartitionList(byte[] key) {
        return partitionIds;
    }

    @Override
    public List<Integer> getReplicatingPartitionList(int partitionId) {
        return partitionIds;
    }

    /**
     * Obtain the master partition for a given key
     * 
     * @param key
     * @return master partition id
     */
    @Override
    public Integer getMasterPartition(byte[] key) {
        return ROUTE_TO_ALL_PARTITION_ID;
    }

    @Override
    public String getType() {
        return RoutingStrategyType.TO_ALL_STRATEGY;
    }
}
