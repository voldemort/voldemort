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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import voldemort.cluster.Node;

/**
 * A routing strategy which just routes each request to all the nodes given
 * 
 * 
 */
public class RouteToAllStrategy implements RoutingStrategy {

    private Collection<Node> nodes;

    public RouteToAllStrategy(Collection<Node> nodes) {
        this.nodes = nodes;
    }

    public int getNumReplicas() {
        return nodes.size();
    }

    public List<Node> routeRequest(byte[] key) {
        return new ArrayList<Node>(nodes);
    }

    public Set<Node> getNodes() {
        return new HashSet<Node>(nodes);
    }

    public List<Integer> getPartitionList(byte[] key) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public List<Integer> getReplicatingPartitionList(int partitionId) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    /**
     * Obtain the master partition for a given key
     * 
     * @param key
     * @return
     */
    public Integer getMasterPartition(byte[] key) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public String getType() {
        return RoutingStrategyType.TO_ALL_STRATEGY;
    }
}
