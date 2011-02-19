/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.slop.strategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A strategy which hands a hint off to any one of N nodes adjacent to the
 * failed node in the ring, the list of the N nodes being static
 */
public class ConsistentHandoffStrategy implements HintedHandoffStrategy {

    private final Map<Integer, List<Node>> routeToMap;

    /**
     * Creates a consistent handoff strategy instance
     * 
     * @param cluster The cluster
     * @param prefListSize The number of nodes adjacent to the failed node in
     *        the that could be selected to receive given hint
     * @param enableZoneRouting is zone routing enabled?
     * @param clientZoneId client zone id if zone routing is enabled
     */
    public ConsistentHandoffStrategy(Cluster cluster,
                                     int prefListSize,
                                     boolean enableZoneRouting,
                                     int clientZoneId) {
        int nodesInCluster = cluster.getNumberOfNodes();
        if(prefListSize > nodesInCluster - 1)
            throw new IllegalArgumentException("Preference list size must be less than "
                                               + "number of nodes in the cluster - 1");

        routeToMap = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        for(Node node: cluster.getNodes()) {
            List<Node> prefList = Lists.newArrayListWithCapacity(prefListSize);
            int i = node.getId();
            int n = 0;
            while(n < prefListSize) {
                i = (i + 1) % cluster.getNumberOfNodes();
                Node peer = cluster.getNodeById(i);
                if(peer.getId() != node.getId()) {
                    if(enableZoneRouting && cluster.getZones().size() > 1) {
                        // don't handoff hints to the same zone
                        int zoneId = node.getZoneId();
                        if(clientZoneId == zoneId) {
                            if(peer.getZoneId() != zoneId)
                                continue;
                        } else {
                            if(peer.getZoneId() == zoneId)
                                continue;
                        }
                    }
                    prefList.add(peer);
                    n++;
                }
                routeToMap.put(node.getId(), prefList);
            }
        }
    }

    public List<Node> routeHint(Node origin) {
        List<Node> prefList = Lists.newArrayList(routeToMap.get(origin.getId()));
        Collections.shuffle(prefList);
        return prefList;
    }

    @Override
    public String toString() {
        return "ConsistentHandoffStrategy(" + routeToMap.toString() + ")";
    }
}
