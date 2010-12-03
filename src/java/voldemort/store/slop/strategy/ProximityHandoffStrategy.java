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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A preference list based handoff strategy which stores multi-zone information
 */
public class ProximityHandoffStrategy implements HintedHandoffStrategy {

    private final Cluster cluster;
    private final int clientZoneId;
    private Map<Integer, List<Node>> zoneMapping;

    /**
     * Constructor which makes zone based mapping
     * 
     * @param cluster The cluster
     * @param clientZoneId Client zone id
     */
    public ProximityHandoffStrategy(Cluster cluster, int clientZoneId) {
        this.cluster = cluster;
        this.clientZoneId = clientZoneId;

        // Generate a mapping from zone to nodes
        zoneMapping = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            List<Node> nodes = zoneMapping.get(node.getZoneId());
            if(nodes == null) {
                nodes = Lists.newArrayList();
                zoneMapping.put(node.getZoneId(), nodes);
            }
            nodes.add(node);
        }

    }

    public List<Node> routeHint(Node origin) {
        List<Node> proximityList = new ArrayList<Node>();

        // Add the client zone id
        Collections.shuffle(zoneMapping.get(clientZoneId));
        proximityList.addAll(zoneMapping.get(clientZoneId));

        for(Integer zoneId: cluster.getZoneById(clientZoneId).getProximityList()) {
            Collections.shuffle(zoneMapping.get(zoneId));
            proximityList.addAll(zoneMapping.get(zoneId));
        }

        // Remove the origin node
        proximityList.remove(origin);
        return proximityList;
    }

    @Override
    public String toString() {
        return "ProximityHandoffStrategy(" + cluster.getNodes() + ")";
    }
}
