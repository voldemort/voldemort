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
 * A preference list based handoff strategy which stores multi-zone information
 */
public class ProximityHandoffStrategy implements HintedHandoffStrategy {

    private final Map<Integer, List<Node>> zoneMapping;
    private final Cluster cluster;

    /**
     * Constructor which makes zone based mapping
     * 
     * @param cluster The cluster
     * @param clientZoneId Client zone id
     */
    public ProximityHandoffStrategy(Cluster cluster) {
        this.cluster = cluster;
        this.zoneMapping = Maps.newHashMap();
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
        List<Node> prefList = Lists.newArrayList();
        prefList.addAll(zoneMapping.get(origin.getZoneId()));
        Collections.shuffle(prefList);
        if(cluster.getNumberOfZones() > 1) {
            for(Integer zoneId: cluster.getZoneById(origin.getZoneId()).getProximityList()) {
                List<Node> nodesInZone = zoneMapping.get(zoneId);
                Collections.shuffle(nodesInZone);
                prefList.addAll(nodesInZone);
            }
        }
        return prefList;
    }

    @Override
    public String toString() {
        return "ProximityHandoffStrategy(" + cluster.getNodes() + ")";
    }
}
