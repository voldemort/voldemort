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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;

import com.google.common.collect.Lists;

/**
 * A strategy that hands a hint off to any random live node in the ring
 */
public class HandoffToAnyStrategy implements HintedHandoffStrategy {

    private final List<Node> nodes;
    private final Collection<Zone> zones;
    private final boolean enableZoneRouting;
    private final int clientZoneId;

    /**
     * Creates a to-any handoff strategy instance
     * 
     * @param cluster The cluster
     * @param enableZoneRouting Is zone routing enabled
     * @param clientZoneId Client zone id
     */
    public HandoffToAnyStrategy(Cluster cluster, boolean enableZoneRouting, int clientZoneId) {
        this.nodes = Lists.newArrayList(cluster.getNodes());
        this.zones = cluster.getZones();
        this.enableZoneRouting = enableZoneRouting;
        this.clientZoneId = clientZoneId;
    }

    public List<Node> routeHint(Node origin) {
        List<Node> prefList = Lists.newArrayListWithCapacity(nodes.size());
        int originZoneId = origin.getZoneId();

        for(Node node: nodes) {
            if(node.getId() != origin.getId()) {
                if(enableZoneRouting && zones.size() > 1) {
                    if(originZoneId == clientZoneId) {
                        if(node.getZoneId() != clientZoneId)
                            continue;
                    } else {
                        if(node.getZoneId() == originZoneId)
                            continue;
                    }
                }
                prefList.add(node);
            }
        }
        Collections.shuffle(prefList);
        return prefList;
    }

    @Override
    public String toString() {
        return "HandoffToAllStrategy(" + nodes + ")";
    }
}
