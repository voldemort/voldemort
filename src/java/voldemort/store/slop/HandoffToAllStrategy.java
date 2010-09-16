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

package voldemort.store.slop;

import com.google.common.collect.Lists;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.Collections;
import java.util.List;

/**
 * A strategy that hands a hint off to any random live node in the ring
 */
public class HandoffToAllStrategy implements HintedHandoffStrategy {

    private final List<Node> nodes;

    /**
     * Creates a to-all handoff strategy instance
     * @param cluster The cluster
     */
    public HandoffToAllStrategy(Cluster cluster) {
        nodes = Lists.newArrayList(cluster.getNodes());
    }

    public List<Node> routeHint(Node origin) {
        List<Node> prefList = Lists.newArrayListWithCapacity(nodes.size());
        for(Node node: nodes) {
            if(node.getId() != origin.getId())
                prefList.add(node);
        }
        Collections.shuffle(prefList);
        return prefList;
    }

    @Override
    public String toString() {
        return "HandoffToAllStrategy(" + nodes + ")";
    }
}
