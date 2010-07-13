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

package voldemort.store.routed;

import java.util.HashSet;
import java.util.List;

import voldemort.cluster.Node;
import voldemort.utils.ByteArray;

public class BasicPipelineData<V> extends PipelineData<ByteArray, V> {

    private List<Node> nodes;

    private int nodeIndex;

    private int successes;

    private HashSet<Integer> zoneResponses;

    private Integer zonesRequired;

    public BasicPipelineData() {
        super();
        zoneResponses = new HashSet<Integer>();
    }

    public HashSet<Integer> getZoneResponses() {
        return zoneResponses;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public void incrementNodeIndex() {
        nodeIndex++;
    }

    public int getSuccesses() {
        return successes;
    }

    public void incrementSuccesses() {
        successes++;
    }

    public void setZonesRequired(Integer zonesRequired) {
        this.zonesRequired = zonesRequired;
    }

    public Integer getZonesRequired() {
        return this.zonesRequired;
    }

}
