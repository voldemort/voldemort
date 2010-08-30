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

package voldemort.store.routed.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GetAllConfigureNodes
        extends
        AbstractConfigureNodes<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    private final int preferred;

    private final Iterable<ByteArray> keys;

    private final Zone clientZone;

    private final Map<ByteArray, byte[]> transforms;

    public GetAllConfigureNodes(GetAllPipelineData pipelineData,
                                Event completeEvent,
                                FailureDetector failureDetector,
                                int preferred,
                                int required,
                                RoutingStrategy routingStrategy,
                                Iterable<ByteArray> keys,
                                Map<ByteArray, byte[]> transforms,
                                Zone clientZone) {
        super(pipelineData, completeEvent, failureDetector, required, routingStrategy);
        this.preferred = preferred;
        this.keys = keys;
        this.transforms = transforms;
        this.clientZone = clientZone;
    }

    public void execute(Pipeline pipeline) {
        Map<Node, List<ByteArray>> nodeToKeysMap = Maps.newHashMap();
        Map<ByteArray, List<Node>> keyToExtraNodesMap = Maps.newHashMap();

        for(ByteArray key: keys) {
            List<Node> nodes = null;

            try {
                nodes = getNodes(key);
            } catch(VoldemortException e) {
                pipelineData.setFatalError(e);
                pipeline.addEvent(Event.ERROR);
                return;
            }

            List<Node> preferredNodes = Lists.newArrayListWithCapacity(preferred);
            List<Node> extraNodes = Lists.newArrayListWithCapacity(3);

            if(pipelineData.getZonesRequired() != null) {

                if(pipelineData.getZonesRequired() > this.clientZone.getProximityList().size()) {
                    throw new VoldemortException("Number of zones required should be less than the total number of zones");
                }

                if(pipelineData.getZonesRequired() > required) {
                    throw new VoldemortException("Number of zones required should be less than the required number of "
                                                 + pipeline.getOperation().getSimpleName() + "s");
                }

                // Create zone id to node mapping
                Map<Integer, List<Node>> zoneIdToNode = new HashMap<Integer, List<Node>>();
                for(Node node: nodes) {
                    List<Node> nodesList = null;
                    if(zoneIdToNode.containsKey(node.getZoneId())) {
                        nodesList = zoneIdToNode.get(node.getZoneId());
                    } else {
                        nodesList = new ArrayList<Node>();
                        zoneIdToNode.put(node.getZoneId(), nodesList);
                    }
                    nodesList.add(node);
                }

                nodes = new ArrayList<Node>();
                LinkedList<Integer> proximityList = this.clientZone.getProximityList();
                // Add a node from every zone
                for(int index = 0; index < pipelineData.getZonesRequired(); index++) {
                    List<Node> zoneNodes = zoneIdToNode.get(proximityList.get(index));
                    if(zoneNodes != null) {
                        nodes.add(zoneNodes.remove(0));
                    }
                }

                // Add the rest
                nodes.addAll(zoneIdToNode.get(this.clientZone.getId()));
                for(int index = 0; index < proximityList.size(); index++) {
                    List<Node> zoneNodes = zoneIdToNode.get(proximityList.get(index));
                    if(zoneNodes != null)
                        nodes.addAll(zoneNodes);
                }

            }

            for(Node node: nodes) {
                if(preferredNodes.size() < preferred)
                    preferredNodes.add(node);
                else
                    extraNodes.add(node);
            }

            for(Node node: preferredNodes) {
                List<ByteArray> nodeKeys = nodeToKeysMap.get(node);

                if(nodeKeys == null) {
                    nodeKeys = Lists.newArrayList();
                    nodeToKeysMap.put(node, nodeKeys);
                }

                nodeKeys.add(key);
            }

            if(!extraNodes.isEmpty()) {
                List<Node> list = keyToExtraNodesMap.get(key);

                if(list == null)
                    keyToExtraNodesMap.put(key, extraNodes);
                else
                    list.addAll(extraNodes);
            }
        }

        pipelineData.setKeyToExtraNodesMap(keyToExtraNodesMap);
        pipelineData.setNodeToKeysMap(nodeToKeysMap);
        pipelineData.setTransforms(transforms);

        pipeline.addEvent(completeEvent);
    }

}
