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

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GetAllConfigureNodes extends
        AbstractConfigureNodes<Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    protected final int preferred;

    protected final Iterable<ByteArray> keys;

    public GetAllConfigureNodes(GetAllPipelineData pipelineData,
                                Event completeEvent,
                                FailureDetector failureDetector,
                                int preferred,
                                int required,
                                RoutingStrategy routingStrategy,
                                Iterable<ByteArray> keys) {
        super(pipelineData, completeEvent, failureDetector, required, routingStrategy);
        this.preferred = preferred;
        this.keys = keys;
    }

    public void execute(Pipeline pipeline, Object eventData) {
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
    }

}
