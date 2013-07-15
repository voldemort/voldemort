/*
 * Copyright 2013 LinkedIn, Inc
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
import java.util.List;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

/**
 * Configure the Nodes obtained via the routing strategy based on the zone
 * information. Local zone nodes first, followed by the corresponding nodes from
 * each of the other zones, ordered by proximity.
 */
public class ConfigureNodesLocalZoneOnly<V, PD extends BasicPipelineData<V>> extends
        AbstractConfigureNodes<ByteArray, V, PD> {

    private final ByteArray key;

    private final Zone clientZone;

    public ConfigureNodesLocalZoneOnly(PD pipelineData,
                                       Event completeEvent,
                                       FailureDetector failureDetector,
                                       int required,
                                       RoutingStrategy routingStrategy,
                                       ByteArray key,
                                       Zone clientZone) {
        super(pipelineData, completeEvent, failureDetector, required, routingStrategy);
        this.key = key;
        this.clientZone = clientZone;
    }

    @Override
    protected List<Node> getNodes(ByteArray key) {
        List<Node> originalNodes = null;
        List<Node> nodes = new ArrayList<Node>();
        try {
            originalNodes = super.getNodes(key);
        } catch(VoldemortException e) {
            pipelineData.setFatalError(e);
            return null;
        }

        for(Node node: originalNodes) {
            if(node.getZoneId() == clientZone.getId()) {
                nodes.add(node);
            }
        }

        if(logger.isDebugEnabled())
            logger.debug("Adding " + nodes.size() + " node(s) to preference list");

        return nodes;
    }

    @Override
    public void execute(Pipeline pipeline) {
        List<Node> nodes = null;

        nodes = getNodes(key);
        if(nodes == null) {
            pipeline.abort();
            return;
        }

        if(logger.isDebugEnabled()) {
            StringBuilder nodeStr = new StringBuilder();
            for(Node node: nodes) {
                nodeStr.append(node.getId() + ",");
            }
            logger.debug("Key " + ByteUtils.toHexString(key.get())
                         + " final preference list to contact " + nodeStr);
        }
        pipelineData.setNodes(nodes);
        pipeline.addEvent(completeEvent);
    }

}
