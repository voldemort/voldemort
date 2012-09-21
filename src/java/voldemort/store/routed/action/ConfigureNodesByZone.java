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
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

/**
 * Configure the Nodes obtained via the routing strategy based on the zone
 * information. Local zone nodes first, followed by the corresponding nodes from
 * each of the other zones, ordered by proximity.
 */
public class ConfigureNodesByZone<V, PD extends BasicPipelineData<V>> extends
        AbstractConfigureNodes<ByteArray, V, PD> {

    private final ByteArray key;

    private final Zone clientZone;

    public ConfigureNodesByZone(PD pipelineData,
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

    public List<Node> getNodes(ByteArray key, Operation op) {
        List<Node> nodes = null;
        try {
            nodes = super.getNodes(key);
        } catch(VoldemortException e) {
            pipelineData.setFatalError(e);
            return null;
        }

        if(logger.isDebugEnabled())
            logger.debug("Adding " + nodes.size() + " node(s) to preference list");

        if(pipelineData.getZonesRequired() > this.clientZone.getProximityList().size()) {
            throw new VoldemortException("Number of zones required should be less than the total number of zones");
        }

        if(pipelineData.getZonesRequired() > required) {
            throw new VoldemortException("Number of zones required should be less than the required number of "
                                         + op.getSimpleName() + "s");
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
        LinkedList<Integer> zoneProximityList = this.clientZone.getProximityList();
        if(op != Operation.PUT) {
            // GET, GET_VERSIONS, DELETE

            // Add a node from every zone, upto a max of
            // zoneCountReads/zoneCountWrites.
            for(int index = 0; index < pipelineData.getZonesRequired(); index++) {
                List<Node> zoneNodes = zoneIdToNode.get(zoneProximityList.get(index));
                if(zoneNodes != null && zoneNodes.size() > 0) {
                    nodes.add(zoneNodes.remove(0));
                }
            }

        }

        // Add the rest, starting with client zone...
        List<Node> clientZoneNodes = zoneIdToNode.get(clientZone.getId());
        if(clientZoneNodes != null && clientZoneNodes.size() > 0)
            nodes.addAll(clientZoneNodes);
        // ...followed by other zones sorted by proximity list
        for(int index = 0; index < zoneProximityList.size(); index++) {
            List<Node> zoneNodes = zoneIdToNode.get(zoneProximityList.get(index));
            if(zoneNodes != null && zoneNodes.size() > 0) {
                nodes.addAll(zoneNodes);
            }
        }

        return nodes;
    }

    public void execute(Pipeline pipeline) {
        List<Node> nodes = null;

        nodes = getNodes(key, pipeline.getOperation());
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
