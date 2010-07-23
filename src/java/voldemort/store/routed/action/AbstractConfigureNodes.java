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
import java.util.List;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.PipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

public abstract class AbstractConfigureNodes<K, V, PD extends PipelineData<K, V>> extends
        AbstractAction<K, V, PD> {

    protected final FailureDetector failureDetector;

    protected final int required;

    protected final RoutingStrategy routingStrategy;

    protected AbstractConfigureNodes(PD pipelineData,
                                     Event completeEvent,
                                     FailureDetector failureDetector,
                                     int required,
                                     RoutingStrategy routingStrategy) {
        super(pipelineData, completeEvent);
        this.failureDetector = failureDetector;
        this.required = required;
        this.routingStrategy = routingStrategy;
    }

    protected List<Node> getNodes(ByteArray key) {
        List<Node> nodes = new ArrayList<Node>();

        for(Node node: routingStrategy.routeRequest(key.get())) {
            if(failureDetector.isAvailable(node))
                nodes.add(node);
            else
                pipelineData.addFailedNode(node);
        }

        if(nodes.size() < required)
            throw new InsufficientOperationalNodesException("Only " + nodes.size()
                                                            + " nodes in preference list, but "
                                                            + required + " required.");

        return nodes;
    }
}
