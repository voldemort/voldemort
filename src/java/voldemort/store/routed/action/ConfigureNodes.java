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

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.ListStateData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;

public class ConfigureNodes extends AbstractKeyBasedAction<ListStateData> {

    private RoutingStrategy routingStrategy;

    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }

    public void setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        for(Node node: routingStrategy.routeRequest(key.get())) {
            if(failureDetector.isAvailable(node))
                pipelineData.addNode(node);
        }

        if(pipelineData.getNodes().size() < required) {
            pipelineData.setFatalError(new InsufficientOperationalNodesException("Only "
                                                                                 + pipelineData.getNodes()
                                                                                               .size()
                                                                                 + " nodes in preference list, but "
                                                                                 + required
                                                                                 + " required."));
            pipeline.addEvent(Event.ERROR);
        } else {
            if(logger.isDebugEnabled())
                logger.debug(pipelineData.getNodes().size() + " nodes in preference list");

            pipeline.addEvent(completeEvent);
        }
    }

}
