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

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

public class ConfigureNodes extends AbstractConfigureNodes<BasicPipelineData> {

    protected final ByteArray key;

    public ConfigureNodes(BasicPipelineData pipelineData,
                          Event completeEvent,
                          FailureDetector failureDetector,
                          int required,
                          RoutingStrategy routingStrategy,
                          ByteArray key) {
        super(pipelineData, completeEvent, failureDetector, required, routingStrategy);
        this.key = key;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        List<Node> nodes = null;

        try {
            nodes = getNodes(key);
        } catch(VoldemortException e) {
            pipelineData.setFatalError(e);
            pipeline.addEvent(Event.ERROR);
            return;
        }

        if(logger.isDebugEnabled())
            logger.debug("Adding " + nodes.size() + " node(s) to preference list");

        pipelineData.setNodes(nodes);
        pipeline.addEvent(completeEvent);
    }

}
