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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.routing.RouteToAllStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;

public class ConfigureNodesTest extends AbstractActionTest {

    @Test
    public void testConfigureNodes() throws Exception {
        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        ConfigureNodes<byte[], BasicPipelineData<byte[]>> action = new ConfigureNodes<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                         Event.COMPLETED,
                                                                                                                         failureDetector,
                                                                                                                         1,
                                                                                                                         routingStrategy,
                                                                                                                         aKey);
        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        assertEquals(cluster.getNodes().size(), pipelineData.getNodes().size());
    }

    @Test(expected = InsufficientOperationalNodesException.class)
    public void testConfigureNodesNotEnoughNodes() throws Exception {
        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        ConfigureNodes<byte[], BasicPipelineData<byte[]>> action = new ConfigureNodes<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                         Event.COMPLETED,
                                                                                                                         failureDetector,
                                                                                                                         cluster.getNodes()
                                                                                                                                .size() + 1,
                                                                                                                         routingStrategy,
                                                                                                                         aKey);
        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
        else
            fail();
    }

}
