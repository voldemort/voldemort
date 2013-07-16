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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.cluster.Node;
import voldemort.routing.RouteToAllStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;

public class GetAllConfigureNodesTest extends AbstractActionTest {

    @Test
    public void testConfigureNodes() throws Exception {
        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());
        GetAllPipelineData pipelineData = new GetAllPipelineData();
        List<ByteArray> keys = new ArrayList<ByteArray>();

        for(int i = 0; i < 10; i++)
            keys.add(TestUtils.toByteArray("key-" + i));

        int preferred = cluster.getNumberOfNodes() - 1;

        GetAllConfigureNodes action = new GetAllConfigureNodes(pipelineData,
                                                               Event.COMPLETED,
                                                               failureDetector,
                                                               preferred,
                                                               preferred - 1,
                                                               routingStrategy,
                                                               keys,
                                                               null,
                                                               null,
                                                               null);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        for(ByteArray key: keys) {
            List<Node> allNodesList = routingStrategy.routeRequest(key.get());
            assertEquals(cluster.getNumberOfNodes(), allNodesList.size());

            List<Node> extraNodes = pipelineData.getKeyToExtraNodesMap().get(key);
            assertEquals(cluster.getNumberOfNodes() - preferred, extraNodes.size());

            Node expectedExtraNode = allNodesList.get(preferred);
            Node actualExtraNode = extraNodes.get(0);

            assertEquals(expectedExtraNode, actualExtraNode);

            List<Node> preferredNodes = allNodesList.subList(0, preferred);
            assertEquals(preferred, preferredNodes.size());

            for(Node node: preferredNodes) {
                List<ByteArray> nodeKeys = pipelineData.getNodeToKeysMap().get(node);

                if(!nodeKeys.contains(key))
                    fail();
            }
        }
    }

    @Test(expected = InsufficientOperationalNodesException.class)
    public void testConfigureNodesNotEnoughNodes() throws Exception {
        for(Node node: cluster.getNodes())
            failureDetector.recordException(node,
                                            0,
                                            new UnreachableStoreException("Test for "
                                                                          + getClass().getName()));

        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());
        GetAllPipelineData pipelineData = new GetAllPipelineData();

        GetAllConfigureNodes action = new GetAllConfigureNodes(pipelineData,
                                                               Event.COMPLETED,
                                                               failureDetector,
                                                               1,
                                                               1,
                                                               routingStrategy,
                                                               Arrays.asList(aKey),
                                                               null,
                                                               null,
                                                               null);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        throw pipelineData.getFatalError();
    }

}
