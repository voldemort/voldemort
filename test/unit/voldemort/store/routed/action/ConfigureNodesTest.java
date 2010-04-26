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
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.NoopFailureDetector;
import voldemort.routing.RouteToAllStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;

public class ConfigureNodesTest {

    private Cluster cluster;
    private final ByteArray aKey = TestUtils.toByteArray("jay");
    private final byte[] aValue = "kreps".getBytes();
    private FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;

    @Before
    public void setUp() throws Exception {
        cluster = getNineNodeCluster();
        failureDetector = new NoopFailureDetector();
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        if(routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();
    }

    @Test
    public void testConfigureNodes() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());

        failureDetector = new NoopFailureDetector();
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        ConfigureNodes<byte[], BasicPipelineData<byte[]>> action = new ConfigureNodes<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                         Event.COMPLETED,
                                                                                                                         failureDetector,
                                                                                                                         1,
                                                                                                                         routingStrategy,
                                                                                                                         aKey);
        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.execute();

        assertEquals(cluster.getNodes().size(), pipelineData.getNodes().size());
    }

    @Test(expected = InsufficientOperationalNodesException.class)
    public void testConfigureNodesNotEnoughNodes() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());

        failureDetector = new NoopFailureDetector();
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
        pipeline.execute();

        throw pipelineData.getFatalError();
    }

}
