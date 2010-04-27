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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;

public class AcknowledgeResponseTest extends AbstractActionTest {

    @Test
    public void testResponse() throws Exception {
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        pipelineData.setAttempts(1);

        AcknowledgeResponse<byte[], BasicPipelineData<byte[]>> action = new AcknowledgeResponse<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                                   Event.COMPLETED,
                                                                                                                                   failureDetector,
                                                                                                                                   1,
                                                                                                                                   1,
                                                                                                                                   Event.COMPLETED);

        Node node = cluster.getNodeById(0);
        Response<ByteArray, byte[]> response = new Response<ByteArray, byte[]>(node,
                                                                               aKey,
                                                                               new byte[8],
                                                                               777);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        action.execute(pipeline, response);

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        assertEquals(1, pipelineData.getSuccesses());
        assertEquals(response, pipelineData.getResponses().get(0));
        assertTrue(failureDetector.isAvailable(node));
    }

    @Test(expected = InsufficientOperationalNodesException.class)
    public void testNotEnoughSuccessesError() throws Exception {
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        pipelineData.setAttempts(1);

        AcknowledgeResponse<byte[], BasicPipelineData<byte[]>> action = new AcknowledgeResponse<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                                   Event.COMPLETED,
                                                                                                                                   failureDetector,
                                                                                                                                   1,
                                                                                                                                   1,
                                                                                                                                   null);

        Node node = cluster.getNodeById(0);
        Response<ByteArray, Object> response = new Response<ByteArray, Object>(node,
                                                                               aKey,
                                                                               new Exception("testing failures in "
                                                                                             + getClass().getSimpleName()),
                                                                               777);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        action.execute(pipeline, response);

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
        else
            fail();
    }

    @Test
    public void testInsufficientSuccessesEvent() throws Exception {
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        pipelineData.setAttempts(1);

        Event expectedEvent = Event.NOP;

        AcknowledgeResponse<byte[], BasicPipelineData<byte[]>> action = new AcknowledgeResponse<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                                   Event.COMPLETED,
                                                                                                                                   failureDetector,
                                                                                                                                   1,
                                                                                                                                   1,
                                                                                                                                   expectedEvent);

        Node node = cluster.getNodeById(0);
        Response<ByteArray, Object> response = new Response<ByteArray, Object>(node,
                                                                               aKey,
                                                                               new Exception("testing failures in "
                                                                                             + getClass().getSimpleName()),
                                                                               777);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        action.execute(pipeline, response);

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        BlockingQueue<?> queue = TestUtils.getPrivateValue(pipeline, "eventDataQueue");
        Event actualEvent = TestUtils.getPrivateValue(queue.peek(), "event");

        assertEquals(expectedEvent, actualEvent);
    }

    @Test
    public void testState() throws Exception {
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        pipelineData.setAttempts(2);

        AcknowledgeResponse<byte[], BasicPipelineData<byte[]>> action = new AcknowledgeResponse<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                                   Event.COMPLETED,
                                                                                                                                   failureDetector,
                                                                                                                                   1,
                                                                                                                                   1,
                                                                                                                                   null);

        Node node1 = cluster.getNodeById(0);
        Node node2 = cluster.getNodeById(1);
        UnreachableStoreException node1Error = new UnreachableStoreException("testing failures in "
                                                                             + getClass().getSimpleName());
        Response<ByteArray, Object> response1 = new Response<ByteArray, Object>(node1,
                                                                                aKey,
                                                                                node1Error,
                                                                                777);
        Response<ByteArray, Object> response2 = new Response<ByteArray, Object>(node2,
                                                                                aKey,
                                                                                new byte[8],
                                                                                777);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        action.execute(pipeline, response1);
        action.execute(pipeline, response2);

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        assertEquals(1, pipelineData.getSuccesses());
        assertEquals(1, pipelineData.getResponses().size());
        assertEquals(response2, pipelineData.getResponses().get(0));
        assertEquals(1, pipelineData.getFailures().size());
        assertEquals(node1Error, pipelineData.getFailures().get(0));
        assertTrue(!failureDetector.isAvailable(node1));
        assertTrue(failureDetector.isAvailable(node2));
    }

    @Test(expected = VoldemortApplicationException.class)
    public void testVoldemortApplicationException() throws Exception {
        BasicPipelineData<byte[]> pipelineData = new BasicPipelineData<byte[]>();
        pipelineData.setAttempts(1);

        AcknowledgeResponse<byte[], BasicPipelineData<byte[]>> action = new AcknowledgeResponse<byte[], BasicPipelineData<byte[]>>(pipelineData,
                                                                                                                                   Event.COMPLETED,
                                                                                                                                   failureDetector,
                                                                                                                                   1,
                                                                                                                                   1,
                                                                                                                                   null);

        Node node1 = cluster.getNodeById(0);
        VoldemortApplicationException node1Error = new VoldemortApplicationException("testing failures in "
                                                                                     + getClass().getSimpleName());
        Response<ByteArray, Object> response1 = new Response<ByteArray, Object>(node1,
                                                                                aKey,
                                                                                node1Error,
                                                                                777);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        action.execute(pipeline, response1);

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
        else
            fail();
    }

}
