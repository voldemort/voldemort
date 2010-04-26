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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
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
    public void testBasicError() throws Exception {
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

}
