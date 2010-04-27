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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.cluster.Node;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetAllAcknowledgeResponseTest extends AbstractActionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testResponse() throws Exception {
        GetAllPipelineData pipelineData = new GetAllPipelineData();
        pipelineData.setAttempts(1);

        GetAllAcknowledgeResponse action = new GetAllAcknowledgeResponse(pipelineData,
                                                                         Event.COMPLETED,
                                                                         failureDetector);

        Node node1 = cluster.getNodeById(0);
        Node node2 = cluster.getNodeById(1);

        ByteArray anotherKey = TestUtils.toByteArray("kreps");
        ByteArray yetAnotherKey = TestUtils.toByteArray("finite");

        Versioned<byte[]> keyValue = new Versioned<byte[]>(new byte[8]);

        Map<ByteArray, List<Versioned<byte[]>>> values1 = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        values1.put(aKey, Arrays.asList(keyValue, keyValue));

        Map<ByteArray, List<Versioned<byte[]>>> values2 = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        values2.put(aKey, Arrays.asList(keyValue));
        values2.put(anotherKey, Arrays.asList(keyValue));

        Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> response1 = new Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>>(node1,
                                                                                                                                                                      Arrays.asList(aKey,
                                                                                                                                                                                    anotherKey),
                                                                                                                                                                      values1,
                                                                                                                                                                      777);
        Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> response2 = new Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>>(node2,
                                                                                                                                                                      Arrays.asList(aKey,
                                                                                                                                                                                    anotherKey,
                                                                                                                                                                                    yetAnotherKey),
                                                                                                                                                                      values2,
                                                                                                                                                                      777);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        action.execute(pipeline, response1);
        action.execute(pipeline, response2);

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        assertEquals(2, pipelineData.getSuccessCount(aKey).intValue());
        assertEquals(2, pipelineData.getSuccessCount(anotherKey).intValue());
        assertEquals(1, pipelineData.getSuccessCount(yetAnotherKey).intValue());
        assertEquals(response1, pipelineData.getResponses().get(0));
        assertEquals(response2, pipelineData.getResponses().get(1));
        assertEquals(keyValue, pipelineData.getResult().get(aKey).get(0));
        assertEquals(keyValue, pipelineData.getResult().get(anotherKey).get(0));

        assertTrue(failureDetector.isAvailable(node1));
        assertTrue(failureDetector.isAvailable(node2));
    }
}
