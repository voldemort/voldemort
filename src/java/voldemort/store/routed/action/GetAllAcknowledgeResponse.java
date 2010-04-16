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
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class GetAllAcknowledgeResponse
        extends
        AbstractAcknowledgeResponse<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    public GetAllAcknowledgeResponse(GetAllPipelineData pipelineData,
                                     Event completeEvent,
                                     FailureDetector failureDetector) {
        super(pipelineData, completeEvent, failureDetector);
    }

    @Override
    protected void executeInternal(Pipeline pipeline,
                                   Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> response) {
        if(!checkError(pipeline, response)) {
            Map<ByteArray, List<Versioned<byte[]>>> responseValue = response.getValue();

            for(ByteArray key: response.getKey()) {
                if(logger.isTraceEnabled())
                    logger.trace("Response received from " + response.getNode().getId() + " for "
                                 + pipeline.getOperation().getSimpleName());

                MutableInt successCount = pipelineData.getSuccessCount(key);
                successCount.increment();

                List<Versioned<byte[]>> retrieved = responseValue.get(key);
                /*
                 * retrieved can be null if there are no values for the key
                 * provided
                 */
                if(retrieved != null) {
                    List<Versioned<byte[]>> existing = pipelineData.getResult().get(key);

                    if(existing == null)
                        pipelineData.getResult().put(key, Lists.newArrayList(retrieved));
                    else
                        existing.addAll(retrieved);
                }
            }

            pipelineData.getResponses().add(response);
            failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
        }

        if(pipelineData.getCompleted() == pipelineData.getAttempts())
            pipeline.addEvent(completeEvent);
    }
}
