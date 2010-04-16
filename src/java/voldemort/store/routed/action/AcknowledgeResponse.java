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

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

public class AcknowledgeResponse<V, PD extends BasicPipelineData<V>> extends
        AbstractAcknowledgeResponse<ByteArray, V, PD> {

    protected final int preferred;

    protected final int required;

    protected final Event insufficientSuccessesEvent;

    protected boolean isComplete;

    public AcknowledgeResponse(PD pipelineData,
                               Event completeEvent,
                               FailureDetector failureDetector,
                               int preferred,
                               int required,
                               Event insufficientSuccessesEvent) {
        super(pipelineData, completeEvent, failureDetector);
        this.preferred = preferred;
        this.required = required;
        this.insufficientSuccessesEvent = insufficientSuccessesEvent;
    }

    @Override
    protected void executeInternal(Pipeline pipeline, Response<ByteArray, V> response) {
        if(!checkError(pipeline, response)) {
            pipelineData.incrementSuccesses();
            pipelineData.getResponses().add(response);
            failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
        }

        if(logger.isTraceEnabled())
            logger.trace("Response received, successes: " + pipelineData.getSuccesses()
                         + ", preferred: " + preferred + ", required: " + required);

        // If we get to here, that means we couldn't hit the preferred number
        // of writes, throw an exception if you can't even hit the required
        // number
        if(pipelineData.getCompleted() == pipelineData.getAttempts()
           && pipelineData.getSuccesses() < required) {
            if(insufficientSuccessesEvent != null) {
                pipeline.addEvent(insufficientSuccessesEvent);
            } else {
                pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                             + " "
                                                                                             + pipeline.getOperation()
                                                                                                       .getSimpleName()
                                                                                             + "s required, but "
                                                                                             + pipelineData.getSuccesses()
                                                                                             + " succeeded",
                                                                                     pipelineData.getFailures()));

                pipeline.addEvent(Event.ERROR);
            }
        } else if(pipelineData.getSuccesses() >= preferred && !isComplete) {
            isComplete = true;

            pipeline.addEvent(completeEvent);
        }
    }

}
