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

import org.apache.log4j.Level;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.RequestCompletedCallback;
import voldemort.store.routed.Pipeline.Event;

public class AcknowledgeResponse extends AbstractAction<BasicPipelineData> {

    protected final FailureDetector failureDetector;

    protected final int preferred;

    protected final int required;

    protected final Event insufficientSuccessesEvent;

    protected boolean isComplete;

    public AcknowledgeResponse(BasicPipelineData pipelineData,
                               Event completeEvent,
                               FailureDetector failureDetector,
                               int preferred,
                               int required,
                               Event insufficientSuccessesEvent) {
        super(pipelineData, completeEvent);
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.insufficientSuccessesEvent = insufficientSuccessesEvent;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        RequestCompletedCallback rcc = (RequestCompletedCallback) eventData;
        pipelineData.incrementCompleted();

        if(rcc.getResult() instanceof Exception) {
            Node node = rcc.getNode();
            Exception e = (Exception) rcc.getResult();
            long requestTime = rcc.getRequestTime();

            if(e instanceof UnreachableStoreException) {
                pipelineData.recordFailure(e);
                failureDetector.recordException(node, requestTime, (UnreachableStoreException) e);
            } else if(e instanceof VoldemortApplicationException) {
                pipelineData.setFatalError((VoldemortApplicationException) e);
                pipeline.addEvent(Event.ERROR);
                return;
            } else {
                pipelineData.recordFailure(e);

                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Error in " + pipeline.getOperation() + " on node " + node.getId()
                                + "(" + node.getHost() + ")", e);
            }
        } else {
            pipelineData.incrementSuccesses();
            pipelineData.getInterimResults().add(rcc);
            failureDetector.recordSuccess(rcc.getNode(), rcc.getRequestTime());
        }

        if(logger.isDebugEnabled())
            logger.debug("Response received, successes: " + pipelineData.getSuccesses()
                         + ", attempts: " + pipelineData.getAttempts() + ", completed: "
                         + pipelineData.getCompleted() + ", preferred: " + preferred
                         + ", required: " + required);

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
