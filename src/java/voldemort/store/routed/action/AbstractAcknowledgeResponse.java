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
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PipelineData;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;

public abstract class AbstractAcknowledgeResponse<K, V, PD extends PipelineData<K, V>> extends
        AbstractAction<K, V, PD> {

    protected final FailureDetector failureDetector;

    public AbstractAcknowledgeResponse(PD pipelineData,
                                       Event completeEvent,
                                       FailureDetector failureDetector) {
        super(pipelineData, completeEvent);
        this.failureDetector = failureDetector;
    }

    @SuppressWarnings("unchecked")
    public void execute(Pipeline pipeline, Object eventData) {
        Response<K, V> response = (Response<K, V>) eventData;
        pipelineData.incrementCompleted();

        if(logger.isTraceEnabled())
            logger.trace("Response received from node " + response.getNode().getId() + " for "
                         + pipeline.getOperation().getSimpleName() + " - attempts: "
                         + pipelineData.getAttempts() + ", completed: "
                         + pipelineData.getCompleted());

        executeInternal(pipeline, response);
    }

    protected abstract void executeInternal(Pipeline pipeline, Response<K, V> response);

    protected boolean checkError(Pipeline pipeline, Response<K, V> response) {
        if(!(response.getValue() instanceof Exception))
            return false;

        Node node = response.getNode();
        Exception e = (Exception) response.getValue();
        long requestTime = response.getRequestTime();

        if(logger.isEnabledFor(Level.WARN))
            logger.warn("Error in " + pipeline.getOperation().getSimpleName() + " on node "
                        + node.getId() + "(" + node.getHost() + ")", e);

        if(e instanceof UnreachableStoreException) {
            pipelineData.recordFailure(e);
            failureDetector.recordException(node, requestTime, (UnreachableStoreException) e);
        } else if(e instanceof VoldemortApplicationException) {
            pipelineData.setFatalError((VoldemortApplicationException) e);
            pipeline.addEvent(Event.ERROR);
        } else {
            pipelineData.recordFailure(e);
        }

        return true;
    }

}
