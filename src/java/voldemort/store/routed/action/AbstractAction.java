/*
 * Copyright 2010-2012 LinkedIn, Inc
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
import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.StoreTimeoutException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.PipelineData;
import voldemort.store.routed.Response;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;

public abstract class AbstractAction<K, V, PD extends PipelineData<K, V>> implements Action {

    protected final PD pipelineData;

    protected final Event completeEvent;

    protected final Logger logger = Logger.getLogger(getClass());

    protected AbstractAction(PD pipelineData, Event completeEvent) {
        this.pipelineData = Utils.notNull(pipelineData);
        this.completeEvent = Utils.notNull(completeEvent);
    }

    protected boolean handleResponseError(Response<?, ?> response,
                                          Pipeline pipeline,
                                          FailureDetector failureDetector) {
        return handleResponseError((Exception) response.getValue(),
                                   response.getNode(),
                                   response.getRequestTime(),
                                   pipeline,
                                   failureDetector);
    }

    protected boolean handleResponseError(Exception e,
                                          Node node,
                                          long requestTime,
                                          Pipeline pipeline,
                                          FailureDetector failureDetector) {
        if(e instanceof StoreTimeoutException || e instanceof ObsoleteVersionException
           || e instanceof UnreachableStoreException) {
            // Quietly mask all errors that are "expected" regularly.
            if(logger.isEnabledFor(Level.DEBUG)) {
                logger.debug("Error in " + pipeline.getOperation().getSimpleName() + " on node "
                             + node.getId() + " (" + node.getHost() + ") : " + e.getMessage());
            }
        } else {
            if(logger.isEnabledFor(Level.WARN)) {
                logger.warn("Error in " + pipeline.getOperation().getSimpleName() + " on node "
                            + node.getId() + " (" + node.getHost() + ")", e);
            }
        }

        if(e instanceof UnreachableStoreException) {
            pipelineData.addFailedNode(node);
            pipelineData.recordFailure(e);
            failureDetector.recordException(node, requestTime, (UnreachableStoreException) e);
        } else if(e instanceof VoldemortApplicationException) {
            pipelineData.setFatalError((VoldemortApplicationException) e);
            pipeline.abort();

            if(logger.isEnabledFor(Level.TRACE))
                logger.trace("Error is terminal - aborting further pipeline processing");

            return true;
        } else {
            pipelineData.recordFailure(e);
        }

        return false;
    }

}
