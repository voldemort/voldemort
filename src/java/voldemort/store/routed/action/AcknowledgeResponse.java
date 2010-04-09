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
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.ListStateData;
import voldemort.store.routed.RequestCompletedCallback;
import voldemort.store.routed.StateMachine;
import voldemort.store.routed.StateMachine.Event;

public class AcknowledgeResponse extends AbstractAction<ListStateData> {

    private Event insufficientSuccessesEvent;

    private boolean isComplete;

    public Event getInsufficientSuccessesEvent() {
        return insufficientSuccessesEvent;
    }

    public void setInsufficientSuccessesEvent(Event insufficientSuccessesEvent) {
        this.insufficientSuccessesEvent = insufficientSuccessesEvent;
    }

    public void execute(StateMachine stateMachine, Object eventData) {
        RequestCompletedCallback rcc = (RequestCompletedCallback) eventData;
        stateData.incrementCompleted();

        if(rcc.getResult() instanceof Exception) {
            Node node = rcc.getNode();
            Exception e = (Exception) rcc.getResult();
            long requestTime = rcc.getRequestTime();

            if(e instanceof UnreachableStoreException) {
                stateData.recordFailure(e);
                failureDetector.recordException(node, requestTime, (UnreachableStoreException) e);
            } else if(e instanceof VoldemortApplicationException) {
                stateData.setFatalError((VoldemortApplicationException) e);
                stateMachine.addEvent(Event.ERROR);
                return;
            } else {
                stateData.recordFailure(e);

                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Error in " + stateData.getOperation() + " on node " + node.getId()
                                + "(" + node.getHost() + ")", e);
            }
        } else {
            stateData.incrementSuccesses();
            stateData.getInterimResults().add(rcc);
            failureDetector.recordSuccess(rcc.getNode(), rcc.getRequestTime());
        }

        if(logger.isDebugEnabled())
            logger.debug("Response received, successes: " + stateData.getSuccesses()
                         + ", attempts: " + stateData.getAttempts() + ", completed: "
                         + stateData.getCompleted() + ", preferred: " + preferred + ", required: "
                         + required);

        // If we get to here, that means we couldn't hit the preferred number
        // of writes, throw an exception if you can't even hit the required
        // number
        if(stateData.getCompleted() == stateData.getAttempts()
           && stateData.getSuccesses() < required) {
            if(insufficientSuccessesEvent != null) {
                stateMachine.addEvent(insufficientSuccessesEvent);
            } else {
                stateData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                          + " "
                                                                                          + stateData.getOperation()
                                                                                                     .getSimpleName()
                                                                                          + "s required, but "
                                                                                          + stateData.getSuccesses()
                                                                                          + " succeeded",
                                                                                  stateData.getFailures()));

                stateMachine.addEvent(Event.ERROR);
            }
        } else if(stateData.getSuccesses() >= preferred && !isComplete) {
            isComplete = true;

            stateMachine.addEvent(completeEvent);
        }
    }

}
