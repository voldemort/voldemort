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

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.ListStateData;
import voldemort.store.routed.StateMachine;
import voldemort.store.routed.StateMachine.Event;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class PerformSerialPutRequests extends AbstractKeyBasedAction<ListStateData> {

    private Versioned<byte[]> versioned;

    private Event masterDeterminedEvent;

    public Versioned<byte[]> getVersioned() {
        return versioned;
    }

    public void setVersioned(Versioned<byte[]> versioned) {
        this.versioned = versioned;
    }

    public Event getMasterDeterminedEvent() {
        return masterDeterminedEvent;
    }

    public void setMasterDeterminedEvent(Event masterDeterminedEvent) {
        this.masterDeterminedEvent = masterDeterminedEvent;
    }

    public void execute(StateMachine stateMachine, Object eventData) {
        int currentNode = 0;
        List<Node> nodes = stateData.getNodes();

        if(logger.isDebugEnabled())
            logger.debug("Performing serial put requests to determine master");

        for(; currentNode < nodes.size(); currentNode++) {
            Node node = nodes.get(currentNode);
            long startNs = System.nanoTime();

            try {
                Versioned<byte[]> versionedCopy = incremented(versioned, node.getId());

                if(logger.isTraceEnabled())
                    logger.trace("Attempt # " + (currentNode + 1) + " to perform put (node "
                                 + node.getId() + ")");

                stores.get(node.getId()).put(key, versionedCopy);

                stateData.incrementSuccesses();

                long requestTime = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                failureDetector.recordSuccess(node, requestTime);

                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " succeeded, using as master");

                stateData.setMaster(node);
                stateData.setVersionedCopy(versionedCopy);

                break;
            } catch(UnreachableStoreException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " failed: " + e);

                stateData.recordFailure(e);
                long requestTime = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                failureDetector.recordException(node, requestTime, e);
            } catch(VoldemortApplicationException e) {
                throw e;
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " failed: " + e);

                stateData.recordFailure(e);
            }
        }

        if(stateData.getSuccesses() < 1) {
            List<Exception> failures = stateData.getFailures();
            stateData.setFatalError(new InsufficientOperationalNodesException("No master node succeeded!",
                                                                              failures.size() > 0 ? failures.get(0)
                                                                                                 : null));
            stateMachine.addEvent(Event.ERROR);
            return;
        }

        currentNode++;

        // There aren't any more requests to make...
        if(currentNode == nodes.size()) {
            if(stateData.getSuccesses() < required) {
                stateData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                          + " "
                                                                                          + stateData.getOperation()
                                                                                                     .getSimpleName()
                                                                                          + "s required, but "
                                                                                          + stateData.getSuccesses()
                                                                                          + " succeeded",
                                                                                  stateData.getFailures()));
                stateMachine.addEvent(Event.ERROR);
            } else {
                stateMachine.addEvent(completeEvent);
            }
        } else {
            stateMachine.addEvent(masterDeterminedEvent);
        }
    }

    private Versioned<byte[]> incremented(Versioned<byte[]> versioned, int nodeId) {
        return new Versioned<byte[]>(versioned.getValue(),
                                     ((VectorClock) versioned.getVersion()).incremented(nodeId,
                                                                                        time.getMilliseconds()));
    }

}