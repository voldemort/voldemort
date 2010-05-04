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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class PerformSerialPutRequests extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final FailureDetector failureDetector;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final int required;

    private final long timeoutMs;

    private final Versioned<byte[]> versioned;

    private final Time time;

    private final Event masterDeterminedEvent;

    public PerformSerialPutRequests(PutPipelineData pipelineData,
                                    Event completeEvent,
                                    ByteArray key,
                                    FailureDetector failureDetector,
                                    Map<Integer, NonblockingStore> nonblockingStores,
                                    int required,
                                    long timeoutMs,
                                    Versioned<byte[]> versioned,
                                    Time time,
                                    Event masterDeterminedEvent) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.nonblockingStores = nonblockingStores;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.versioned = versioned;
        this.time = time;
        this.masterDeterminedEvent = masterDeterminedEvent;
    }

    public void execute(Pipeline pipeline) {
        int currentNode = 0;
        List<Node> nodes = pipelineData.getNodes();

        if(logger.isDebugEnabled())
            logger.debug("Performing serial put requests to determine master");

        for(; currentNode < nodes.size(); currentNode++) {
            Node node = nodes.get(currentNode);
            long startNs = System.nanoTime();

            try {
                VectorClock versionedClock = (VectorClock) versioned.getVersion();
                Versioned<byte[]> versionedCopy = new Versioned<byte[]>(versioned.getValue(),
                                                                        versionedClock.incremented(node.getId(),
                                                                                                   time.getMilliseconds()));

                if(logger.isTraceEnabled())
                    logger.trace("Attempt # " + (currentNode + 1) + " to perform put (node "
                                 + node.getId() + ")");

                final CountDownLatch latch = new CountDownLatch(1);
                PutCallback callback = new PutCallback(latch);
                NonblockingStore store = nonblockingStores.get(node.getId());
                store.submitPutRequest(key, versionedCopy, callback);

                boolean successful = false;

                try {
                    successful = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
                } catch(InterruptedException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }

                if(!successful) {
                    List<Exception> failures = pipelineData.getFailures();
                    pipelineData.setFatalError(new InsufficientOperationalNodesException("No master node succeeded!",
                                                                                         failures.size() > 0 ? failures.get(0)
                                                                                                            : null));
                    pipeline.addEvent(Event.ERROR);
                    return;
                }

                if(callback.result instanceof Exception)
                    throw (Exception) callback.result;

                pipelineData.incrementSuccesses();

                long requestTime = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                failureDetector.recordSuccess(node, requestTime);

                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " succeeded, using as master");

                pipelineData.setMaster(node);
                pipelineData.setVersionedCopy(versionedCopy);

                break;
            } catch(UnreachableStoreException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " failed: " + e);

                pipelineData.recordFailure(e);
                long requestTime = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                failureDetector.recordException(node, requestTime, e);
            } catch(VoldemortApplicationException e) {
                pipelineData.setFatalError(e);
                pipeline.addEvent(Event.ERROR);
                return;
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " failed: " + e);

                pipelineData.recordFailure(e);
            }
        }

        if(pipelineData.getSuccesses() < 1) {
            List<Exception> failures = pipelineData.getFailures();
            pipelineData.setFatalError(new InsufficientOperationalNodesException("No master node succeeded!",
                                                                                 failures.size() > 0 ? failures.get(0)
                                                                                                    : null));
            pipeline.addEvent(Event.ERROR);
            return;
        }

        currentNode++;

        // There aren't any more requests to make...
        if(currentNode == nodes.size()) {
            if(pipelineData.getSuccesses() < required) {
                pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                             + " "
                                                                                             + pipeline.getOperation()
                                                                                                       .getSimpleName()
                                                                                             + "s required, but "
                                                                                             + pipelineData.getSuccesses()
                                                                                             + " succeeded",
                                                                                     pipelineData.getFailures()));
                pipeline.addEvent(Event.ERROR);
            } else {
                pipeline.addEvent(completeEvent);
            }
        } else {
            pipeline.addEvent(masterDeterminedEvent);
        }
    }

    private static class PutCallback implements NonblockingStoreCallback {

        private final CountDownLatch latch;

        private Object result;

        private PutCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        public void requestComplete(Object result, long requestTime) {
            this.result = result;
            latch.countDown();
        }

    }

}