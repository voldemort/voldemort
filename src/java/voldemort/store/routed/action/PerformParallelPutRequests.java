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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class PerformParallelPutRequests extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final int required;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    public PerformParallelPutRequests(PutPipelineData pipelineData,
                                      Event completeEvent,
                                      ByteArray key,
                                      FailureDetector failureDetector,
                                      int required,
                                      long timeoutMs,
                                      Map<Integer, NonblockingStore> nonblockingStores) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
    }

    public void execute(final Pipeline pipeline) {
        Node master = pipelineData.getMaster();
        Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();

        if(logger.isDebugEnabled())
            logger.debug("Serial put requests determined master node as " + master.getId()
                         + ", submitting remaining requests in parallel");

        List<Node> nodes = pipelineData.getNodes();
        int firstParallelNodeIndex = nodes.indexOf(master) + 1;
        int attempts = nodes.size() - firstParallelNodeIndex;

        final List<Response<ByteArray, Object>> responses = new ArrayList<Response<ByteArray, Object>>();
        final CountDownLatch latch = new CountDownLatch(attempts);
        final AtomicBoolean isComplete = new AtomicBoolean(false);
        final Object lock = new Object();

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        for(int i = firstParallelNodeIndex; i < (firstParallelNodeIndex + attempts); i++) {
            final Node node = nodes.get(i);
            pipelineData.incrementNodeIndex();

            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                public void requestComplete(Object result, long requestTime) {
                    synchronized(lock) {
                        if(isComplete.get())
                            return;

                        if(logger.isTraceEnabled())
                            logger.trace(pipeline.getOperation().getSimpleName()
                                         + " response received (" + requestTime
                                         + " ms.) from node " + node.getId());

                        responses.add(new Response<ByteArray, Object>(node,
                                                                      key,
                                                                      result,
                                                                      requestTime));
                        latch.countDown();
                    }
                }

            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitPutRequest(key, versionedCopy, callback);
        }

        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        synchronized(lock) {
            isComplete.set(true);
        }

        for(Response<ByteArray, Object> response: responses) {
            if(response.getValue() instanceof Exception) {
                Node node = response.getNode();
                Exception e = (Exception) response.getValue();
                long requestTime = response.getRequestTime();

                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Error in " + pipeline.getOperation().getSimpleName() + " on node "
                                + node.getId() + "(" + node.getHost() + ")", e);

                if(e instanceof UnreachableStoreException) {
                    pipelineData.recordFailure(e);
                    failureDetector.recordException(node,
                                                    requestTime,
                                                    (UnreachableStoreException) e);
                } else if(e instanceof VoldemortApplicationException) {
                    pipelineData.setFatalError((VoldemortApplicationException) e);
                    pipeline.addEvent(Event.ERROR);
                    return;
                } else {
                    pipelineData.recordFailure(e);
                }
            } else {
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
            }
        }

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
    }
}