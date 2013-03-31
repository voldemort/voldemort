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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.Response;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

public class PerformParallelRequests<V, PD extends BasicPipelineData<V>> extends
        AbstractKeyBasedAction<ByteArray, V, PD> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private final Event insufficientSuccessesEvent;

    private final Event insufficientZonesEvent;

    private byte[] transforms;

    public PerformParallelRequests(PD pipelineData,
                                   Event completeEvent,
                                   ByteArray key,
                                   byte[] transforms,
                                   FailureDetector failureDetector,
                                   int preferred,
                                   int required,
                                   long timeoutMs,
                                   Map<Integer, NonblockingStore> nonblockingStores,
                                   Event insufficientSuccessesEvent,
                                   Event insufficientZonesEvent) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.transforms = transforms;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.insufficientSuccessesEvent = insufficientSuccessesEvent;
        this.insufficientZonesEvent = insufficientZonesEvent;
    }

    public void execute(final Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();
        int attempts = Math.min(preferred, nodes.size());
        final Map<Integer, Response<ByteArray, Object>> responses = new ConcurrentHashMap<Integer, Response<ByteArray, Object>>();
        final CountDownLatch latch = new CountDownLatch(attempts);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        for(int i = 0; i < attempts; i++) {
            final Node node = nodes.get(i);
            pipelineData.incrementNodeIndex();

            final long startMs = logger.isDebugEnabled() ? System.currentTimeMillis() : -1;

            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                public void requestComplete(Object result, long requestTime) {
                    if(logger.isTraceEnabled())
                        logger.trace(pipeline.getOperation().getSimpleName()
                                     + " response received (" + requestTime + " ms.) from node "
                                     + node.getId());

                    Response<ByteArray, Object> response = new Response<ByteArray, Object>(node,
                                                                                           key,
                                                                                           result,
                                                                                           requestTime);
                    if(logger.isDebugEnabled())
                        logger.debug("Finished " + pipeline.getOperation().getSimpleName()
                                     + " for key " + ByteUtils.toHexString(key.get())
                                     + " (keyRef: " + System.identityHashCode(key)
                                     + "); started at " + startMs + " took " + requestTime
                                     + " ms on node " + node.getId() + "(" + node.getHost() + ")");

                    responses.put(node.getId(), response);
                    latch.countDown();

                    // Note errors that come in after the pipeline has finished.
                    // These will *not* get a chance to be called in the loop of
                    // responses below.
                    if(pipeline.isFinished() && response.getValue() instanceof Exception) {
                        if(response.getValue() instanceof InvalidMetadataException) {
                            pipelineData.reportException((InvalidMetadataException) response.getValue());
                            logger.warn("Received invalid metadata problem after a successful "
                                        + pipeline.getOperation().getSimpleName()
                                        + " call on node " + node.getId() + ", store '"
                                        + pipelineData.getStoreName() + "'");
                        } else {
                            handleResponseError(response, pipeline, failureDetector);
                        }
                    }
                }

            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());

            if(pipeline.getOperation() == Operation.GET)
                store.submitGetRequest(key, transforms, callback, timeoutMs);
            else if(pipeline.getOperation() == Operation.GET_VERSIONS)
                store.submitGetVersionsRequest(key, callback, timeoutMs);
            else
                throw new IllegalStateException(getClass().getName()
                                                + " does not support pipeline operation "
                                                + pipeline.getOperation());
        }

        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        for(Response<ByteArray, Object> response: responses.values()) {
            if(response.getValue() instanceof Exception) {
                if(handleResponseError(response, pipeline, failureDetector))
                    return;
            } else {
                pipelineData.incrementSuccesses();

                Response<ByteArray, V> rCast = Utils.uncheckedCast(response);
                pipelineData.getResponses().add(rCast);
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
            }
        }

        if(logger.isDebugEnabled())
            logger.debug("GET for key " + ByteUtils.toHexString(key.get()) + " (keyRef: "
                         + System.identityHashCode(key) + "); successes: "
                         + pipelineData.getSuccesses() + " preferred: " + preferred + " required: "
                         + required);

        if(pipelineData.getSuccesses() < required) {
            if(insufficientSuccessesEvent != null) {
                pipeline.addEvent(insufficientSuccessesEvent);
            } else {
                pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                             + " "
                                                                                             + pipeline.getOperation()
                                                                                                       .getSimpleName()
                                                                                             + "s required, but only "
                                                                                             + pipelineData.getSuccesses()
                                                                                             + " succeeded",
                                                                                     new ArrayList<Node>(pipelineData.getReplicationSet()),
                                                                                     new ArrayList<Node>(pipelineData.getNodes()),
                                                                                     new ArrayList<Node>(pipelineData.getFailedNodes()),
                                                                                     pipelineData.getFailures()));

                pipeline.abort();
            }

        } else {

            if(pipelineData.getZonesRequired() != null) {

                int zonesSatisfied = pipelineData.getZoneResponses().size();
                if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                    pipeline.addEvent(completeEvent);
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Operation " + pipeline.getOperation().getSimpleName()
                                     + "failed due to insufficent zone responses, required "
                                     + pipelineData.getZonesRequired() + " obtained "
                                     + zonesSatisfied + " " + pipelineData.getZoneResponses());
                    }
                    if(this.insufficientZonesEvent != null) {
                        pipeline.addEvent(this.insufficientZonesEvent);
                    } else {
                        pipelineData.setFatalError(new InsufficientZoneResponsesException((pipelineData.getZonesRequired() + 1)
                                                                                          + " "
                                                                                          + pipeline.getOperation()
                                                                                                    .getSimpleName()
                                                                                          + "s required zone, but only "
                                                                                          + zonesSatisfied
                                                                                          + " succeeded"));
                    }

                }

            } else {
                pipeline.addEvent(completeEvent);
            }
        }
    }
}
