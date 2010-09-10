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

import java.util.Date;
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
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;

public class PerformParallelRequests<V, PD extends BasicPipelineData<V>> extends
        AbstractKeyBasedAction<ByteArray, V, PD> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private final Event insufficientSuccessesEvent;

    private final Event insufficientZonesEvent;

    private final boolean enableHintedHandoff;

    private final HintedHandoff hintedHandoff;

    private final Version version;

    public PerformParallelRequests(PD pipelineData,
                                   Event completeEvent,
                                   ByteArray key,
                                   FailureDetector failureDetector,
                                   int preferred,
                                   int required,
                                   long timeoutMs,
                                   Map<Integer, NonblockingStore> nonblockingStores,
                                   HintedHandoff hintedHandoff,
                                   Version version,
                                   Event insufficientSuccessesEvent,
                                   Event insufficientZonesEvent) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.insufficientSuccessesEvent = insufficientSuccessesEvent;
        this.insufficientZonesEvent = insufficientZonesEvent;
        this.enableHintedHandoff = hintedHandoff != null;
        this.version = version;
        this.hintedHandoff = hintedHandoff;
    }

    public boolean isHintedHandoffEnabled() {
        return enableHintedHandoff;
    }

    @SuppressWarnings("unchecked")
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
                    responses.put(node.getId(), response);
                    if(isHintedHandoffEnabled() && Pipeline.Operation.DELETE.equals(pipeline.getOperation())) {
                        if(response.getValue() instanceof Exception) {
                            Slop slop = new Slop(pipelineData.getStoreName(),
                                                 Slop.Operation.DELETE,
                                                 key,
                                                 null,
                                                 node.getId(),
                                                 new Date());
                            pipelineData.addFailedNode(node);
                            hintedHandoff.sendHint(node, version, slop);
                        }
                    }
                    latch.countDown();
                }

            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());

            if(pipeline.getOperation() == Operation.DELETE)
                store.submitDeleteRequest(key, version, callback, timeoutMs);
            else if(pipeline.getOperation() == Operation.GET)
                store.submitGetRequest(key, callback, timeoutMs);
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
                pipelineData.getResponses().add((Response<ByteArray, V>) response);
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
            }
        }

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
                                                                                     pipelineData.getFailures()));

                pipeline.abort();
            }

        } else {

            if(pipelineData.getZonesRequired() != null) {

                int zonesSatisfied = pipelineData.getZoneResponses().size();
                if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                    pipeline.addEvent(completeEvent);
                } else {
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
