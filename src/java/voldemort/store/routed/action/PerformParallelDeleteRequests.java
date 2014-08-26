/*
 * Copyright 2012 LinkedIn, Inc
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Response;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;

public class PerformParallelDeleteRequests<V, PD extends BasicPipelineData<V>> extends
        AbstractKeyBasedAction<ByteArray, V, PD> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private final boolean enableHintedHandoff;

    private final HintedHandoff hintedHandoff;

    private final Version version;

    public PerformParallelDeleteRequests(PD pipelineData,
                                         Event completeEvent,
                                         ByteArray key,
                                         FailureDetector failureDetector,
                                         int preferred,
                                         int required,
                                         long timeoutMs,
                                         Map<Integer, NonblockingStore> nonblockingStores,
                                         HintedHandoff hintedHandoff,
                                         Version version) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.enableHintedHandoff = hintedHandoff != null;
        this.version = version;
        this.hintedHandoff = hintedHandoff;
    }

    /**
     * 
     * @param response
     * @param pipeline
     * @param isParallel
     * @return true if it is a terminal error, false otherwise
     */
    private boolean handleException(Response<ByteArray, Object> response,
                                    Pipeline pipeline,
                                    boolean isParallel) {
        Node node = response.getNode();
        Exception ex = null;
        if(!(response.getValue() instanceof Exception)) {
            return false;
        }

        ex = (Exception) response.getValue();
        if(ex instanceof ObsoleteVersionException) {
            // ignore this completely here this means that a higher version was
            // able to write on this node and should be termed as clean success.
            return false;
        }

        if(enableHintedHandoff) {
            if(ex instanceof UnreachableStoreException || ex instanceof QuotaExceededException) {
                Slop slop = new Slop(pipelineData.getStoreName(),
                                     Slop.Operation.DELETE,
                                     key,
                                     null,
                                     null,
                                     node.getId(),
                                     new Date());

                pipelineData.addFailedNode(node);
                if(isParallel) {
                    hintedHandoff.sendHintParallel(node, version, slop);
                } else {
                    // TODO : Ideally should use sendHintParallel, but don't
                    // have time now to figure out what is the difference,
                    // and impact. saving it for a rainy day.
                    hintedHandoff.sendHintSerial(node, version, slop);
                }
            }
        }

        // Note errors that come in after the pipeline has finished.
        // These will *not* get a chance to be called in the loop of
        // responses below.
        if(ex instanceof InvalidMetadataException && pipeline.isFinished()) {
            pipelineData.reportException(ex);
            logger.warn("Received invalid metadata problem after a successful "
                        + pipeline.getOperation().getSimpleName() + " call on node " + node.getId()
                        + ", store '" + pipelineData.getStoreName() + "'");
        } else {
            return handleResponseError(response, pipeline, failureDetector);
        }
        return false;
    }

    /**
     * 
     * @param responses
     * @param pipeline
     * @return true if pipeline should be aborted, false otherwise
     */
    private boolean processResponses(Map<Integer, Response<ByteArray, Object>> responses,
                                     Pipeline pipeline) {
        for(Entry<Integer, Response<ByteArray, Object>> responseEntry: responses.entrySet()) {
            Response<ByteArray, Object> response = responseEntry.getValue();
            responses.remove(responseEntry.getKey());

            if(response.getValue() instanceof Exception) {
                if(handleException(response, pipeline, true)) {
                    return true;
                }
            } else {
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
                Response<ByteArray, V> rCast = Utils.uncheckedCast(response);
                pipelineData.getResponses().add(rCast);
            }
        }
        return false;
    }

    public void execute(final Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();
        final Map<Integer, Response<ByteArray, Object>> responses = new ConcurrentHashMap<Integer, Response<ByteArray, Object>>();
        int attempts = nodes.size();
        int blocks = Math.min(preferred, attempts);
        final CountDownLatch attemptsLatch = new CountDownLatch(attempts);
        final CountDownLatch blocksLatch = new CountDownLatch(blocks);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        long beginTime = System.nanoTime();

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

                    if(logger.isTraceEnabled()) {
                        logger.trace(attemptsLatch.getCount() + " attempts remaining. Will block "
                                     + " for " + blocksLatch.getCount() + " more ");
                    }
                    responses.put(node.getId(), response);

                    if(response.getValue() instanceof Exception && pipeline.isFinished()) {
                        handleException(response, pipeline, false);
                    }

                    attemptsLatch.countDown();
                    blocksLatch.countDown();

                }
            };

            if(logger.isTraceEnabled())
                logger.info("Submitting " + pipeline.getOperation().getSimpleName()
                            + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitDeleteRequest(key, version, callback, timeoutMs);
        }

        try {
            long ellapsedNs = System.nanoTime() - beginTime;
            long remainingNs = (timeoutMs * Time.NS_PER_MS) - ellapsedNs;
            if(remainingNs > 0) {
                blocksLatch.await(remainingNs, TimeUnit.NANOSECONDS);
            }
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        if(processResponses(responses, pipeline))
            return;

        // wait for more responses in case we did not have enough successful
        // response to achieve the required count
        boolean quorumSatisfied = true;
        if(pipelineData.getSuccesses() < required) {
            long ellapsedNs = System.nanoTime() - beginTime;
            long remainingNs = (timeoutMs * Time.NS_PER_MS) - ellapsedNs;
            if(remainingNs > 0) {
                try {
                    attemptsLatch.await(remainingNs, TimeUnit.NANOSECONDS);
                } catch(InterruptedException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }

                if(processResponses(responses, pipeline))
                    return;
            }

            if(pipelineData.getSuccesses() < required) {
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
                quorumSatisfied = false;
            }
        }

        if(quorumSatisfied) {
            if(pipelineData.getZonesRequired() != null) {
                int zonesSatisfied = pipelineData.getZoneResponses().size();
                if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                    pipeline.addEvent(completeEvent);
                } else {
                    long timeMs = (System.nanoTime() - beginTime) / Time.NS_PER_MS;

                    if((timeoutMs - timeMs) > 0) {
                        try {
                            attemptsLatch.await(timeoutMs - timeMs, TimeUnit.MILLISECONDS);
                        } catch(InterruptedException e) {
                            if(logger.isEnabledFor(Level.WARN))
                                logger.warn(e, e);
                        }

                        if(processResponses(responses, pipeline))
                            return;
                    }

                    if(pipelineData.getZoneResponses().size() >= (pipelineData.getZonesRequired() + 1)) {
                        pipeline.addEvent(completeEvent);
                    } else {
                        pipelineData.setFatalError(new InsufficientZoneResponsesException((pipelineData.getZonesRequired() + 1)
                                                                                          + " "
                                                                                          + pipeline.getOperation()
                                                                                                    .getSimpleName()
                                                                                          + "s required zone, but only "
                                                                                          + zonesSatisfied
                                                                                          + " succeeded"));
                        pipeline.abort();
                    }
                }

            } else {
                pipeline.addEvent(completeEvent);
            }
        }
    }
}
