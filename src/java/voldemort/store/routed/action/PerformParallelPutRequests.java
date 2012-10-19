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
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Response;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

public class PerformParallelPutRequests extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private byte[] transforms;

    private final HintedHandoff hintedHandoff;

    public boolean enableHintedHandoff;

    public PerformParallelPutRequests(PutPipelineData pipelineData,
                                      Event completeEvent,
                                      ByteArray key,
                                      byte[] transforms,
                                      FailureDetector failureDetector,
                                      int preferred,
                                      int required,
                                      long timeoutMs,
                                      Map<Integer, NonblockingStore> nonblockingStores,
                                      HintedHandoff hintedHandoff) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.transforms = transforms;
        this.nonblockingStores = nonblockingStores;
        this.hintedHandoff = hintedHandoff;
        this.enableHintedHandoff = hintedHandoff != null;
    }

    public boolean isHintedHandoffEnabled() {
        return enableHintedHandoff;
    }

    public void execute(final Pipeline pipeline) {
        Node master = pipelineData.getMaster();
        final Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();

        if(logger.isDebugEnabled())
            logger.debug("Serial put requests determined master node as " + master.getId()
                         + ", submitting remaining requests in parallel");

        List<Node> nodes = pipelineData.getNodes();
        int firstParallelNodeIndex = nodes.indexOf(master) + 1;
        int attempts = nodes.size() - firstParallelNodeIndex;
        int blocks = Math.min(preferred - 1, attempts);

        final Map<Integer, Response<ByteArray, Object>> responses = new ConcurrentHashMap<Integer, Response<ByteArray, Object>>();
        final CountDownLatch attemptsLatch = new CountDownLatch(attempts);
        final CountDownLatch blocksLatch = new CountDownLatch(blocks);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        for(int i = firstParallelNodeIndex; i < (firstParallelNodeIndex + attempts); i++) {
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

                    if(logger.isDebugEnabled())
                        logger.debug("Finished secondary PUT for key "
                                     + ByteUtils.toHexString(key.get()) + " (keyRef: "
                                     + System.identityHashCode(key) + "); took " + requestTime
                                     + " ms on node " + node.getId() + "(" + node.getHost() + ")");

                    if(isHintedHandoffEnabled() && pipeline.isFinished()) {
                        if(response.getValue() instanceof UnreachableStoreException) {
                            Slop slop = new Slop(pipelineData.getStoreName(),
                                                 Slop.Operation.PUT,
                                                 key,
                                                 versionedCopy.getValue(),
                                                 transforms,
                                                 node.getId(),
                                                 new Date());
                            pipelineData.addFailedNode(node);
                            hintedHandoff.sendHintSerial(node, versionedCopy.getVersion(), slop);
                        }
                    }

                    attemptsLatch.countDown();
                    blocksLatch.countDown();

                    if(logger.isTraceEnabled())
                        logger.trace(attemptsLatch.getCount() + " attempts remaining. Will block "
                                     + " for " + blocksLatch.getCount() + " more ");

                    // Note errors that come in after the pipeline has finished.
                    // These will *not* get a chance to be called in the loop of
                    // responses below.
                    if(pipeline.isFinished() && response.getValue() instanceof Exception
                       && !(response.getValue() instanceof ObsoleteVersionException)) {
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
            store.submitPutRequest(key, versionedCopy, transforms, callback, timeoutMs);
        }

        try {
            long ellapsedNs = System.nanoTime() - pipelineData.getStartTimeNs();
            long remainingNs = (timeoutMs * Time.NS_PER_MS) - ellapsedNs;
            if(remainingNs > 0)
                blocksLatch.await(remainingNs, TimeUnit.NANOSECONDS);
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        for(Entry<Integer, Response<ByteArray, Object>> responseEntry: responses.entrySet()) {
            Response<ByteArray, Object> response = responseEntry.getValue();
            // Treat ObsoleteVersionExceptions as success since such an
            // exception means that a higher version was able to write on the
            // node.
            if(response.getValue() instanceof Exception
               && !(response.getValue() instanceof ObsoleteVersionException)) {
                if(handleResponseError(response, pipeline, failureDetector))
                    return;
            } else {
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
                responses.remove(responseEntry.getKey());
            }
        }

        boolean quorumSatisfied = true;
        if(pipelineData.getSuccesses() < required) {
            long ellapsedNs = System.nanoTime() - pipelineData.getStartTimeNs();
            long remainingNs = (timeoutMs * Time.NS_PER_MS) - ellapsedNs;
            if(remainingNs > 0) {
                try {
                    attemptsLatch.await(remainingNs, TimeUnit.NANOSECONDS);
                } catch(InterruptedException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }

                for(Entry<Integer, Response<ByteArray, Object>> responseEntry: responses.entrySet()) {
                    Response<ByteArray, Object> response = responseEntry.getValue();
                    // Treat ObsoleteVersionExceptions as success since such an
                    // exception means that a higher version was able to write
                    // on the node.
                    if(response.getValue() instanceof Exception
                       && !(response.getValue() instanceof ObsoleteVersionException)) {
                        if(handleResponseError(response, pipeline, failureDetector))
                            return;
                    } else {
                        pipelineData.incrementSuccesses();
                        failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                        pipelineData.getZoneResponses().add(response.getNode().getZoneId());
                        responses.remove(responseEntry.getKey());
                    }
                }
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
                    long timeMs = (System.nanoTime() - pipelineData.getStartTimeNs())
                                  / Time.NS_PER_MS;

                    if((timeoutMs - timeMs) > 0) {
                        try {
                            attemptsLatch.await(timeoutMs - timeMs, TimeUnit.MILLISECONDS);
                        } catch(InterruptedException e) {
                            if(logger.isEnabledFor(Level.WARN))
                                logger.warn(e, e);
                        }

                        for(Entry<Integer, Response<ByteArray, Object>> responseEntry: responses.entrySet()) {
                            Response<ByteArray, Object> response = responseEntry.getValue();
                            if(response.getValue() instanceof Exception) {
                                if(handleResponseError(response, pipeline, failureDetector))
                                    return;
                            } else {
                                pipelineData.incrementSuccesses();
                                failureDetector.recordSuccess(response.getNode(),
                                                              response.getRequestTime());
                                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
                                responses.remove(responseEntry.getKey());
                            }
                        }
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
                                                                                          + " succeeded. Failing nodes : "
                                                                                          + pipelineData.getFailedNodes()));
                        pipeline.abort();
                    }
                }

            } else {
                pipeline.addEvent(completeEvent);
            }
        }
    }
}
