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

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.Store;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.PutPipelineData;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class PerformSerialPutRequests extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final FailureDetector failureDetector;

    private final int required;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;

    private final Versioned<byte[]> versioned;

    private final Time time;

    private final Event masterDeterminedEvent;

    private byte[] transforms;

    public PerformSerialPutRequests(PutPipelineData pipelineData,
                                    Event completeEvent,
                                    ByteArray key,
                                    byte[] transforms,
                                    FailureDetector failureDetector,
                                    Map<Integer, Store<ByteArray, byte[], byte[]>> stores,
                                    int required,
                                    Versioned<byte[]> versioned,
                                    Time time,
                                    Event masterDeterminedEvent) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.stores = stores;
        this.required = required;
        this.versioned = versioned;
        this.time = time;
        this.transforms = transforms;
        this.masterDeterminedEvent = masterDeterminedEvent;
    }

    public void execute(Pipeline pipeline) {
        int currentNode = 0;
        List<Node> nodes = pipelineData.getNodes();

        long startMasterMs = -1;
        long startMasterNs = -1;

        if(logger.isDebugEnabled()) {
            startMasterMs = System.currentTimeMillis();
            startMasterNs = System.nanoTime();
        }

        if(logger.isDebugEnabled())
            logger.debug("Performing serial put requests to determine master");

        Node node = null;
        for(; currentNode < nodes.size(); currentNode++) {
            node = nodes.get(currentNode);
            pipelineData.incrementNodeIndex();

            VectorClock versionedClock = (VectorClock) versioned.getVersion();
            final Versioned<byte[]> versionedCopy = new Versioned<byte[]>(versioned.getValue(),
                                                                          versionedClock.incremented(node.getId(),
                                                                                                     time.getMilliseconds()));

            if(logger.isDebugEnabled())
                logger.debug("Attempt #" + (currentNode + 1) + " to perform put (node "
                             + node.getId() + ")");

            long start = System.nanoTime();

            try {
                stores.get(node.getId()).put(key, versionedCopy, transforms);
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(node, requestTime);

                if(logger.isDebugEnabled())
                    logger.debug("Put on node " + node.getId() + " succeeded, using as master");

                pipelineData.setMaster(node);
                pipelineData.setVersionedCopy(versionedCopy);
                pipelineData.getZoneResponses().add(node.getZoneId());
                break;
            } catch(Exception e) {
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

                if(logger.isDebugEnabled())
                    logger.debug("Master PUT at node " + currentNode + "(" + node.getHost() + ")"
                                 + " failed (" + e.getMessage() + ") in "
                                 + (System.nanoTime() - start) + " ns" + " (keyRef: "
                                 + System.identityHashCode(key) + ")");

                if(handleResponseError(e, node, requestTime, pipeline, failureDetector))
                    return;
            }
        }

        if(pipelineData.getSuccesses() < 1) {
            List<Exception> failures = pipelineData.getFailures();
            pipelineData.setFatalError(new InsufficientOperationalNodesException("No master node succeeded!",
                                                                                 failures.size() > 0 ? failures.get(0)
                                                                                                    : null));
            pipeline.abort();
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
                                                                                             + "s required, but only "
                                                                                             + pipelineData.getSuccesses()
                                                                                             + " succeeded",
                                                                                     new ArrayList<Node>(pipelineData.getReplicationSet()),
                                                                                     new ArrayList<Node>(pipelineData.getNodes()),
                                                                                     new ArrayList<Node>(pipelineData.getFailedNodes()),
                                                                                     pipelineData.getFailures()));
                pipeline.abort();
            } else {
                if(pipelineData.getZonesRequired() != null) {

                    int zonesSatisfied = pipelineData.getZoneResponses().size();
                    if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
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

                } else {
                    if(logger.isDebugEnabled())
                        logger.debug("Finished master PUT for key "
                                     + ByteUtils.toHexString(key.get()) + " (keyRef: "
                                     + System.identityHashCode(key) + "); started at "
                                     + startMasterMs + " took "
                                     + (System.nanoTime() - startMasterNs) + " ns on node "
                                     + (node == null ? "NULL" : node.getId()) + "("
                                     + (node == null ? "NULL" : node.getHost()) + "); now complete");

                    pipeline.addEvent(completeEvent);
                }
            }
        } else {
            if(logger.isDebugEnabled())
                logger.debug("Finished master PUT for key " + ByteUtils.toHexString(key.get())
                             + " (keyRef: " + System.identityHashCode(key) + "); started at "
                             + startMasterMs + " took " + (System.nanoTime() - startMasterNs)
                             + " ns on node " + (node == null ? "NULL" : node.getId()) + "("
                             + (node == null ? "NULL" : node.getHost()) + ")");

            pipeline.addEvent(masterDeterminedEvent);
        }
    }
}
