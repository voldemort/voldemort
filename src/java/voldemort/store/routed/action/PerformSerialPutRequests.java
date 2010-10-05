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

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.Store;
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

        if(logger.isDebugEnabled())
            logger.debug("Performing serial put requests to determine master");

        for(; currentNode < nodes.size(); currentNode++) {
            Node node = nodes.get(currentNode);
            pipelineData.incrementNodeIndex();

            VectorClock versionedClock = (VectorClock) versioned.getVersion();
            final Versioned<byte[]> versionedCopy = new Versioned<byte[]>(versioned.getValue(),
                                                                          versionedClock.incremented(node.getId(),
                                                                                                     time.getMilliseconds()));

            if(logger.isTraceEnabled())
                logger.trace("Attempt #" + (currentNode + 1) + " to perform put (node "
                             + node.getId() + ")");

            long start = System.nanoTime();

            try {
                stores.get(node.getId()).put(key, versionedCopy, transforms);
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(node, requestTime);

                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " succeeded, using as master");

                pipelineData.setMaster(node);
                pipelineData.setVersionedCopy(versionedCopy);
                pipelineData.getZoneResponses().add(node.getZoneId());
                break;
            } catch(Exception e) {
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

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
                    pipeline.addEvent(completeEvent);
                }
            }
        } else {
            pipeline.addEvent(masterDeterminedEvent);
        }
    }
}
