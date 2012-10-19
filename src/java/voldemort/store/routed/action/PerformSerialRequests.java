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
import voldemort.store.StoreRequest;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Response;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;

public class PerformSerialRequests<V, PD extends BasicPipelineData<V>> extends
        AbstractKeyBasedAction<ByteArray, V, PD> {

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;

    private final int preferred;

    private final int required;

    private final StoreRequest<V> storeRequest;

    private final Event insufficientSuccessesEvent;

    public PerformSerialRequests(PD pipelineData,
                                 Event completeEvent,
                                 ByteArray key,
                                 FailureDetector failureDetector,
                                 Map<Integer, Store<ByteArray, byte[], byte[]>> stores,
                                 int preferred,
                                 int required,
                                 StoreRequest<V> storeRequest,
                                 Event insufficientSuccessesEvent) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.stores = stores;
        this.preferred = preferred;
        this.required = required;
        this.storeRequest = storeRequest;
        this.insufficientSuccessesEvent = insufficientSuccessesEvent;
    }

    /**
     * Checks whether every property except 'preferred' is satisfied
     * 
     * @return
     */
    private boolean isSatisfied() {
        if(pipelineData.getZonesRequired() != null) {
            return ((pipelineData.getSuccesses() >= required) && (pipelineData.getZoneResponses()
                                                                              .size() >= (pipelineData.getZonesRequired() + 1)));
        } else {
            return pipelineData.getSuccesses() >= required;
        }
    }

    public void execute(Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();

        // Now if we had any failures we will be short a few reads. Do serial
        // reads to make up for these.
        while(pipelineData.getSuccesses() < preferred && pipelineData.getNodeIndex() < nodes.size()) {
            Node node = nodes.get(pipelineData.getNodeIndex());
            long start = System.nanoTime();

            try {
                Store<ByteArray, byte[], byte[]> store = stores.get(node.getId());
                V result = storeRequest.request(store);

                Response<ByteArray, V> response = new Response<ByteArray, V>(node,
                                                                             key,
                                                                             result,
                                                                             ((System.nanoTime() - start) / Time.NS_PER_MS));

                if(logger.isDebugEnabled())
                    logger.debug(pipeline.getOperation().getSimpleName() + " for key "
                                 + ByteUtils.toHexString(key.get()) + " successes: "
                                 + pipelineData.getSuccesses() + " preferred: " + preferred
                                 + " required: " + required + " new "
                                 + pipeline.getOperation().getSimpleName() + " success on node "
                                 + node.getId());

                pipelineData.incrementSuccesses();
                pipelineData.getResponses().add(response);
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(node.getZoneId());
            } catch(Exception e) {
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

                if(handleResponseError(e, node, requestTime, pipeline, failureDetector))
                    return;
            }

            // break out if we have satisfied everything
            if(isSatisfied())
                break;

            pipelineData.incrementNodeIndex();
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
                    // if you run with zoneCountReads > 0, we could frequently
                    // run into this exception since our preference list for
                    // zone routing is laid out thus : <a node from each of
                    // 'zoneCountReads' zones>, <nodes from local zone>, <nodes
                    // from remote zone1>, <nodes from remote zone2>,...
                    // #preferred number of reads may not be able to satisfy
                    // zoneCountReads, if the original read to a remote node
                    // fails in the parallel stage
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
    }
}
