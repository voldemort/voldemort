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
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.Store;
import voldemort.store.StoreRequest;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;

public class PerformZoneSerialRequests<V, PD extends BasicPipelineData<V>> extends
        AbstractKeyBasedAction<ByteArray, V, PD> {

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;

    private final StoreRequest<V> storeRequest;

    public PerformZoneSerialRequests(PD pipelineData,
                                     Event completeEvent,
                                     ByteArray key,
                                     FailureDetector failureDetector,
                                     Map<Integer, Store<ByteArray, byte[], byte[]>> stores,
                                     StoreRequest<V> storeRequest) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.stores = stores;
        this.storeRequest = storeRequest;
    }

    public void execute(Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();

        while(pipelineData.getNodeIndex() < nodes.size()
              && (pipelineData.getZoneResponses().size() + 1) < pipelineData.getZonesRequired()) {
            Node node = nodes.get(pipelineData.getNodeIndex());
            long start = System.nanoTime();

            try {
                Store<ByteArray, byte[], byte[]> store = stores.get(node.getId());
                V result = storeRequest.request(store);

                Response<ByteArray, V> response = new Response<ByteArray, V>(node,
                                                                             key,
                                                                             result,
                                                                             ((System.nanoTime() - start) / Time.NS_PER_MS));

                pipelineData.incrementSuccesses();
                pipelineData.getResponses().add(response);
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(node.getZoneId());
            } catch(Exception e) {
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

                if(handleResponseError(e, node, requestTime, pipeline, failureDetector))
                    return;
            }

            pipelineData.incrementNodeIndex();
        }

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
    }
}