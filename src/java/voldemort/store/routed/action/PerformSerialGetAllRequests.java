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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

public class PerformSerialGetAllRequests
        extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    protected final Iterable<ByteArray> keys;

    protected final FailureDetector failureDetector;

    protected final Map<Integer, Store<ByteArray, byte[]>> stores;

    protected final int preferred;

    protected final int required;

    public PerformSerialGetAllRequests(GetAllPipelineData pipelineData,
                                       Event completeEvent,
                                       Iterable<ByteArray> keys,
                                       FailureDetector failureDetector,
                                       Map<Integer, Store<ByteArray, byte[]>> stores,
                                       int preferred,
                                       int required) {
        super(pipelineData, completeEvent);
        this.keys = keys;
        this.failureDetector = failureDetector;
        this.stores = stores;
        this.preferred = preferred;
        this.required = required;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        for(ByteArray key: keys) {
            MutableInt successCount = pipelineData.getSuccessCount(key);

            if(successCount.intValue() >= preferred)
                continue;

            List<Node> extraNodes = pipelineData.getKeyToExtraNodesMap().get(key);

            if(extraNodes == null)
                continue;

            for(Node node: extraNodes) {
                long start = System.nanoTime();

                try {
                    Store<ByteArray, byte[]> store = stores.get(node.getId());
                    List<Versioned<byte[]>> result = store.get(key);

                    Map<ByteArray, List<Versioned<byte[]>>> map = new HashMap<ByteArray, List<Versioned<byte[]>>>();
                    map.put(key, result);

                    Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> response = new Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>>(node,
                                                                                                                                                                                 Arrays.asList(key),
                                                                                                                                                                                 map,
                                                                                                                                                                                 ((System.nanoTime() - start) / Time.NS_PER_MS));

                    successCount.increment();
                    pipelineData.getResponses().add(response);
                    failureDetector.recordSuccess(response.getNode(), response.getRequestTime());

                    if(successCount.intValue() >= preferred)
                        break;
                } catch(UnreachableStoreException e) {
                    if(logger.isTraceEnabled())
                        logger.trace("GetAll on node " + node.getId() + " failed: " + e);

                    pipelineData.recordFailure(e);
                    long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;
                    failureDetector.recordException(node, requestTime, e);
                } catch(VoldemortApplicationException e) {
                    pipelineData.setFatalError(e);
                    pipeline.addEvent(Event.ERROR);
                    return;
                } catch(Exception e) {
                    if(logger.isTraceEnabled())
                        logger.trace("GetAll on node " + node.getId() + " failed: " + e);

                    pipelineData.recordFailure(e);
                }
            }
        }
    }
}