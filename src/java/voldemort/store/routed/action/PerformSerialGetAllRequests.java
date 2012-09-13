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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Response;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class PerformSerialGetAllRequests
        extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    private final Iterable<ByteArray> keys;

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;

    private final int preferred;

    private final int required;

    private final boolean allowPartial;

    public PerformSerialGetAllRequests(GetAllPipelineData pipelineData,
                                       Event completeEvent,
                                       Iterable<ByteArray> keys,
                                       FailureDetector failureDetector,
                                       Map<Integer, Store<ByteArray, byte[], byte[]>> stores,
                                       int preferred,
                                       int required,
                                       boolean allowPartial) {
        super(pipelineData, completeEvent);
        this.keys = keys;
        this.failureDetector = failureDetector;
        this.stores = stores;
        this.preferred = preferred;
        this.required = required;
        this.allowPartial = allowPartial;
    }

    public void execute(Pipeline pipeline) {
        Map<ByteArray, List<Versioned<byte[]>>> result = pipelineData.getResult();

        for(ByteArray key: keys) {
            boolean zoneRequirement = false;
            MutableInt successCount = pipelineData.getSuccessCount(key);

            if(logger.isDebugEnabled())
                logger.debug("GETALL for key " + ByteUtils.toHexString(key.get()) + " (keyRef: "
                             + System.identityHashCode(key) + ") successes: "
                             + successCount.intValue() + " preferred: " + preferred + " required: "
                             + required);

            if(successCount.intValue() >= preferred) {
                if(pipelineData.getZonesRequired() != null && pipelineData.getZonesRequired() > 0) {

                    if(pipelineData.getKeyToZoneResponse().containsKey(key)) {
                        int zonesSatisfied = pipelineData.getKeyToZoneResponse().get(key).size();
                        if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                            continue;
                        } else {
                            zoneRequirement = true;
                        }
                    } else {
                        zoneRequirement = true;
                    }

                } else {
                    continue;
                }
            }

            List<Node> extraNodes = pipelineData.getKeyToExtraNodesMap().get(key);
            Map<ByteArray, byte[]> transforms = pipelineData.getTransforms();

            if(extraNodes == null)
                continue;

            for(Node node: extraNodes) {
                long start = System.nanoTime();

                try {
                    Store<ByteArray, byte[], byte[]> store = stores.get(node.getId());
                    List<Versioned<byte[]>> values;
                    if(transforms == null)
                        values = store.get(key, null);
                    else
                        values = store.get(key, transforms.get(key));

                    if(values.size() != 0) {
                        if(result.get(key) == null)
                            result.put(key, Lists.newArrayList(values));
                        else
                            result.get(key).addAll(values);
                    }

                    Map<ByteArray, List<Versioned<byte[]>>> map = new HashMap<ByteArray, List<Versioned<byte[]>>>();
                    map.put(key, values);

                    Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> response = new Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>>(node,
                                                                                                                                                                                 Arrays.asList(key),
                                                                                                                                                                                 map,
                                                                                                                                                                                 ((System.nanoTime() - start) / Time.NS_PER_MS));

                    successCount.increment();
                    pipelineData.getResponses().add(response);
                    failureDetector.recordSuccess(response.getNode(), response.getRequestTime());

                    if(logger.isDebugEnabled())
                        logger.debug("GET for key " + ByteUtils.toHexString(key.get())
                                     + " (keyRef: " + System.identityHashCode(key)
                                     + ") successes: " + successCount.intValue() + " preferred: "
                                     + preferred + " required: " + required
                                     + " new GET success on node " + node.getId());

                    HashSet<Integer> zoneResponses = null;
                    if(pipelineData.getKeyToZoneResponse().containsKey(key)) {
                        zoneResponses = pipelineData.getKeyToZoneResponse().get(key);
                    } else {
                        zoneResponses = new HashSet<Integer>();
                        pipelineData.getKeyToZoneResponse().put(key, zoneResponses);
                    }
                    zoneResponses.add(response.getNode().getZoneId());

                    if(zoneRequirement) {
                        if(zoneResponses.size() >= pipelineData.getZonesRequired())
                            break;
                    } else {
                        if(successCount.intValue() >= preferred)
                            break;
                    }

                } catch(Exception e) {
                    long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

                    if(handleResponseError(e, node, requestTime, pipeline, failureDetector))
                        return;
                }
            }
        }

        for(ByteArray key: keys) {
            MutableInt successCount = pipelineData.getSuccessCount(key);

            if(successCount.intValue() < required) {
                // if we allow partial results, then just remove keys that did
                // not meet 'required' guarantee; else raise error
                if(allowPartial) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Excluding Key " + ByteUtils.toHexString(key.get())
                                     + " from partial get_all result");
                    }
                    result.remove(key);
                } else {
                    pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                                 + " "
                                                                                                 + pipeline.getOperation()
                                                                                                           .getSimpleName()
                                                                                                 + "s required, but "
                                                                                                 + successCount.intValue()
                                                                                                 + " succeeded. Failing nodes : "
                                                                                                 + pipelineData.getFailedNodes(),
                                                                                         pipelineData.getFailures()));
                    pipeline.addEvent(Event.ERROR);
                    return;
                }
            }
        }

        pipeline.addEvent(completeEvent);
    }

}
