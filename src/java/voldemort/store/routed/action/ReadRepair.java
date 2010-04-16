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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.BasicResponseCallback;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.ReadRepairer;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ReadRepair<PD extends BasicPipelineData<List<Versioned<byte[]>>>> extends
        AbstractAction<ByteArray, List<Versioned<byte[]>>, PD> {

    protected final int preferred;

    protected final Map<Integer, NonblockingStore> nonblockingStores;

    protected final ReadRepairer<ByteArray, byte[]> readRepairer;

    public ReadRepair(PD pipelineData,
                      Event completeEvent,
                      int preferred,
                      Map<Integer, NonblockingStore> nonblockingStores,
                      ReadRepairer<ByteArray, byte[]> readRepairer) {
        super(pipelineData, completeEvent);
        this.preferred = preferred;
        this.nonblockingStores = nonblockingStores;
        this.readRepairer = readRepairer;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayListWithExpectedSize(pipelineData.getResponses()
                                                                                                       .size());
        Map<Integer, Node> nodes = new HashMap<Integer, Node>();

        for(Response<ByteArray, List<Versioned<byte[]>>> response: pipelineData.getResponses()) {
            List<Versioned<byte[]>> result = response.getValue();

            if(result.size() == 0) {
                nodeValues.add(new NodeValue<ByteArray, byte[]>(response.getNode().getId(),
                                                                response.getKey(),
                                                                new Versioned<byte[]>(null)));
            } else {
                for(Versioned<byte[]> versioned: result)
                    nodeValues.add(new NodeValue<ByteArray, byte[]>(response.getNode().getId(),
                                                                    response.getKey(),
                                                                    versioned));
            }

            nodes.put(response.getNode().getId(), response.getNode());
        }

        if(nodeValues.size() > 1 && preferred > 1) {
            final List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
            /*
             * We clone after computing read repairs in the assumption that the
             * output will be smaller than the input. Note that we clone the
             * version, but not the key or value as the latter two are not
             * mutated.
             */
            for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
                Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                              ((VectorClock) v.getVersion()).clone());
                toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(),
                                                                  v.getKey(),
                                                                  versioned));
            }

            for(NodeValue<ByteArray, byte[]> v: toReadRepair) {
                try {
                    if(logger.isDebugEnabled())
                        logger.debug("Doing read repair on node " + v.getNodeId() + " for key '"
                                     + v.getKey() + "' with version " + v.getVersion() + ".");

                    Node node = nodes.get(v.getNodeId());
                    NonblockingStore store = nonblockingStores.get(node.getId());
                    NonblockingStoreCallback callback = new BasicResponseCallback<ByteArray>(pipeline,
                                                                                             node,
                                                                                             v.getKey());
                    store.submitPutRequest(v.getKey(), v.getVersioned(), callback);
                } catch(VoldemortApplicationException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Read repair cancelled due to application level exception on node "
                                     + v.getNodeId()
                                     + " for key '"
                                     + v.getKey()
                                     + "' with version " + v.getVersion() + ": " + e.getMessage());
                } catch(Exception e) {
                    logger.debug("Read repair failed: ", e);
                }
            }
        }

        pipeline.addEvent(completeEvent);
    }

}
