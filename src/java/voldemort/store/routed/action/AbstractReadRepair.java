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

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.PipelineData;
import voldemort.store.routed.ReadRepairer;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public abstract class AbstractReadRepair<K, V, PD extends PipelineData<K, V>> extends
        AbstractAction<K, V, PD> {

    private final int preferred;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final ReadRepairer<ByteArray, byte[]> readRepairer;

    private final List<NodeValue<ByteArray, byte[]>> nodeValues;

    public AbstractReadRepair(PD pipelineData,
                              Event completeEvent,
                              int preferred,
                              long timeoutMs,
                              Map<Integer, NonblockingStore> nonblockingStores,
                              ReadRepairer<ByteArray, byte[]> readRepairer) {
        super(pipelineData, completeEvent);
        this.preferred = preferred;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.readRepairer = readRepairer;
        this.nodeValues = Lists.newArrayListWithExpectedSize(pipelineData.getResponses().size());
    }

    protected abstract void insertNodeValues();

    protected void insertNodeValue(Node node, ByteArray key, List<Versioned<byte[]>> value) {
        if(value.size() == 0) {
            Versioned<byte[]> versioned = new Versioned<byte[]>(null);
            nodeValues.add(new NodeValue<ByteArray, byte[]>(node.getId(), key, versioned));
        } else {
            for(Versioned<byte[]> versioned: value)
                nodeValues.add(new NodeValue<ByteArray, byte[]>(node.getId(), key, versioned));
        }
    }

    public void execute(Pipeline pipeline) {
        insertNodeValues();

        long startTimeNs = -1;

        if(logger.isTraceEnabled())
            startTimeNs = System.nanoTime();

        if(nodeValues.size() > 1 && preferred > 1) {
            List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();

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
                                     + ByteUtils.toHexString(v.getKey().get()) + "' with version "
                                     + v.getVersion() + ".");

                    NonblockingStore store = nonblockingStores.get(v.getNodeId());
                    store.submitPutRequest(v.getKey(), v.getVersioned(), null, null, timeoutMs);
                } catch(VoldemortApplicationException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Read repair cancelled due to application level exception on node "
                                     + v.getNodeId()
                                     + " for key '"
                                     + ByteUtils.toHexString(v.getKey().get())
                                     + "' with version "
                                     + v.getVersion() + ": " + e.getMessage());
                } catch(Exception e) {
                    logger.debug("Read repair failed: ", e);
                }
            }

            if(logger.isDebugEnabled()) {
                String logStr = "Repaired (node, key, version): (";
                for(NodeValue<ByteArray, byte[]> v: toReadRepair) {
                    logStr += "(" + v.getNodeId() + ", " + v.getKey() + "," + v.getVersion() + ") ";
                }
                logStr += "in " + (System.nanoTime() - startTimeNs) + " ns";
                logger.debug(logStr);
            }
        }

        pipeline.addEvent(completeEvent);
    }

}
