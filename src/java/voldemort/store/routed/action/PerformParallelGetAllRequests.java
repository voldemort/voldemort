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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.BasicResponseCallback;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class PerformParallelGetAllRequests
        extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    private final int preferred;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    public PerformParallelGetAllRequests(GetAllPipelineData pipelineData,
                                         Event completeEvent,
                                         int preferred,
                                         Map<Integer, NonblockingStore> nonblockingStores) {
        super(pipelineData, completeEvent);
        this.preferred = preferred;
        this.nonblockingStores = nonblockingStores;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        pipelineData.setAttempts(pipelineData.getNodeToKeysMap().size());

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + pipelineData.getAttempts() + " "
                         + pipeline.getOperation().getSimpleName() + " operations in parallel");

        if(preferred <= 0 && completeEvent != null)
            pipeline.addEvent(completeEvent);

        for(Map.Entry<Node, List<ByteArray>> entry: pipelineData.getNodeToKeysMap().entrySet()) {
            Node node = entry.getKey();
            Collection<ByteArray> keys = entry.getValue();
            NonblockingStoreCallback callback = new BasicResponseCallback<Iterable<ByteArray>>(pipeline,
                                                                                               node,
                                                                                               keys);

            if(logger.isTraceEnabled())
                logger.trace("Submitting request to getAll on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitGetAllRequest(keys, callback);
        }
    }

}