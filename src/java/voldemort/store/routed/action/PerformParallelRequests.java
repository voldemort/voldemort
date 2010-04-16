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
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;

public class PerformParallelRequests<V, PD extends BasicPipelineData<V>> extends AbstractAction<PD> {

    protected final int preferred;

    protected final Map<Integer, NonblockingStore> nonblockingStores;

    protected final NonblockingStoreRequest storeRequest;

    public PerformParallelRequests(PD pipelineData,
                                   Event completeEvent,
                                   int preferred,
                                   Map<Integer, NonblockingStore> nonblockingStores,
                                   NonblockingStoreRequest storeRequest) {
        super(pipelineData, completeEvent);
        this.preferred = preferred;
        this.nonblockingStores = nonblockingStores;
        this.storeRequest = storeRequest;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        List<Node> nodes = pipelineData.getNodes();
        pipelineData.setAttempts(nodes.size());

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + pipelineData.getAttempts() + " "
                         + pipeline.getOperation().getSimpleName() + " operations in parallel");

        if(preferred <= 0 && completeEvent != null)
            pipeline.addEvent(completeEvent);

        for(Node node: nodes) {
            pipelineData.incrementNodeIndex();
            NonblockingStore store = nonblockingStores.get(node.getId());
            storeRequest.request(node, store);
        }
    }

    public interface NonblockingStoreRequest {

        public void request(Node node, NonblockingStore store);

    }

}