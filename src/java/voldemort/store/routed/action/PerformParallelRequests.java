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

import voldemort.cluster.Node;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;

public class PerformParallelRequests extends AbstractAction<BasicPipelineData> {

    private NonblockingStoreRequest storeRequest;

    public NonblockingStoreRequest getStoreRequest() {
        return storeRequest;
    }

    public void setStoreRequest(NonblockingStoreRequest storeRequest) {
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