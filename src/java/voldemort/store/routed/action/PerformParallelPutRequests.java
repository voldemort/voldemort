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
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.BasicResponseCallback;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class PerformParallelPutRequests extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    protected final Map<Integer, NonblockingStore> nonblockingStores;

    public PerformParallelPutRequests(PutPipelineData pipelineData,
                                      Event completeEvent,
                                      ByteArray key,
                                      Map<Integer, NonblockingStore> nonblockingStores) {
        super(pipelineData, completeEvent, key);
        this.nonblockingStores = nonblockingStores;
    }

    public void execute(Pipeline pipeline, Object eventData) {
        Node master = pipelineData.getMaster();
        Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();

        if(logger.isDebugEnabled())
            logger.debug("Serial put requests determined master node as " + master.getId()
                         + ", submitting remaining requests in parallel");

        List<Node> nodes = pipelineData.getNodes();
        int currentNode = nodes.indexOf(master) + 1;
        int attempts = 0;

        for(; currentNode < nodes.size(); currentNode++) {
            attempts++;
            Node node = nodes.get(currentNode);
            NonblockingStore store = nonblockingStores.get(node.getId());

            NonblockingStoreCallback callback = new BasicResponseCallback<ByteArray>(pipeline,
                                                                                     node,
                                                                                     key);

            if(logger.isTraceEnabled())
                logger.trace("Submitting request to put on node " + node.getId());

            store.submitPutRequest(key, versionedCopy, callback);
        }

        pipelineData.setAttempts(attempts);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + pipelineData.getAttempts() + " "
                         + pipeline.getOperation().getSimpleName() + " operations in parallel");

        pipeline.addEvent(completeEvent);
    }

}