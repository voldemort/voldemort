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
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.ListStateData;
import voldemort.store.routed.StateMachine;
import voldemort.store.routed.StateMachineEventNonblockingStoreCallback;
import voldemort.versioning.Versioned;

public class PerformParallelPutRequests extends AbstractKeyBasedAction<ListStateData> {

    public void execute(StateMachine stateMachine, Object eventData) {
        Node master = stateData.getMaster();
        Versioned<byte[]> versionedCopy = stateData.getVersionedCopy();

        if(logger.isDebugEnabled())
            logger.debug("Serial put requests determined master node as " + master.getId()
                         + ", submitting remaining requests in parallel");

        List<Node> nodes = stateData.getNodes();
        int currentNode = nodes.indexOf(master) + 1;
        int attempts = 0;

        for(; currentNode < nodes.size(); currentNode++) {
            attempts++;
            Node node = nodes.get(currentNode);
            NonblockingStore store = nonblockingStores.get(node.getId());

            NonblockingStoreCallback callback = new StateMachineEventNonblockingStoreCallback(stateMachine,
                                                                                              node,
                                                                                              key);

            if(logger.isTraceEnabled())
                logger.trace("Submitting request to put on node " + node.getId());

            store.submitPutRequest(key, versionedCopy, callback);
        }

        stateData.setAttempts(attempts);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + stateData.getAttempts() + " "
                         + stateData.getOperation().getSimpleName() + " operations in parallel");

        stateMachine.addEvent(completeEvent);
    }

}