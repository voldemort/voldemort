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

package voldemort.store.slop;

import org.apache.log4j.Logger;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;

/**
 * Hinted handoff: if, when processing a pipeline for a given request, failures shall
 * occur on specific nodes, the requests for these failed nodes should be queued up on other,
 * currently available nodes. Semantics of the operation should not change i.e., if
 * <code>requires-writes</code> are not met, the request should still be considered a
 * failure.
 */
public class HintedHandoff {

    private static final Logger logger = Logger.getLogger(HintedHandoff.class);
    
    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, Slop>> slopStores;

    private final HintedHandoffStrategy handoffStrategy;

    private final List<Node> failedNodes;

    /**
     * Create a Hinted Handoff object
     *
     * @param failureDetector The failure detector
     * @param slopStores A map of node ids to slop stores for these node ids
     * @param handoffStrategy The {@link HintedHandoffStrategy} implementation
     * @param failedNodes A list of nodes in the original preflist for the request
     *        that have failed or are unavailable
     */
    public HintedHandoff(FailureDetector failureDetector,
                         Map<Integer, Store<ByteArray, Slop>> slopStores,
                         HintedHandoffStrategy handoffStrategy,
                         List<Node> failedNodes) {
        this.failureDetector = failureDetector;
        this.slopStores = slopStores;
        this.handoffStrategy = handoffStrategy;
        this.failedNodes = failedNodes;
    }

    /**
     * Send a hint of a request originally meant for the failed node to
     * another node in the ring, as selected by the {@link HintedHandoffStrategy}
     * implementation passed in the constructor
     *
     * @param failedNode The node the request was originally meant for
     * @param version The version of the request's object
     * @param slop The hint
     * @return True if persisted on another node, false otherwise
     */
    public boolean sendHint(Node failedNode, Version version, Slop slop) {
        boolean persisted = false;
        for(Node node: handoffStrategy.routeHint(failedNode)) {
            int nodeId = node.getId();
            if(logger.isTraceEnabled())
                logger.trace("Trying to send hint to " + nodeId);
            
            if(!failedNodes.contains(node) && failureDetector.isAvailable(node)) {
                Store<ByteArray, Slop> slopStore = slopStores.get(nodeId);
                Utils.notNull(slopStore);
                long startNs = System.nanoTime();

                try {
                    if(logger.isTraceEnabled())
                        logger.trace("Attempt to write " + slop.getKey() + " for "
                                     + failedNode + " to node " + node);

                    slopStore.put(slop.makeKey(),
                                  new Versioned<Slop>(slop, version));

                    persisted = true;
                    failureDetector.recordSuccess(node,
                                                  (System.nanoTime() - startNs) / Time.NS_PER_MS);
                    if(logger.isTraceEnabled())
                        logger.trace("Finished hinted handoff for " + failedNode
                                     + " wrote slop to " + node);
                    break;
                } catch(UnreachableStoreException e) {
                    failureDetector.recordException(node,
                                                    (System.nanoTime() - startNs) / Time.NS_PER_MS,
                                                    e);
                    logger.warn("Error during hinted handoff", e);
                }
            }
        }

        return persisted;
    }
}
