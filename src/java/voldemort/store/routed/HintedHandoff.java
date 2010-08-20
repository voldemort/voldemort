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

package voldemort.store.routed;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class HintedHandoff {

    private static final Logger logger = Logger.getLogger(HintedHandoff.class);
    
    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, Slop>> slopStores;

    private final List<Node> nodes;

    private final List<Node> failedNodes;

    public HintedHandoff(FailureDetector failureDetector,
                         Map<Integer, Store<ByteArray, Slop>> slopStores,
                         Collection<Node> nodes,
                         List<Node> failedNodes) {
        this.failureDetector = failureDetector;
        this.slopStores = slopStores;
        this.nodes = Lists.newArrayList(nodes);
        this.failedNodes = failedNodes;

        // shuffle potential slop nodes to avoid cascading failures
        Collections.shuffle(this.nodes, new Random());
    }


    public boolean sendHint(Node failedNode, Version version, Slop slop) {
        Set<Node> used = Sets.newHashSetWithExpectedSize(nodes.size());
        boolean persisted = false;
        for(Node node: nodes) {
            int nodeId = node.getId();

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
                    used.add(node);

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

        if(nodes.size() > used.size())
            for(Node usedNode: used)
                nodes.remove(usedNode);
        return persisted;
    }
}
