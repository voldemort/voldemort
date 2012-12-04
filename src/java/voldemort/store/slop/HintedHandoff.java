/*
 * Copyright 2010-2012 LinkedIn, Inc
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

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Response;
import voldemort.store.slop.strategy.HintedHandoffStrategy;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Hinted handoff: if, when processing a pipeline for a given request, failures
 * shall occur on specific nodes, the requests for these failed nodes should be
 * queued up on other, currently available nodes. Semantics of the operation
 * should not change i.e., if <code>required-writes</code> are not met, the
 * request should still be considered a failure.
 */
public class HintedHandoff {

    private static final Logger logger = Logger.getLogger(HintedHandoff.class);

    private static final Serializer<Slop> slopSerializer = new SlopSerializer();

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores;

    private final Map<Integer, NonblockingStore> nonblockingSlopStores;

    private final HintedHandoffStrategy handoffStrategy;

    private final List<Node> failedNodes;

    private final long timeoutMs;

    /**
     * Create a Hinted Handoff object
     * 
     * @param failureDetector The failure detector
     * @param nonblockingSlopStores A map of node ids to nonb-locking slop
     *        stores
     * @param slopStores A map of node ids to blocking slop stores
     * @param handoffStrategy The {@link HintedHandoffStrategy} implementation
     * @param failedNodes A list of nodes in the original preflist for the
     *        request that have failed or are unavailable
     * @param timeoutMs Timeout for slop stores
     */
    public HintedHandoff(FailureDetector failureDetector,
                         Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                         Map<Integer, NonblockingStore> nonblockingSlopStores,
                         HintedHandoffStrategy handoffStrategy,
                         List<Node> failedNodes,
                         long timeoutMs) {
        this.failureDetector = failureDetector;
        this.slopStores = slopStores;
        this.nonblockingSlopStores = nonblockingSlopStores;
        this.handoffStrategy = handoffStrategy;
        this.failedNodes = failedNodes;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Like
     * {@link #sendHintSerial(voldemort.cluster.Node, voldemort.versioning.Version, Slop)}
     * , but doesn't block the pipeline. Intended for handling prolonged
     * failures without incurring a performance cost.
     * 
     * @see #sendHintSerial(voldemort.cluster.Node,
     *      voldemort.versioning.Version, Slop)
     */
    public void sendHintParallel(final Node failedNode, final Version version, final Slop slop) {
        final ByteArray slopKey = slop.makeKey();
        Versioned<byte[]> slopVersioned = new Versioned<byte[]>(slopSerializer.toBytes(slop),
                                                                version);

        for(final Node node: handoffStrategy.routeHint(failedNode)) {
            int nodeId = node.getId();

            if(logger.isDebugEnabled())
                logger.debug("Sending an async hint to " + nodeId);

            if(!failedNodes.contains(node) && failureDetector.isAvailable(node)) {
                NonblockingStore nonblockingStore = nonblockingSlopStores.get(nodeId);
                Utils.notNull(nonblockingStore);
                final long startNs = System.nanoTime();

                if(logger.isDebugEnabled())
                    logger.debug("Slop attempt to write " + slop.getKey() + " for " + failedNode
                                 + " to node " + node);

                NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                    public void requestComplete(Object result, long requestTime) {
                        Response<ByteArray, Object> response = new Response<ByteArray, Object>(node,
                                                                                               slopKey,
                                                                                               result,
                                                                                               requestTime);
                        if(response.getValue() instanceof Exception) {
                            if(response.getValue() instanceof ObsoleteVersionException) {
                                // Ignore

                                // TODO: Treating ObsoleteVersionException as
                                // "success", but there is no logger.debug to
                                // note that the slop was written, nor is there
                                // a failureDetector.recordSuccess invocation.
                            } else {
                                // Use the blocking approach
                                if(!failedNodes.contains(node))
                                    failedNodes.add(node);
                                if(response.getValue() instanceof UnreachableStoreException) {
                                    UnreachableStoreException use = (UnreachableStoreException) response.getValue();

                                    if(logger.isDebugEnabled()) {
                                        logger.debug("Write of key " + slop.getKey() + " for "
                                                     + failedNode + " to node " + node
                                                     + " failed due to unreachable: "
                                                     + use.getMessage());
                                    }

                                    failureDetector.recordException(node,
                                                                    (System.nanoTime() - startNs)
                                                                            / Time.NS_PER_MS,
                                                                    use);
                                }
                                sendHintSerial(failedNode, version, slop);
                            }
                            return;
                        }

                        if(logger.isDebugEnabled())
                            logger.debug("Slop write of key " + slop.getKey() + " for "
                                         + failedNode + " to node " + node + " succeeded in "
                                         + (System.nanoTime() - startNs) + " ns");

                        failureDetector.recordSuccess(node, (System.nanoTime() - startNs)
                                                            / Time.NS_PER_MS);

                    }
                };

                nonblockingStore.submitPutRequest(slopKey, slopVersioned, null, callback, timeoutMs);
                break;
            }
        }
    }

    /**
     * Send a hint of a request originally meant for the failed node to another
     * node in the ring, as selected by the {@link HintedHandoffStrategy}
     * implementation passed in the constructor
     * 
     * @param failedNode The node the request was originally meant for
     * @param version The version of the request's object
     * @param slop The hint
     * @return True if persisted on another node, false otherwise
     */
    public boolean sendHintSerial(Node failedNode, Version version, Slop slop) {
        boolean persisted = false;
        for(Node node: handoffStrategy.routeHint(failedNode)) {
            int nodeId = node.getId();
            if(logger.isDebugEnabled())
                logger.debug("Trying to send hint to " + nodeId);

            if(!failedNodes.contains(node) && failureDetector.isAvailable(node)) {
                Store<ByteArray, Slop, byte[]> slopStore = slopStores.get(nodeId);
                Utils.notNull(slopStore);
                long startNs = System.nanoTime();

                try {
                    if(logger.isDebugEnabled())
                        logger.debug("Slop attempt to write " + slop.getKey() + " (keyRef: "
                                     + System.identityHashCode(slop.getKey()) + ") for "
                                     + failedNode + " to node " + node);

                    // No transform needs to applied to the slop
                    slopStore.put(slop.makeKey(), new Versioned<Slop>(slop, version), null);

                    persisted = true;
                    failureDetector.recordSuccess(node, (System.nanoTime() - startNs)
                                                        / Time.NS_PER_MS);
                    if(logger.isTraceEnabled())
                        logger.trace("Finished hinted handoff for " + failedNode
                                     + " wrote slop to " + node);
                    break;
                } catch(UnreachableStoreException e) {
                    failureDetector.recordException(node, (System.nanoTime() - startNs)
                                                          / Time.NS_PER_MS, e);
                    logger.warn("Error during hinted handoff", e);
                } catch(ObsoleteVersionException e) {
                    logger.debug(e, e);
                }

                if(logger.isDebugEnabled())
                    logger.debug("Slop write of key " + slop.getKey() + " (keyRef: "
                                 + System.identityHashCode(slop.getKey()) + ") for " + failedNode
                                 + " to node " + node + " succeeded in "
                                 + (System.nanoTime() - startNs) + " ns");
            }
        }

        if(!persisted) {
            logger.error("Slop write of key " + slop.getKey() + " (keyRef: "
                         + System.identityHashCode(slop.getKey()) + ") for " + failedNode
                         + " was not written.");
        }
        return persisted;
    }
}
