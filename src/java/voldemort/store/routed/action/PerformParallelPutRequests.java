/*
 * Copyright 2012 LinkedIn, Inc
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Response;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

public class PerformParallelPutRequests extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private byte[] transforms;

    private final HintedHandoff hintedHandoff;

    public boolean enableHintedHandoff;

    private boolean quorumSatisfied = false;
    private boolean zonesSatisfied = false;
    private Integer numResponsesGot = 0;
    private Integer numNodesPendingResponse = 0;

    public PerformParallelPutRequests(PutPipelineData pipelineData,
                                      Event completeEvent,
                                      ByteArray key,
                                      byte[] transforms,
                                      FailureDetector failureDetector,
                                      int preferred,
                                      int required,
                                      long timeoutMs,
                                      Map<Integer, NonblockingStore> nonblockingStores,
                                      HintedHandoff hintedHandoff) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.transforms = transforms;
        this.nonblockingStores = nonblockingStores;
        this.hintedHandoff = hintedHandoff;
        this.enableHintedHandoff = hintedHandoff != null;
    }

    public boolean isHintedHandoffEnabled() {
        return enableHintedHandoff;
    }

    @Override
    public void execute(final Pipeline pipeline) {
        final Node masterNode = pipelineData.getMaster();
        final List<Node> nodes = pipelineData.getNodes();
        final Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();
        final Integer numNodesTouchedInSerialPut = nodes.indexOf(masterNode) + 1;
        numNodesPendingResponse = nodes.size() - numNodesTouchedInSerialPut;

        if(logger.isDebugEnabled())
            logger.debug("PUT {key:" + key + "} MasterNode={id:" + masterNode.getId()
                         + "} totalNodesToAsyncPut=" + numNodesPendingResponse);

        // initiate parallel puts
        for(int i = numNodesTouchedInSerialPut; i < nodes.size(); i++) {
            final Node node = nodes.get(i);
            pipelineData.incrementNodeIndex();

            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                @Override
                public void requestComplete(Object result, long requestTime) {
                    boolean responseHandledByMaster = false;
                    if(logger.isDebugEnabled())
                        logger.debug("PUT {key:" + key + "} response received from node={id:"
                                     + node.getId() + "} in " + requestTime + " ms)");

                    Response<ByteArray, Object> response;
                    response = new Response<ByteArray, Object>(node, key, result, requestTime);

                    if(logger.isDebugEnabled()) {
                        logger.debug("PUT {key:"
                                     + key
                                     + "} Parallel put thread trying to return result to main thread");
                    }

                    responseHandledByMaster = pipelineData.getSynchronizer()
                                                          .tryDelegateResponseHandling(response);

                    if(logger.isDebugEnabled()) {
                        logger.debug("PUT {key:" + key + "} Master thread accepted the response: "
                                     + responseHandledByMaster);
                    }

                    if(!responseHandledByMaster) {
                        if(isSlopableFailure(response.getValue())) {
                            if(logger.isDebugEnabled())
                                logger.debug("PUT {key:" + key + "} failed on node={id:"
                                             + node.getId() + ",host:" + node.getHost() + "}");

                            if(isHintedHandoffEnabled()) {
                                boolean triedDelegateSlop = pipelineData.getSynchronizer()
                                                                        .tryDelegateSlop(node);
                                if(logger.isDebugEnabled()) {
                                    logger.debug("PUT {key:" + key + "} triedDelegateSlop: "
                                                 + triedDelegateSlop);
                                }
                                if(!triedDelegateSlop) {
                                    Slop slop = new Slop(pipelineData.getStoreName(),
                                                         Slop.Operation.PUT,
                                                         key,
                                                         versionedCopy.getValue(),
                                                         transforms,
                                                         node.getId(),
                                                         new Date());
                                    pipelineData.addFailedNode(node);
                                    if(logger.isDebugEnabled())
                                        logger.debug("PUT {key:" + key
                                                     + "} Start registering Slop(node:"
                                                     + node.getId() + ",host:" + node.getHost()
                                                     + ")");
                                    hintedHandoff.sendHintSerial(node,
                                                                 versionedCopy.getVersion(),
                                                                 slop);
                                    if(logger.isDebugEnabled())
                                        logger.debug("PUT {key:" + key
                                                     + "} Finished registering Slop(node:"
                                                     + node.getId() + ",host:" + node.getHost()
                                                     + ")");
                                }
                            }
                        } else {
                            if(logger.isDebugEnabled()) {
                                if(result instanceof Exception) {
                                    logger.debug("PUT {key:"
                                                 + key
                                                 + "} will not send hint. Response is ignorable exception: "
                                                 + result.getClass().toString());
                                } else {
                                    logger.debug("PUT {key:" + key
                                                 + "} will not send hint. Response is normal");
                                }
                            }
                        }

                        if(result instanceof Exception
                           && !(result instanceof ObsoleteVersionException)) {
                            if(response.getValue() instanceof InvalidMetadataException) {
                                pipelineData.reportException((InvalidMetadataException) response.getValue());
                                logger.warn("Received invalid metadata problem after a successful "
                                            + pipeline.getOperation().getSimpleName()
                                            + " call on node " + node.getId() + ", store '"
                                            + pipelineData.getStoreName() + "'");
                            } else {
                                handleResponseError(response, pipeline, failureDetector);
                            }
                        }
                    }
                }
            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId() + " for key " + key);

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitPutRequest(key, versionedCopy, transforms, callback, timeoutMs);
        }

        try {
            boolean preferredSatisfied = false;
            while(true) {
                long ellapsedNs = System.nanoTime() - pipelineData.getStartTimeNs();
                long remainingNs = (timeoutMs * Time.NS_PER_MS) - ellapsedNs;
                remainingNs = Math.max(0, remainingNs);
                // preferred check
                if(numResponsesGot >= preferred - 1) {
                    preferredSatisfied = true;
                }

                // quorum check
                if(pipelineData.getSuccesses() >= required) {
                    quorumSatisfied = true;
                }

                // zone check
                if(pipelineData.getZonesRequired() == null) {
                    zonesSatisfied = true;
                } else {
                    int numZonesSatisfied = pipelineData.getZoneResponses().size();
                    if(numZonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                        zonesSatisfied = true;
                    }
                }

                if(quorumSatisfied && zonesSatisfied && preferredSatisfied || remainingNs <= 0
                   || numNodesPendingResponse <= 0) {
                    pipelineData.getSynchronizer().cutoffHandling();
                    break;
                } else {
                    if(logger.isTraceEnabled()) {
                        logger.trace("PUT {key:" + key + "} trying to poll from queue");
                    }
                    Response<ByteArray, Object> response = pipelineData.getSynchronizer()
                                                                       .responseQueuePoll(remainingNs,
                                                                                          TimeUnit.NANOSECONDS);
                    processResponse(response, pipeline);
                    if(logger.isTraceEnabled()) {
                        logger.trace("PUT {key:" + key + "} tried to poll from queue. Null?: "
                                     + (response == null) + " numResponsesGot:" + numResponsesGot
                                     + " parellelResponseToWait: " + numNodesPendingResponse
                                     + "; preferred-1: " + (preferred - 1) + "; preferredOK: "
                                     + preferredSatisfied + " quromOK: " + quorumSatisfied
                                     + "; zoneOK: " + zonesSatisfied);
                    }

                }
            }

            // clean leftovers
            // a) The main thread did a processResponse, due to which the
            // criteria (quorum) was satisfied
            // b) After this, the main thread cuts off adding responses to the
            // queue by the async callbacks

            // An async callback can be invoked between a and b (this is the
            // leftover)
            while(!pipelineData.getSynchronizer().responseQueueIsEmpty()) {
                Response<ByteArray, Object> response = pipelineData.getSynchronizer()
                                                                   .responseQueuePoll(0,
                                                                                      TimeUnit.NANOSECONDS);
                processResponse(response, pipeline);
            }

            if(quorumSatisfied && zonesSatisfied) {
                if(logger.isDebugEnabled()) {
                    logger.debug("PUT {key:" + key + "} succeeded at parellel put stage");
                }
                pipelineData.getSynchronizer().disallowDelegateSlop();
                pipeline.addEvent(completeEvent);
            } else {
                VoldemortException fatalError;
                if(!quorumSatisfied) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("PUT {key:" + key
                                     + "} failed due to insufficient nodes. required=" + required
                                     + " success=" + pipelineData.getSuccesses());
                    }
                    fatalError = new InsufficientOperationalNodesException(required
                                                                                   + " "
                                                                                   + pipeline.getOperation()
                                                                                             .getSimpleName()
                                                                                   + "s required, but only "
                                                                                   + pipelineData.getSuccesses()
                                                                                   + " succeeded",
                                                                           new ArrayList<Node>(pipelineData.getReplicationSet()),
                                                                           new ArrayList<Node>(pipelineData.getNodes()),
                                                                           new ArrayList<Node>(pipelineData.getFailedNodes()),
                                                                           pipelineData.getFailures());
                    pipelineData.setFatalError(fatalError);
                } else if(!zonesSatisfied) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("PUT {key:" + key
                                     + "} failed due to insufficient zones. required="
                                     + pipelineData.getZonesRequired() + 1 + " success="
                                     + pipelineData.getZoneResponses().size());
                    }
                    fatalError = new InsufficientZoneResponsesException((pipelineData.getZonesRequired() + 1)
                                                                        + " "
                                                                        + pipeline.getOperation()
                                                                                  .getSimpleName()
                                                                        + "s required zone, but only "
                                                                        + zonesSatisfied
                                                                        + " succeeded. Failing nodes : "
                                                                        + pipelineData.getFailedNodes());
                    pipelineData.setFatalError(fatalError);
                }
                pipeline.abort();
            }
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        } catch(NoSuchElementException e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error("Response Queue is empty. There may be a bug in PerformParallelPutRequest",
                             e);
        } finally {
            if(logger.isDebugEnabled()) {
                logger.debug("PUT {key:" + key + "} marking parallel put stage finished");
            }
        }
    }

    /**
     * Process the response by reporting proper log and feeding failure
     * detectors
     * 
     * @param response
     * @param pipeline
     */
    private void processResponse(Response<ByteArray, Object> response, Pipeline pipeline) {
        if(response == null) {
            logger.warn("RoutingTimedout on waiting for async ops; parellelResponseToWait: "
                        + numNodesPendingResponse + "; preferred-1: " + (preferred - 1)
                        + "; quromOK: " + quorumSatisfied + "; zoneOK: " + zonesSatisfied);
        } else {
            numNodesPendingResponse = numNodesPendingResponse - 1;
            numResponsesGot = numResponsesGot + 1;
            if(response.getValue() instanceof Exception
               && !(response.getValue() instanceof ObsoleteVersionException)) {
                if(logger.isDebugEnabled()) {
                    logger.debug("PUT {key:" + key + "} handling async put error");
                }
                if(handleResponseError(response, pipeline, failureDetector)) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("PUT {key:" + key
                                     + "} severe async put error, exiting parallel put stage");
                    }

                    return;
                }
                if(isSlopableFailure(response.getValue())) {
                    pipelineData.getSynchronizer().tryDelegateSlop(response.getNode());
                }

                if(logger.isDebugEnabled()) {
                    logger.debug("PUT {key:" + key + "} handled async put error");
                }

            } else {
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
            }
        }
    }

    private boolean isSlopableFailure(Object object) {
        return object instanceof UnreachableStoreException
               || object instanceof PersistenceFailureException;
    }
}
