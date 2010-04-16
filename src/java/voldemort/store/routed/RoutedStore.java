/*
 * Copyright 2008-2010 LinkedIn, Inc
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.AcknowledgeResponse;
import voldemort.store.routed.action.Action;
import voldemort.store.routed.action.ConfigureNodes;
import voldemort.store.routed.action.GetAllConfigureNodes;
import voldemort.store.routed.action.IncrementClock;
import voldemort.store.routed.action.PerformParallelPutRequests;
import voldemort.store.routed.action.PerformParallelRequests;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.store.routed.action.PerformSerialRequests;
import voldemort.store.routed.action.ReadRepair;
import voldemort.store.routed.action.UpdateResults;
import voldemort.store.routed.action.PerformParallelRequests.NonblockingStoreRequest;
import voldemort.store.routed.action.PerformSerialRequests.BlockingStoreRequest;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public class RoutedStore implements Store<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(RoutedStore.class.getName());

    private final String name;
    private final Map<Integer, Store<ByteArray, byte[]>> innerStores;
    private final Map<Integer, NonblockingStore> nonblockingStores;
    private final ExecutorService executor;
    private final boolean repairReads;
    private final ReadRepairer<ByteArray, byte[]> readRepairer;
    private final long timeoutMs;
    private final Time time;
    private final StoreDefinition storeDef;
    private final FailureDetector failureDetector;

    private volatile RoutingStrategy routingStrategy;

    /**
     * Create a RoutedStoreClient
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param routingStrategy The strategy for choosing a node given a key
     * @param requiredReads The minimum number of reads that must complete
     *        before the operation will return
     * @param requiredWrites The minimum number of writes that must complete
     *        before the operation will return
     * @param threadPool The threadpool to use
     */
    public RoutedStore(String name,
                       Map<Integer, Store<ByteArray, byte[]>> innerStores,
                       Map<Integer, NonblockingStore> nonblockingStores,
                       Cluster cluster,
                       StoreDefinition storeDef,
                       boolean repairReads,
                       ExecutorService threadPool,
                       long timeoutMs,
                       FailureDetector failureDetector) {
        if(storeDef.getRequiredReads() < 1)
            throw new IllegalArgumentException("Cannot have a storeDef.getRequiredReads() number less than 1.");
        if(storeDef.getRequiredWrites() < 1)
            throw new IllegalArgumentException("Cannot have a storeDef.getRequiredWrites() number less than 1.");
        if(storeDef.getPreferredReads() < storeDef.getRequiredReads())
            throw new IllegalArgumentException("storeDef.getPreferredReads() must be greater or equal to storeDef.getRequiredReads().");
        if(storeDef.getPreferredWrites() < storeDef.getRequiredWrites())
            throw new IllegalArgumentException("storeDef.getPreferredWrites() must be greater or equal to storeDef.getRequiredWrites().");
        if(storeDef.getPreferredReads() > innerStores.size())
            throw new IllegalArgumentException("storeDef.getPreferredReads() is larger than the total number of nodes!");
        if(storeDef.getPreferredWrites() > innerStores.size())
            throw new IllegalArgumentException("storeDef.getPreferredWrites() is larger than the total number of nodes!");

        this.name = name;
        this.innerStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[]>>(innerStores);
        this.nonblockingStores = new ConcurrentHashMap<Integer, NonblockingStore>(nonblockingStores);
        this.repairReads = repairReads;
        this.executor = threadPool;
        this.readRepairer = new ReadRepairer<ByteArray, byte[]>();
        this.timeoutMs = timeoutMs;
        this.time = SystemTime.INSTANCE;
        this.storeDef = storeDef;
        this.failureDetector = failureDetector;
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
    }

    public void updateRoutingStrategy(RoutingStrategy routingStrategy) {
        logger.info("Updating routing strategy for RoutedStore:" + getName());
        this.routingStrategy = routingStrategy;
    }

    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        BasicPipelineData pipelineData = new BasicPipelineData();
        final Pipeline pipeline = new Pipeline(Operation.DELETE);

        NonblockingStoreRequest nonblockingDelete = new NonblockingStoreRequest() {

            public void request(Node node, NonblockingStore store) {
                final NonblockingStoreCallback callback = new PipelineEventNonblockingStoreCallback(pipeline,
                                                                                                    node,
                                                                                                    key);
                store.submitDeleteRequest(key, version, callback);
            }

        };

        BlockingStoreRequest blockingDelete = new BlockingStoreRequest() {

            public Object request(Node node, Store<ByteArray, byte[]> store) {
                return store.delete(key, version);
            }

        };

        Action configureNodes = new ConfigureNodes(pipelineData,
                                                   Event.CONFIGURED,
                                                   failureDetector,
                                                   storeDef.getRequiredWrites(),
                                                   routingStrategy,
                                                   key);

        Action performRequests = new PerformParallelRequests(pipelineData,
                                                             Event.NOP,
                                                             storeDef.getPreferredWrites(),
                                                             nonblockingStores,
                                                             nonblockingDelete);

        Action acknowledgeResponse = new AcknowledgeResponse(pipelineData,
                                                             Event.COMPLETED,
                                                             failureDetector,
                                                             storeDef.getPreferredWrites(),
                                                             storeDef.getRequiredWrites(),
                                                             Event.INSUFFICIENT_SUCCESSES);

        Action performSerialRequests = new PerformSerialRequests(pipelineData,
                                                                 Event.COMPLETED,
                                                                 key,
                                                                 failureDetector,
                                                                 innerStores,
                                                                 storeDef.getPreferredWrites(),
                                                                 storeDef.getRequiredWrites(),
                                                                 blockingDelete,
                                                                 null);

        Action updateResults = new UpdateResults(pipelineData);

        Map<Event, Action> eventActions = new HashMap<Event, Action>();
        eventActions.put(Event.STARTED, configureNodes);
        eventActions.put(Event.CONFIGURED, performRequests);
        eventActions.put(Event.RESPONSE_RECEIVED, acknowledgeResponse);
        eventActions.put(Event.INSUFFICIENT_SUCCESSES, performSerialRequests);
        eventActions.put(Event.COMPLETED, updateResults);

        pipeline.setEventActions(eventActions);
        pipeline.addEvent(Event.STARTED);
        pipeline.processEvents(timeoutMs, TimeUnit.MILLISECONDS);

        List<Boolean> results = pipelineData.get();

        for(Boolean b: results) {
            if(b.booleanValue())
                return true;
        }

        return false;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        GetAllPipelineData pipelineData = new GetAllPipelineData();
        final Pipeline pipeline = new Pipeline(Operation.DELETE);

        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);

        Action configureNodes = new GetAllConfigureNodes(pipelineData,
                                                         null,
                                                         failureDetector,
                                                         storeDef.getPreferredReads(),
                                                         storeDef.getRequiredReads(),
                                                         routingStrategy,
                                                         keys);

        configureNodes.execute(pipeline, null);

        List<Callable<GetAllResult>> callables = Lists.newArrayList();
        for(Map.Entry<Node, List<ByteArray>> entry: pipelineData.getNodeToKeysMap().entrySet()) {
            final Node node = entry.getKey();
            final Collection<ByteArray> nodeKeys = entry.getValue();
            if(failureDetector.isAvailable(node))
                callables.add(new GetAllCallable(node, nodeKeys));
        }

        // A list of thrown exceptions, indicating the number of failures
        List<Throwable> failures = Lists.newArrayList();
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();

        Map<ByteArray, MutableInt> keyToSuccessCount = Maps.newHashMap();
        for(ByteArray key: keys)
            keyToSuccessCount.put(key, new MutableInt(0));

        List<Future<GetAllResult>> futures;
        try {
            // TODO What to do about timeouts? They should be longer as getAll
            // is likely to
            // take longer. At the moment, it's just timeoutMs * 3, but should
            // this be based on the number of the keys?
            futures = executor.invokeAll(callables, timeoutMs * 3, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            throw new InsufficientOperationalNodesException("getAll operation interrupted.", e);
        }
        for(Future<GetAllResult> f: futures) {
            if(f.isCancelled()) {
                logger.warn("Get operation timed out after " + timeoutMs + " ms.");
                continue;
            }
            try {
                GetAllResult getResult = f.get();
                if(getResult.exception != null) {
                    if(getResult.exception instanceof VoldemortApplicationException) {
                        throw (VoldemortException) getResult.exception;
                    }
                    failures.add(getResult.exception);
                    continue;
                }
                for(ByteArray key: getResult.callable.nodeKeys) {
                    List<Versioned<byte[]>> retrieved = getResult.retrieved.get(key);
                    MutableInt successCount = keyToSuccessCount.get(key);
                    successCount.increment();

                    /*
                     * retrieved can be null if there are no values for the key
                     * provided
                     */
                    if(retrieved != null) {
                        List<Versioned<byte[]>> existing = result.get(key);
                        if(existing == null)
                            result.put(key, Lists.newArrayList(retrieved));
                        else
                            existing.addAll(retrieved);
                    }
                }
                nodeValues.addAll(getResult.nodeValues);

            } catch(InterruptedException e) {
                throw new InsufficientOperationalNodesException("getAll operation interrupted.", e);
            } catch(ExecutionException e) {
                // We catch all Throwables apart from Error in the callable, so
                // the else part
                // should never happen
                if(e.getCause() instanceof Error)
                    throw (Error) e.getCause();
                else
                    logger.error(e.getMessage(), e);
            }
        }

        for(ByteArray key: keys) {
            MutableInt successCountWrapper = keyToSuccessCount.get(key);
            int successCount = successCountWrapper.intValue();
            if(successCount < storeDef.getPreferredReads()) {
                List<Node> extraNodes = pipelineData.getKeyToExtraNodesMap().get(key);
                if(extraNodes != null) {
                    for(Node node: extraNodes) {
                        long startNs = System.nanoTime();
                        try {
                            List<Versioned<byte[]>> values = innerStores.get(node.getId()).get(key);
                            fillRepairReadsValues(nodeValues, key, node, values);
                            List<Versioned<byte[]>> versioneds = result.get(key);
                            if(versioneds == null)
                                result.put(key, Lists.newArrayList(values));
                            else
                                versioneds.addAll(values);
                            recordSuccess(node, startNs);
                            if(++successCount >= storeDef.getPreferredReads())
                                break;

                        } catch(UnreachableStoreException e) {
                            failures.add(e);
                            recordException(node, startNs, e);
                        } catch(VoldemortApplicationException e) {
                            throw e;
                        } catch(Exception e) {
                            logger.warn("Error in GET_ALL on node " + node.getId() + "("
                                        + node.getHost() + ")", e);
                            failures.add(e);
                        }
                    }
                }
            }
            successCountWrapper.setValue(successCount);
        }

        repairReads(nodeValues);

        for(Map.Entry<ByteArray, MutableInt> mapEntry: keyToSuccessCount.entrySet()) {
            int successCount = mapEntry.getValue().intValue();
            if(successCount < storeDef.getRequiredReads())
                throw new InsufficientOperationalNodesException(this.storeDef.getRequiredReads()
                                                                        + " reads required, but "
                                                                        + successCount
                                                                        + " succeeded.",
                                                                failures);
        }

        return result;
    }

    public List<Versioned<byte[]>> get(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData pipelineData = new BasicPipelineData();
        final Pipeline pipeline = new Pipeline(Operation.GET);

        NonblockingStoreRequest nonblockingStoreRequest = new NonblockingStoreRequest() {

            public void request(Node node, NonblockingStore store) {
                final NonblockingStoreCallback callback = new PipelineEventNonblockingStoreCallback(pipeline,
                                                                                                    node,
                                                                                                    key);
                store.submitGetRequest(key, callback);
            }

        };

        BlockingStoreRequest blockingStoreRequest = new BlockingStoreRequest() {

            public Object request(Node node, Store<ByteArray, byte[]> store) {
                return store.get(key);
            }

        };

        Action configureNodes = new ConfigureNodes(pipelineData,
                                                   Event.CONFIGURED,
                                                   failureDetector,
                                                   storeDef.getRequiredReads(),
                                                   routingStrategy,
                                                   key);
        Action performRequests = new PerformParallelRequests(pipelineData,
                                                             Event.COMPLETED,
                                                             storeDef.getPreferredReads(),
                                                             nonblockingStores,
                                                             nonblockingStoreRequest);
        Action acknowledgeResponse = new AcknowledgeResponse(pipelineData,
                                                             repairReads ? Event.RESPONSES_RECEIVED
                                                                        : Event.COMPLETED,
                                                             failureDetector,
                                                             storeDef.getPreferredReads(),
                                                             storeDef.getRequiredReads(),
                                                             Event.INSUFFICIENT_SUCCESSES);
        Action performSerialRequests = new PerformSerialRequests(pipelineData,
                                                                 repairReads ? Event.RESPONSES_RECEIVED
                                                                            : Event.COMPLETED,
                                                                 key,
                                                                 failureDetector,
                                                                 innerStores,
                                                                 storeDef.getPreferredReads(),
                                                                 storeDef.getRequiredReads(),
                                                                 blockingStoreRequest,
                                                                 null);
        Action updateResults = new UpdateResults(pipelineData);

        Map<Event, Action> eventActions = new HashMap<Event, Action>();
        eventActions.put(Event.STARTED, configureNodes);
        eventActions.put(Event.CONFIGURED, performRequests);
        eventActions.put(Event.RESPONSE_RECEIVED, acknowledgeResponse);

        if(repairReads) {
            Action readRepair = new ReadRepair(pipelineData,
                                               Event.COMPLETED,
                                               storeDef.getPreferredReads(),
                                               nonblockingStores,
                                               readRepairer);
            eventActions.put(Event.RESPONSES_RECEIVED, readRepair);
        }

        eventActions.put(Event.INSUFFICIENT_SUCCESSES, performSerialRequests);
        eventActions.put(Event.COMPLETED, updateResults);

        pipeline.setEventActions(eventActions);
        pipeline.addEvent(Event.STARTED);
        pipeline.processEvents(timeoutMs, TimeUnit.MILLISECONDS);

        List<Versioned<byte[]>> results = pipelineData.get();

        return results;
    }

    private void fillRepairReadsValues(final List<NodeValue<ByteArray, byte[]>> nodeValues,
                                       final ByteArray key,
                                       Node node,
                                       List<Versioned<byte[]>> fetched) {
        if(repairReads) {
            if(fetched.size() == 0)
                nodeValues.add(nullValue(node, key));
            else {
                for(Versioned<byte[]> f: fetched)
                    nodeValues.add(new NodeValue<ByteArray, byte[]>(node.getId(), key, f));
            }
        }
    }

    private NodeValue<ByteArray, byte[]> nullValue(Node node, ByteArray key) {
        return new NodeValue<ByteArray, byte[]>(node.getId(), key, new Versioned<byte[]>(null));
    }

    private void repairReads(List<NodeValue<ByteArray, byte[]>> nodeValues) {
        if(!repairReads || nodeValues.size() <= 1 || storeDef.getPreferredReads() <= 1)
            return;

        final List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        /*
         * We clone after computing read repairs in the assumption that the
         * output will be smaller than the input. Note that we clone the
         * version, but not the key or value as the latter two are not mutated.
         */
        for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
            Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                          ((VectorClock) v.getVersion()).clone());
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(), v.getKey(), versioned));
        }

        this.executor.execute(new Runnable() {

            public void run() {
                for(NodeValue<ByteArray, byte[]> v: toReadRepair) {
                    try {
                        if(logger.isDebugEnabled())
                            logger.debug("Doing read repair on node " + v.getNodeId()
                                         + " for key '" + v.getKey() + "' with version "
                                         + v.getVersion() + ".");
                        innerStores.get(v.getNodeId()).put(v.getKey(), v.getVersioned());
                    } catch(VoldemortApplicationException e) {
                        if(logger.isDebugEnabled())
                            logger.debug("Read repair cancelled due to application level exception on node "
                                         + v.getNodeId()
                                         + " for key '"
                                         + v.getKey()
                                         + "' with version "
                                         + v.getVersion()
                                         + ": "
                                         + e.getMessage());
                    } catch(Exception e) {
                        logger.debug("Read repair failed: ", e);
                    }
                }
            }
        });
    }

    public String getName() {
        return this.name;
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        PutPipelineData pipelineData = new PutPipelineData();
        Pipeline pipeline = new Pipeline(Operation.PUT);

        Action configureNodes = new ConfigureNodes(pipelineData,
                                                   Event.CONFIGURED,
                                                   failureDetector,
                                                   storeDef.getRequiredWrites(),
                                                   routingStrategy,
                                                   key);
        Action performSerialPutRequests = new PerformSerialPutRequests(pipelineData,
                                                                       Event.COMPLETED,
                                                                       key,
                                                                       failureDetector,
                                                                       innerStores,
                                                                       storeDef.getRequiredWrites(),
                                                                       versioned,
                                                                       time,
                                                                       Event.MASTER_DETERMINED);
        Action performParallelPutRequests = new PerformParallelPutRequests(pipelineData,
                                                                           Event.NOP,
                                                                           key,
                                                                           nonblockingStores);
        Action acknowledgeResponse = new AcknowledgeResponse(pipelineData,
                                                             Event.COMPLETED,
                                                             failureDetector,
                                                             storeDef.getPreferredWrites(),
                                                             storeDef.getRequiredWrites(),
                                                             null);
        Action incrementClock = new IncrementClock(pipelineData, Event.STOPPED, versioned, time);

        Map<Event, Action> eventActions = new HashMap<Event, Action>();
        eventActions.put(Event.STARTED, configureNodes);
        eventActions.put(Event.CONFIGURED, performSerialPutRequests);
        eventActions.put(Event.MASTER_DETERMINED, performParallelPutRequests);
        eventActions.put(Event.RESPONSE_RECEIVED, acknowledgeResponse);
        eventActions.put(Event.COMPLETED, incrementClock);

        pipeline.setEventActions(eventActions);

        pipeline.addEvent(Event.STARTED);
        pipeline.processEvents(timeoutMs, TimeUnit.MILLISECONDS);

        pipelineData.get();
    }

    public void close() {
        this.executor.shutdown();
        try {
            if(!this.executor.awaitTermination(10, TimeUnit.SECONDS))
                this.executor.shutdownNow();
        } catch(InterruptedException e) {
            // okay, fine, playing nice didn't work
            this.executor.shutdownNow();
        }
        VoldemortException exception = null;
        for(Store<?, ?> client: innerStores.values()) {
            try {
                client.close();
            } catch(VoldemortException v) {
                exception = v;
            }
        }
        if(exception != null)
            throw exception;
    }

    public Map<Integer, Store<ByteArray, byte[]>> getInnerStores() {
        return this.innerStores;
    }

    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROUTING_STRATEGY:
                return this.routingStrategy;
            case READ_REPAIRER:
                return this.readRepairer;
            case VERSION_INCREMENTING:
                return true;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }

    public List<Version> getVersions(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData pipelineData = new BasicPipelineData();
        final Pipeline pipeline = new Pipeline(Operation.GET_VERSIONS);

        NonblockingStoreRequest storeRequest = new NonblockingStoreRequest() {

            public void request(Node node, NonblockingStore store) {
                final NonblockingStoreCallback callback = new PipelineEventNonblockingStoreCallback(pipeline,
                                                                                                    node,
                                                                                                    key);
                store.submitGetVersionsRequest(key, callback);
            }

        };

        Action configureNodes = new ConfigureNodes(pipelineData,
                                                   Event.CONFIGURED,
                                                   failureDetector,
                                                   storeDef.getRequiredReads(),
                                                   routingStrategy,
                                                   key);
        Action performRequests = new PerformParallelRequests(pipelineData,
                                                             Event.NOP,
                                                             storeDef.getPreferredReads(),
                                                             nonblockingStores,
                                                             storeRequest);
        Action acknowledgeResponse = new AcknowledgeResponse(pipelineData,
                                                             Event.COMPLETED,
                                                             failureDetector,
                                                             storeDef.getPreferredReads(),
                                                             storeDef.getRequiredReads(),
                                                             null);
        Action updateResults = new UpdateResults(pipelineData);

        Map<Event, Action> eventActions = new HashMap<Event, Action>();
        eventActions.put(Event.STARTED, configureNodes);
        eventActions.put(Event.CONFIGURED, performRequests);
        eventActions.put(Event.RESPONSE_RECEIVED, acknowledgeResponse);
        eventActions.put(Event.COMPLETED, updateResults);

        pipeline.setEventActions(eventActions);
        pipeline.addEvent(Event.STARTED);
        pipeline.processEvents(timeoutMs, TimeUnit.MILLISECONDS);

        List<Version> results = pipelineData.get();

        return results;
    }

    private void recordException(Node node, long startNs, UnreachableStoreException e) {
        failureDetector.recordException(node, (System.nanoTime() - startNs) / Time.NS_PER_MS, e);
    }

    private void recordSuccess(Node node, long startNs) {
        failureDetector.recordSuccess(node, (System.nanoTime() - startNs) / Time.NS_PER_MS);
    }

    private final class GetAllCallable implements Callable<GetAllResult> {

        private final Node node;
        private final Collection<ByteArray> nodeKeys;

        private GetAllCallable(Node node, Collection<ByteArray> nodeKeys) {
            this.node = node;
            this.nodeKeys = nodeKeys;
        }

        public GetAllResult call() {
            Map<ByteArray, List<Versioned<byte[]>>> retrieved = Collections.emptyMap();
            Throwable exception = null;
            List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();
            long startNs = System.nanoTime();
            try {
                retrieved = innerStores.get(node.getId()).getAll(nodeKeys);
                if(repairReads) {
                    for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: retrieved.entrySet())
                        fillRepairReadsValues(nodeValues, entry.getKey(), node, entry.getValue());
                    for(ByteArray nodeKey: nodeKeys) {
                        if(!retrieved.containsKey(nodeKey))
                            fillRepairReadsValues(nodeValues,
                                                  nodeKey,
                                                  node,
                                                  Collections.<Versioned<byte[]>> emptyList());
                    }
                }
                recordSuccess(node, startNs);
            } catch(UnreachableStoreException e) {
                exception = e;
                recordException(node, startNs, e);
            } catch(Throwable e) {
                if(e instanceof Error)
                    throw (Error) e;
                exception = e;
                logger.warn("Error in GET on node " + node.getId() + "(" + node.getHost() + ")", e);
            }
            return new GetAllResult(this, retrieved, nodeValues, exception);
        }
    }

    private static class GetAllResult {

        final GetAllCallable callable;
        final Map<ByteArray, List<Versioned<byte[]>>> retrieved;
        /* Note that this can never be an Error subclass */
        final Throwable exception;
        final List<NodeValue<ByteArray, byte[]>> nodeValues;

        private GetAllResult(GetAllCallable callable,
                             Map<ByteArray, List<Versioned<byte[]>>> retrieved,
                             List<NodeValue<ByteArray, byte[]>> nodeValues,
                             Throwable exception) {
            this.callable = callable;
            this.exception = exception;
            this.retrieved = retrieved;
            this.nodeValues = nodeValues;
        }
    }

}
