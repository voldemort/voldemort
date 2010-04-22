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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreRequest;
import voldemort.store.StoreUtils;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.nonblockingstore.NonblockingStoreRequest;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.AcknowledgeResponse;
import voldemort.store.routed.action.ConfigureNodes;
import voldemort.store.routed.action.GetAllAcknowledgeResponse;
import voldemort.store.routed.action.GetAllConfigureNodes;
import voldemort.store.routed.action.GetAllReadRepair;
import voldemort.store.routed.action.IncrementClock;
import voldemort.store.routed.action.PerformParallelGetAllRequests;
import voldemort.store.routed.action.PerformParallelPutRequests;
import voldemort.store.routed.action.PerformParallelRequests;
import voldemort.store.routed.action.PerformSerialGetAllRequests;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.store.routed.action.PerformSerialRequests;
import voldemort.store.routed.action.ReadRepair;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

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

    public List<Versioned<byte[]>> get(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Versioned<byte[]>>> pipelineData = new BasicPipelineData<List<Versioned<byte[]>>>();
        final Pipeline pipeline = new Pipeline(Operation.GET, timeoutMs, TimeUnit.MILLISECONDS);

        NonblockingStoreRequest nonblockingStoreRequest = new NonblockingStoreRequest() {

            public void submit(Node node, NonblockingStore store) {
                NonblockingStoreCallback callback = new BasicResponseCallback<ByteArray>(pipeline,
                                                                                         node,
                                                                                         key);
                store.submitGetRequest(key, callback);
            }

        };

        StoreRequest<List<Versioned<byte[]>>> blockingStoreRequest = new StoreRequest<List<Versioned<byte[]>>>() {

            public List<Versioned<byte[]>> request(Store<ByteArray, byte[]> store) {
                return store.get(key);
            }

        };

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                        Event.CONFIGURED,
                                                                                                                        failureDetector,
                                                                                                                        storeDef.getRequiredReads(),
                                                                                                                        routingStrategy,
                                                                                                                        key));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                                 Event.COMPLETED,
                                                                                                                                 storeDef.getPreferredReads(),
                                                                                                                                 nonblockingStores,
                                                                                                                                 nonblockingStoreRequest));
        pipeline.addEventAction(Event.RESPONSE_RECEIVED,
                                new AcknowledgeResponse<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                             repairReads ? Event.RESPONSES_RECEIVED
                                                                                                                                        : Event.COMPLETED,
                                                                                                                             failureDetector,
                                                                                                                             storeDef.getPreferredReads(),
                                                                                                                             storeDef.getRequiredReads(),
                                                                                                                             Event.INSUFFICIENT_SUCCESSES));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                               repairReads ? Event.RESPONSES_RECEIVED
                                                                                                                                          : Event.COMPLETED,
                                                                                                                               key,
                                                                                                                               failureDetector,
                                                                                                                               innerStores,
                                                                                                                               storeDef.getPreferredReads(),
                                                                                                                               storeDef.getRequiredReads(),
                                                                                                                               blockingStoreRequest,
                                                                                                                               null));

        if(repairReads)
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new ReadRepair<BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                               Event.COMPLETED,
                                                                                               storeDef.getPreferredReads(),
                                                                                               nonblockingStores,
                                                                                               readRepairer));

        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();

        for(Response<ByteArray, List<Versioned<byte[]>>> response: pipelineData.getResponses()) {
            List<Versioned<byte[]>> value = response.getValue();

            if(value != null)
                results.addAll(value);
        }

        return results;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        GetAllPipelineData pipelineData = new GetAllPipelineData();
        Pipeline pipeline = new Pipeline(Operation.GET_ALL, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new GetAllConfigureNodes(pipelineData,
                                                         Event.CONFIGURED,
                                                         failureDetector,
                                                         storeDef.getPreferredReads(),
                                                         storeDef.getRequiredReads(),
                                                         routingStrategy,
                                                         keys));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelGetAllRequests(pipelineData,
                                                                  null,
                                                                  storeDef.getPreferredReads(),
                                                                  nonblockingStores));
        pipeline.addEventAction(Event.RESPONSE_RECEIVED,
                                new GetAllAcknowledgeResponse(pipelineData,
                                                              Event.INSUFFICIENT_SUCCESSES,
                                                              failureDetector));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialGetAllRequests(pipelineData,
                                                                repairReads ? Event.RESPONSES_RECEIVED
                                                                           : Event.COMPLETED,
                                                                keys,
                                                                failureDetector,
                                                                innerStores,
                                                                storeDef.getPreferredReads(),
                                                                storeDef.getRequiredReads()));

        if(repairReads)
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new GetAllReadRepair(pipelineData,
                                                         Event.COMPLETED,
                                                         storeDef.getPreferredReads(),
                                                         nonblockingStores,
                                                         readRepairer));

        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        return pipelineData.getResult();
    }

    public List<Version> getVersions(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Version>> pipelineData = new BasicPipelineData<List<Version>>();
        final Pipeline pipeline = new Pipeline(Operation.GET_VERSIONS,
                                               timeoutMs,
                                               TimeUnit.MILLISECONDS);

        NonblockingStoreRequest storeRequest = new NonblockingStoreRequest() {

            public void submit(Node node, NonblockingStore store) {
                NonblockingStoreCallback callback = new BasicResponseCallback<ByteArray>(pipeline,
                                                                                         node,
                                                                                         key);
                store.submitGetVersionsRequest(key, callback);
            }

        };

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                    Event.CONFIGURED,
                                                                                                    failureDetector,
                                                                                                    storeDef.getRequiredReads(),
                                                                                                    routingStrategy,
                                                                                                    key));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                             Event.NOP,
                                                                                                             storeDef.getPreferredReads(),
                                                                                                             nonblockingStores,
                                                                                                             storeRequest));
        pipeline.addEventAction(Event.RESPONSE_RECEIVED,
                                new AcknowledgeResponse<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                         Event.COMPLETED,
                                                                                                         failureDetector,
                                                                                                         storeDef.getPreferredReads(),
                                                                                                         storeDef.getRequiredReads(),
                                                                                                         null));

        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        List<Version> results = new ArrayList<Version>();

        for(Response<ByteArray, List<Version>> response: pipelineData.getResponses())
            results.addAll(response.getValue());

        return results;
    }

    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<Boolean> pipelineData = new BasicPipelineData<Boolean>();
        final Pipeline pipeline = new Pipeline(Operation.DELETE, timeoutMs, TimeUnit.MILLISECONDS);

        NonblockingStoreRequest nonblockingDelete = new NonblockingStoreRequest() {

            public void submit(Node node, NonblockingStore store) {
                NonblockingStoreCallback callback = new BasicResponseCallback<ByteArray>(pipeline,
                                                                                         node,
                                                                                         key);
                store.submitDeleteRequest(key, version, callback);
            }

        };

        StoreRequest<Boolean> blockingDelete = new StoreRequest<Boolean>() {

            public Boolean request(Store<ByteArray, byte[]> store) {
                return store.delete(key, version);
            }

        };

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                        Event.CONFIGURED,
                                                                                        failureDetector,
                                                                                        storeDef.getRequiredWrites(),
                                                                                        routingStrategy,
                                                                                        key));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                                 Event.NOP,
                                                                                                 storeDef.getPreferredWrites(),
                                                                                                 nonblockingStores,
                                                                                                 nonblockingDelete));
        pipeline.addEventAction(Event.RESPONSE_RECEIVED,
                                new AcknowledgeResponse<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                             Event.COMPLETED,
                                                                                             failureDetector,
                                                                                             storeDef.getPreferredWrites(),
                                                                                             storeDef.getRequiredWrites(),
                                                                                             Event.INSUFFICIENT_SUCCESSES));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialRequests<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                               Event.COMPLETED,
                                                                                               key,
                                                                                               failureDetector,
                                                                                               innerStores,
                                                                                               storeDef.getPreferredWrites(),
                                                                                               storeDef.getRequiredWrites(),
                                                                                               blockingDelete,
                                                                                               null));

        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        for(Response<ByteArray, Boolean> response: pipelineData.getResponses()) {
            if(response.getValue().booleanValue())
                return true;
        }

        return false;
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        PutPipelineData pipelineData = new PutPipelineData();
        Pipeline pipeline = new Pipeline(Operation.PUT, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<Void, PutPipelineData>(pipelineData,
                                                                          Event.CONFIGURED,
                                                                          failureDetector,
                                                                          storeDef.getRequiredWrites(),
                                                                          routingStrategy,
                                                                          key));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformSerialPutRequests(pipelineData,
                                                             Event.COMPLETED,
                                                             key,
                                                             failureDetector,
                                                             innerStores,
                                                             storeDef.getRequiredWrites(),
                                                             versioned,
                                                             time,
                                                             Event.MASTER_DETERMINED));
        pipeline.addEventAction(Event.MASTER_DETERMINED,
                                new PerformParallelPutRequests(pipelineData,
                                                               Event.NOP,
                                                               key,
                                                               nonblockingStores));
        pipeline.addEventAction(Event.RESPONSE_RECEIVED,
                                new AcknowledgeResponse<Void, PutPipelineData>(pipelineData,
                                                                               Event.RESPONSES_RECEIVED,
                                                                               failureDetector,
                                                                               storeDef.getPreferredWrites(),
                                                                               storeDef.getRequiredWrites(),
                                                                               null));
        pipeline.addEventAction(Event.RESPONSES_RECEIVED, new IncrementClock(pipelineData,
                                                                             Event.COMPLETED,
                                                                             versioned,
                                                                             time));

        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
    }

    public String getName() {
        return this.name;
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

    public void updateRoutingStrategy(RoutingStrategy routingStrategy) {
        logger.info("Updating routing strategy for RoutedStore:" + getName());
        this.routingStrategy = routingStrategy;
    }

    public Map<Integer, Store<ByteArray, byte[]>> getInnerStores() {
        return this.innerStores;
    }

}
