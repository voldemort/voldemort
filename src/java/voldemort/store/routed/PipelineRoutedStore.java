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
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreRequest;
import voldemort.store.StoreUtils;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.nonblockingstore.NonblockingStoreRequest;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.ConfigureNodes;
import voldemort.store.routed.action.GetAllConfigureNodes;
import voldemort.store.routed.action.GetAllReadRepair;
import voldemort.store.routed.action.IncrementClock;
import voldemort.store.routed.action.PerformDeleteHintedHandoff;
import voldemort.store.routed.action.PerformPutHintedHandoff;
import voldemort.store.routed.action.PerformParallelGetAllRequests;
import voldemort.store.routed.action.PerformParallelPutRequests;
import voldemort.store.routed.action.PerformParallelRequests;
import voldemort.store.routed.action.PerformSerialGetAllRequests;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.store.routed.action.PerformSerialRequests;
import voldemort.store.routed.action.PerformZoneSerialRequests;
import voldemort.store.routed.action.ReadRepair;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public class PipelineRoutedStore extends RoutedStore {

    private final Map<Integer, NonblockingStore> nonblockingStores;
    private final Map<Integer, Store<ByteArray, Slop>> slopStores;
    private final Cluster cluster;
    private Zone clientZone;
    private boolean zoneRoutingEnabled;

    /**
     * Create a PipelineRoutedStore
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
    public PipelineRoutedStore(String name,
                               Map<Integer, Store<ByteArray, byte[]>> innerStores,
                               Map<Integer, NonblockingStore> nonblockingStores,
                               Map<Integer, Store<ByteArray, Slop>> slopStores,
                               Cluster cluster,
                               StoreDefinition storeDef,
                               boolean repairReads,
                               int clientZoneId,
                               long timeoutMs,
                               FailureDetector failureDetector) {
        super(name,
              innerStores,
              cluster,
              storeDef,
              repairReads,
              timeoutMs,
              failureDetector,
              SystemTime.INSTANCE);

        this.clientZone = cluster.getZoneById(clientZoneId);
        if(storeDef.getRoutingStrategyType().compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
            zoneRoutingEnabled = true;
        } else {
            zoneRoutingEnabled = false;
        }

        this.nonblockingStores = new ConcurrentHashMap<Integer, NonblockingStore>(nonblockingStores);
        this.slopStores = slopStores;
        this.cluster = cluster;
    }

    public List<Versioned<byte[]>> get(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Versioned<byte[]>>> pipelineData = new BasicPipelineData<List<Versioned<byte[]>>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);

        final Pipeline pipeline = new Pipeline(Operation.GET, timeoutMs, TimeUnit.MILLISECONDS);

        NonblockingStoreRequest nonblockingStoreRequest = new NonblockingStoreRequest() {

            public void submit(Node node, NonblockingStore store, NonblockingStoreCallback callback) {
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
                                                                                                                        key,
                                                                                                                        clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                                 repairReads ? Event.RESPONSES_RECEIVED
                                                                                                                                            : Event.COMPLETED,
                                                                                                                                 key,
                                                                                                                                 failureDetector,
                                                                                                                                 storeDef.getPreferredReads(),
                                                                                                                                 storeDef.getRequiredReads(),
                                                                                                                                 timeoutMs,
                                                                                                                                 nonblockingStores,
                                                                                                                                 nonblockingStoreRequest,
                                                                                                                                 Event.INSUFFICIENT_SUCCESSES,
                                                                                                                                 Event.INSUFFICIENT_ZONES));
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

        if(zoneRoutingEnabled)
            pipeline.addEventAction(Event.INSUFFICIENT_ZONES,
                                    new PerformZoneSerialRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                                       repairReads ? Event.RESPONSES_RECEIVED
                                                                                                                                                  : Event.COMPLETED,
                                                                                                                                       key,
                                                                                                                                       failureDetector,
                                                                                                                                       innerStores,
                                                                                                                                       blockingStoreRequest));

        pipeline.addEvent(Event.STARTED);
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
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        Pipeline pipeline = new Pipeline(Operation.GET_ALL, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new GetAllConfigureNodes(pipelineData,
                                                         Event.CONFIGURED,
                                                         failureDetector,
                                                         storeDef.getPreferredReads(),
                                                         storeDef.getRequiredReads(),
                                                         routingStrategy,
                                                         keys,
                                                         clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelGetAllRequests(pipelineData,
                                                                  Event.INSUFFICIENT_SUCCESSES,
                                                                  failureDetector,
                                                                  timeoutMs,
                                                                  nonblockingStores));
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

        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        return pipelineData.getResult();
    }

    public List<Version> getVersions(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Version>> pipelineData = new BasicPipelineData<List<Version>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        final Pipeline pipeline = new Pipeline(Operation.GET_VERSIONS,
                                               timeoutMs,
                                               TimeUnit.MILLISECONDS);

        NonblockingStoreRequest storeRequest = new NonblockingStoreRequest() {

            public void submit(Node node, NonblockingStore store, NonblockingStoreCallback callback) {
                store.submitGetVersionsRequest(key, callback);
            }

        };

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                    Event.CONFIGURED,
                                                                                                    failureDetector,
                                                                                                    storeDef.getRequiredReads(),
                                                                                                    routingStrategy,
                                                                                                    key,
                                                                                                    clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                             Event.COMPLETED,
                                                                                                             key,
                                                                                                             failureDetector,
                                                                                                             storeDef.getPreferredReads(),
                                                                                                             storeDef.getRequiredReads(),
                                                                                                             timeoutMs,
                                                                                                             nonblockingStores,
                                                                                                             storeRequest,
                                                                                                             null,
                                                                                                             null));

        pipeline.addEvent(Event.STARTED);
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
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStoreName(name);
        final Pipeline pipeline = new Pipeline(Operation.DELETE, timeoutMs, TimeUnit.MILLISECONDS);
        pipeline.setEnableHintedHandoff(isHintedHandoffEnabled());

        NonblockingStoreRequest nonblockingDelete = new NonblockingStoreRequest() {

            public void submit(Node node, NonblockingStore store, NonblockingStoreCallback callback) {
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
                                                                                        key,
                                                                                        clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                                 isHintedHandoffEnabled()
                                                                                                 ? Event.RESPONSES_RECEIVED
                                                                                                 : Event.COMPLETED,
                                                                                                 key,
                                                                                                 failureDetector,
                                                                                                 storeDef.getPreferredWrites(),
                                                                                                 storeDef.getRequiredWrites(),
                                                                                                 timeoutMs,
                                                                                                 nonblockingStores,
                                                                                                 nonblockingDelete,
                                                                                                 Event.INSUFFICIENT_SUCCESSES,
                                                                                                 Event.INSUFFICIENT_ZONES));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialRequests<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                               isHintedHandoffEnabled()
                                                                                               ? Event.RESPONSES_RECEIVED
                                                                                               : Event.COMPLETED,
                                                                                               key,
                                                                                               failureDetector,
                                                                                               innerStores,
                                                                                               storeDef.getPreferredWrites(),
                                                                                               storeDef.getRequiredWrites(),
                                                                                               blockingDelete,
                                                                                               null));

        if(zoneRoutingEnabled)
            pipeline.addEventAction(Event.INSUFFICIENT_ZONES,
                                    new PerformZoneSerialRequests<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                                       isHintedHandoffEnabled()
                                                                                                       ? Event.RESPONSES_RECEIVED
                                                                                                       : Event.COMPLETED,
                                                                                                       key,
                                                                                                       failureDetector,
                                                                                                       innerStores,
                                                                                                       blockingDelete));

        if(isHintedHandoffEnabled()) {
            pipeline.addEventAction(Event.RESPONSES_RECEIVED, new PerformDeleteHintedHandoff(pipelineData,
                                                                                             Event.COMPLETED,
                                                                                             key,
                                                                                             version,
                                                                                             failureDetector,
                                                                                             slopStores,
                                                                                             cluster));
            pipeline.addEventAction(Event.ABORTED, new PerformDeleteHintedHandoff(pipelineData,
                                                                                  Event.ERROR,
                                                                                  key,
                                                                                  version,
                                                                                  failureDetector,
                                                                                  slopStores,
                                                                                  cluster));
        }
        
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        for(Response<ByteArray, Boolean> response: pipelineData.getResponses()) {
            if(response.getValue().booleanValue())
                return true;
        }

        return false;
    }

    public boolean isHintedHandoffEnabled() {
        return slopStores != null;
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        PutPipelineData pipelineData = new PutPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStartTimeNs(System.nanoTime());
        pipelineData.setStoreName(name);

        Pipeline pipeline = new Pipeline(Operation.PUT, timeoutMs, TimeUnit.MILLISECONDS);
        pipeline.setEnableHintedHandoff(isHintedHandoffEnabled());

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<Void, PutPipelineData>(pipelineData,
                                                                          Event.CONFIGURED,
                                                                          failureDetector,
                                                                          storeDef.getRequiredWrites(),
                                                                          routingStrategy,
                                                                          key,
                                                                          clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformSerialPutRequests(pipelineData,
                                                             isHintedHandoffEnabled()
                                                             ? Event.RESPONSES_RECEIVED
                                                             : Event.COMPLETED,
                                                             key,
                                                             failureDetector,
                                                             innerStores,
                                                             storeDef.getRequiredWrites(),
                                                             versioned,
                                                             time,
                                                             Event.MASTER_DETERMINED));
        pipeline.addEventAction(Event.MASTER_DETERMINED,
                                new PerformParallelPutRequests(pipelineData,
                                                               Event.RESPONSES_RECEIVED,
                                                               key,
                                                               failureDetector,
                                                               storeDef.getPreferredWrites(),
                                                               storeDef.getRequiredWrites(),
                                                               timeoutMs,
                                                               nonblockingStores,
                                                               slopStores,
                                                               new ArrayList<Node>(cluster.getNodes())));
        if(isHintedHandoffEnabled()) {
            pipeline.addEventAction(Event.ABORTED, new PerformPutHintedHandoff(pipelineData,
                                                                               Event.ERROR,
                                                                               key,
                                                                               versioned,
                                                                               failureDetector,
                                                                               slopStores,
                                                                               cluster,
                                                                               time));
            pipeline.addEventAction(Event.RESPONSES_RECEIVED, new PerformPutHintedHandoff(pipelineData,
                                                                                          Event.HANDOFF_FINISHED,
                                                                                          key,
                                                                                          versioned,
                                                                                          failureDetector,
                                                                                          slopStores,
                                                                                          cluster,
                                                                                          time));
            pipeline.addEventAction(Event.HANDOFF_FINISHED, new IncrementClock(pipelineData,
                                                                               Event.COMPLETED,
                                                                               versioned,
                                                                               time));
        } else
            pipeline.addEventAction(Event.RESPONSES_RECEIVED, new IncrementClock(pipelineData,
                                                                                 Event.COMPLETED,
                                                                                 versioned,
                                                                                 time));

        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
    }
    
    @Override
    public void close() {
        VoldemortException exception = null;

        for(NonblockingStore store: nonblockingStores.values()) {
            try {
                store.close();
            } catch(VoldemortException e) {
                exception = e;
            }
        }

        if(exception != null)
            throw exception;

        super.close();
    }

}
