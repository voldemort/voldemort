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
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreRequest;
import voldemort.store.StoreUtils;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.ConfigureNodes;
import voldemort.store.routed.action.GetAllConfigureNodes;
import voldemort.store.routed.action.GetAllReadRepair;
import voldemort.store.routed.action.IncrementClock;
import voldemort.store.routed.action.PerformDeleteHintedHandoff;
import voldemort.store.routed.action.PerformParallelDeleteRequests;
import voldemort.store.routed.action.PerformParallelGetAllRequests;
import voldemort.store.routed.action.PerformParallelPutRequests;
import voldemort.store.routed.action.PerformParallelRequests;
import voldemort.store.routed.action.PerformPutHintedHandoff;
import voldemort.store.routed.action.PerformSerialGetAllRequests;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.store.routed.action.PerformSerialRequests;
import voldemort.store.routed.action.PerformZoneSerialRequests;
import voldemort.store.routed.action.ReadRepair;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.store.slop.strategy.HintedHandoffStrategy;
import voldemort.store.slop.strategy.HintedHandoffStrategyFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.JmxUtils;
import voldemort.utils.SystemTime;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public class PipelineRoutedStore extends RoutedStore {

    private static AtomicInteger jmxIdCounter = new AtomicInteger(0);

    private final Map<Integer, NonblockingStore> nonblockingStores;
    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores;
    private final Map<Integer, NonblockingStore> nonblockingSlopStores;
    private final HintedHandoffStrategy handoffStrategy;
    private Zone clientZone;
    private boolean zoneRoutingEnabled;
    private PipelineRoutedStats stats;
    private final int jmxId;

    /**
     * Create a PipelineRoutedStore
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param nonblockingStores
     * @param slopStores The stores for hints
     * @param cluster Cluster definition
     * @param storeDef Store definition
     * @param repairReads Is read repair enabled?
     * @param clientZoneId Zone the client is in
     * @param timeoutMs Routing timeout
     * @param failureDetector Failure detector object
     */
    public PipelineRoutedStore(String name,
                               Map<Integer, Store<ByteArray, byte[], byte[]>> innerStores,
                               Map<Integer, NonblockingStore> nonblockingStores,
                               Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                               Map<Integer, NonblockingStore> nonblockingSlopStores,
                               Cluster cluster,
                               StoreDefinition storeDef,
                               boolean repairReads,
                               int clientZoneId,
                               long timeoutMs,
                               FailureDetector failureDetector,
                               boolean jmxEnabled) {
        super(name,
              innerStores,
              cluster,
              storeDef,
              repairReads,
              timeoutMs,
              failureDetector,
              SystemTime.INSTANCE);
        this.nonblockingSlopStores = nonblockingSlopStores;
        this.clientZone = cluster.getZoneById(clientZoneId);
        if(storeDef.getRoutingStrategyType().compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
            zoneRoutingEnabled = true;
        } else {
            zoneRoutingEnabled = false;
        }
        this.jmxId = jmxIdCounter.getAndIncrement();
        this.nonblockingStores = new ConcurrentHashMap<Integer, NonblockingStore>(nonblockingStores);
        this.slopStores = slopStores;
        if(storeDef.hasHintedHandoffStrategyType()) {
            HintedHandoffStrategyFactory factory = new HintedHandoffStrategyFactory(zoneRoutingEnabled,
                                                                                    clientZone.getId());
            this.handoffStrategy = factory.updateHintedHandoffStrategy(storeDef, cluster);
        } else {
            this.handoffStrategy = null;
        }

        if(jmxEnabled) {
            stats = new PipelineRoutedStats();
            JmxUtils.registerMbean(stats,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(stats.getClass()),
                                                             getName() + jmxId()));
        }
    }

    public List<Versioned<byte[]>> get(final ByteArray key, final byte[] transforms) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Versioned<byte[]>>> pipelineData = new BasicPipelineData<List<Versioned<byte[]>>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStats(stats);

        final Pipeline pipeline = new Pipeline(Operation.GET, timeoutMs, TimeUnit.MILLISECONDS);
        boolean allowReadRepair = repairReads && transforms == null;

        StoreRequest<List<Versioned<byte[]>>> blockingStoreRequest = new StoreRequest<List<Versioned<byte[]>>>() {

            public List<Versioned<byte[]>> request(Store<ByteArray, byte[], byte[]> store) {
                return store.get(key, transforms);
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
                                                                                                                                 allowReadRepair ? Event.RESPONSES_RECEIVED
                                                                                                                                                : Event.COMPLETED,
                                                                                                                                 key,
                                                                                                                                 transforms,
                                                                                                                                 failureDetector,
                                                                                                                                 storeDef.getPreferredReads(),
                                                                                                                                 storeDef.getRequiredReads(),
                                                                                                                                 timeoutMs,
                                                                                                                                 nonblockingStores,
                                                                                                                                 Event.INSUFFICIENT_SUCCESSES,
                                                                                                                                 Event.INSUFFICIENT_ZONES));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                               allowReadRepair ? Event.RESPONSES_RECEIVED
                                                                                                                                              : Event.COMPLETED,
                                                                                                                               key,
                                                                                                                               failureDetector,
                                                                                                                               innerStores,
                                                                                                                               storeDef.getPreferredReads(),
                                                                                                                               storeDef.getRequiredReads(),
                                                                                                                               blockingStoreRequest,
                                                                                                                               null));

        if(allowReadRepair)
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new ReadRepair<BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                               Event.COMPLETED,
                                                                                               storeDef.getPreferredReads(),
                                                                                               timeoutMs,
                                                                                               nonblockingStores,
                                                                                               readRepairer));

        if(zoneRoutingEnabled)
            pipeline.addEventAction(Event.INSUFFICIENT_ZONES,
                                    new PerformZoneSerialRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                                       allowReadRepair ? Event.RESPONSES_RECEIVED
                                                                                                                                                      : Event.COMPLETED,
                                                                                                                                       key,
                                                                                                                                       failureDetector,
                                                                                                                                       innerStores,
                                                                                                                                       blockingStoreRequest));

        pipeline.addEvent(Event.STARTED);

        if(logger.isDebugEnabled()) {
            logger.debug("Operation " + pipeline.getOperation().getSimpleName() + " Key "
                         + ByteUtils.toHexString(key.get()));
        }

        try {
            pipeline.execute();
        } catch(VoldemortException e) {
            stats.reportException(e);
            throw e;
        }

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

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        boolean allowReadRepair = repairReads && (transforms == null || transforms.size() == 0);

        GetAllPipelineData pipelineData = new GetAllPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStats(stats);

        Pipeline pipeline = new Pipeline(Operation.GET_ALL, timeoutMs, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED,
                                new GetAllConfigureNodes(pipelineData,
                                                         Event.CONFIGURED,
                                                         failureDetector,
                                                         storeDef.getPreferredReads(),
                                                         storeDef.getRequiredReads(),
                                                         routingStrategy,
                                                         keys,
                                                         transforms,
                                                         clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelGetAllRequests(pipelineData,
                                                                  Event.INSUFFICIENT_SUCCESSES,
                                                                  failureDetector,
                                                                  timeoutMs,
                                                                  nonblockingStores));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialGetAllRequests(pipelineData,
                                                                allowReadRepair ? Event.RESPONSES_RECEIVED
                                                                               : Event.COMPLETED,
                                                                keys,
                                                                failureDetector,
                                                                innerStores,
                                                                storeDef.getPreferredReads(),
                                                                storeDef.getRequiredReads()));

        if(allowReadRepair)
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new GetAllReadRepair(pipelineData,
                                                         Event.COMPLETED,
                                                         storeDef.getPreferredReads(),
                                                         timeoutMs,
                                                         nonblockingStores,
                                                         readRepairer));

        pipeline.addEvent(Event.STARTED);

        if(logger.isDebugEnabled()) {
            StringBuilder keyStr = new StringBuilder();
            for(ByteArray key: keys) {
                keyStr.append(ByteUtils.toHexString(key.get()) + ",");
            }
            logger.debug("Operation " + pipeline.getOperation().getSimpleName() + " Keys "
                         + keyStr.toString());
        }
        try {
            pipeline.execute();
        } catch(VoldemortException e) {
            stats.reportException(e);
            throw e;
        }

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
        pipelineData.setStats(stats);
        Pipeline pipeline = new Pipeline(Operation.GET_VERSIONS, timeoutMs, TimeUnit.MILLISECONDS);

        StoreRequest<List<Version>> blockingStoreRequest = new StoreRequest<List<Version>>() {

            public List<Version> request(Store<ByteArray, byte[], byte[]> store) {
                return store.getVersions(key);
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
                                                                                                             null,
                                                                                                             failureDetector,
                                                                                                             storeDef.getPreferredReads(),
                                                                                                             storeDef.getRequiredReads(),
                                                                                                             timeoutMs,
                                                                                                             nonblockingStores,
                                                                                                             Event.INSUFFICIENT_SUCCESSES,
                                                                                                             Event.INSUFFICIENT_ZONES));

        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialRequests<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                           Event.COMPLETED,
                                                                                                           key,
                                                                                                           failureDetector,
                                                                                                           innerStores,
                                                                                                           storeDef.getPreferredReads(),
                                                                                                           storeDef.getRequiredReads(),
                                                                                                           blockingStoreRequest,
                                                                                                           null));

        if(zoneRoutingEnabled)
            pipeline.addEventAction(Event.INSUFFICIENT_ZONES,
                                    new PerformZoneSerialRequests<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                                   Event.COMPLETED,
                                                                                                                   key,
                                                                                                                   failureDetector,
                                                                                                                   innerStores,
                                                                                                                   blockingStoreRequest));

        pipeline.addEvent(Event.STARTED);
        if(logger.isDebugEnabled()) {
            logger.debug("Operation  " + pipeline.getOperation().getSimpleName() + "Key "
                         + ByteUtils.toHexString(key.get()));
        }
        try {
            pipeline.execute();
        } catch(VoldemortException e) {
            stats.reportException(e);
            throw e;
        }

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
        pipelineData.setStats(stats);

        Pipeline pipeline = new Pipeline(Operation.DELETE, timeoutMs, TimeUnit.MILLISECONDS);
        pipeline.setEnableHintedHandoff(isHintedHandoffEnabled());

        HintedHandoff hintedHandoff = null;

        if(isHintedHandoffEnabled())
            hintedHandoff = new HintedHandoff(failureDetector,
                                              slopStores,
                                              nonblockingSlopStores,
                                              handoffStrategy,
                                              pipelineData.getFailedNodes(),
                                              timeoutMs);

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                        Event.CONFIGURED,
                                                                                        failureDetector,
                                                                                        storeDef.getRequiredWrites(),
                                                                                        routingStrategy,
                                                                                        key,
                                                                                        clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelDeleteRequests<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                                       isHintedHandoffEnabled() ? Event.RESPONSES_RECEIVED
                                                                                                                               : Event.COMPLETED,
                                                                                                       key,
                                                                                                       failureDetector,
                                                                                                       storeDef.getPreferredWrites(),
                                                                                                       storeDef.getRequiredWrites(),
                                                                                                       timeoutMs,
                                                                                                       nonblockingStores,
                                                                                                       hintedHandoff,
                                                                                                       version));

        if(isHintedHandoffEnabled()) {
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new PerformDeleteHintedHandoff(pipelineData,
                                                                   Event.COMPLETED,
                                                                   key,
                                                                   version,
                                                                   hintedHandoff));
            pipeline.addEventAction(Event.ABORTED, new PerformDeleteHintedHandoff(pipelineData,
                                                                                  Event.ERROR,
                                                                                  key,
                                                                                  version,
                                                                                  hintedHandoff));

        }

        pipeline.addEvent(Event.STARTED);
        if(logger.isDebugEnabled()) {
            logger.debug("Operation " + pipeline.getOperation().getSimpleName() + " Key "
                         + ByteUtils.toHexString(key.get()));
        }
        try {
            pipeline.execute();
        } catch(VoldemortException e) {
            stats.reportException(e);
            throw e;
        }

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

    public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PutPipelineData pipelineData = new PutPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStartTimeNs(System.nanoTime());
        pipelineData.setStoreName(name);
        pipelineData.setStats(stats);

        Pipeline pipeline = new Pipeline(Operation.PUT, timeoutMs, TimeUnit.MILLISECONDS);
        pipeline.setEnableHintedHandoff(isHintedHandoffEnabled());

        HintedHandoff hintedHandoff = null;

        if(isHintedHandoffEnabled())
            hintedHandoff = new HintedHandoff(failureDetector,
                                              slopStores,
                                              nonblockingSlopStores,
                                              handoffStrategy,
                                              pipelineData.getFailedNodes(),
                                              timeoutMs);

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
                                                             isHintedHandoffEnabled() ? Event.RESPONSES_RECEIVED
                                                                                     : Event.COMPLETED,
                                                             key,
                                                             transforms,
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
                                                               transforms,
                                                               failureDetector,
                                                               storeDef.getPreferredWrites(),
                                                               storeDef.getRequiredWrites(),
                                                               timeoutMs,
                                                               nonblockingStores,
                                                               hintedHandoff));
        if(isHintedHandoffEnabled()) {
            pipeline.addEventAction(Event.ABORTED, new PerformPutHintedHandoff(pipelineData,
                                                                               Event.ERROR,
                                                                               key,
                                                                               versioned,
                                                                               transforms,
                                                                               hintedHandoff,
                                                                               time));
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new PerformPutHintedHandoff(pipelineData,
                                                                Event.HANDOFF_FINISHED,
                                                                key,
                                                                versioned,
                                                                transforms,
                                                                hintedHandoff,
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
        if(logger.isDebugEnabled()) {
            logger.debug("Operation " + pipeline.getOperation().getSimpleName() + " Key "
                         + ByteUtils.toHexString(key.get()));
        }
        try {
            pipeline.execute();
        } catch(VoldemortException e) {
            stats.reportException(e);
            throw e;
        }

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

    /* Give a unique id to avoid jmx clashes */
    private String jmxId() {
        return jmxId == 0 ? "" : Integer.toString(jmxId);
    }
}
