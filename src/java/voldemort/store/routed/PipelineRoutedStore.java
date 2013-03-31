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
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.common.VoldemortOpCode;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreRequest;
import voldemort.store.StoreUtils;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.AbstractConfigureNodes;
import voldemort.store.routed.action.ConfigureNodes;
import voldemort.store.routed.action.ConfigureNodesByZone;
import voldemort.store.routed.action.ConfigureNodesDefault;
import voldemort.store.routed.action.ConfigureNodesLocalHost;
import voldemort.store.routed.action.ConfigureNodesLocalHostByZone;
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

    private final Map<Integer, NonblockingStore> nonblockingStores;
    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores;
    private final Map<Integer, NonblockingStore> nonblockingSlopStores;
    private final HintedHandoffStrategy handoffStrategy;
    private Zone clientZone;
    private boolean zoneRoutingEnabled;
    private PipelineRoutedStats stats;
    private boolean jmxEnabled;
    private int jmxId;

    private enum ConfigureNodesType {
        DEFAULT,
        BYZONE,
        DEFAULT_LOCAL,
        BYZONE_LOCAL
    }

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
     * @param jmxEnabled is monitoring enabled
     * @param jmxId unique ID for the factory instance
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
                               TimeoutConfig timeoutConfig,
                               FailureDetector failureDetector,
                               boolean jmxEnabled,
                               int jmxId) {
        super(name,
              innerStores,
              cluster,
              storeDef,
              repairReads,
              timeoutConfig,
              failureDetector,
              SystemTime.INSTANCE);
        this.nonblockingSlopStores = nonblockingSlopStores;
        this.clientZone = cluster.getZoneById(clientZoneId);
        if(storeDef.getRoutingStrategyType().compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
            zoneRoutingEnabled = true;
        } else {
            zoneRoutingEnabled = false;
        }
        this.nonblockingStores = new ConcurrentHashMap<Integer, NonblockingStore>(nonblockingStores);
        this.slopStores = slopStores;
        if(storeDef.hasHintedHandoffStrategyType()) {
            HintedHandoffStrategyFactory factory = new HintedHandoffStrategyFactory(zoneRoutingEnabled,
                                                                                    clientZone.getId());
            this.handoffStrategy = factory.updateHintedHandoffStrategy(storeDef, cluster);
        } else {
            this.handoffStrategy = null;
        }

        this.jmxEnabled = jmxEnabled;
        this.jmxId = jmxId;
        if(this.jmxEnabled) {
            stats = new PipelineRoutedStats();
            JmxUtils.registerMbean(stats,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(stats.getClass()),
                                                             getName()
                                                                     + JmxUtils.getJmxId(this.jmxId)));
        }
    }

    private ConfigureNodesType obtainNodeConfigurationType(Integer zonesRequired) {

        if(zonesRequired != null) {
            if(routingStrategy.getType().equals(RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY)) {
                return ConfigureNodesType.BYZONE_LOCAL;
            } else {
                return ConfigureNodesType.BYZONE;
            }
        } else {
            if(routingStrategy.getType().equals(RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY)) {
                return ConfigureNodesType.DEFAULT_LOCAL;
            }
        }

        return ConfigureNodesType.DEFAULT;
    }

    private AbstractConfigureNodes<ByteArray, List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>> makeNodeConfigurationForGet(BasicPipelineData<List<Versioned<byte[]>>> pipelineData,
                                                                                                                                               ByteArray key) {
        switch(obtainNodeConfigurationType(pipelineData.getZonesRequired())) {
            case DEFAULT:
                return new ConfigureNodesDefault<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                      Event.CONFIGURED,
                                                                                                                      failureDetector,
                                                                                                                      storeDef.getRequiredReads(),
                                                                                                                      routingStrategy,
                                                                                                                      key);
            case BYZONE:
                return new ConfigureNodesByZone<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                     Event.CONFIGURED,
                                                                                                                     failureDetector,
                                                                                                                     storeDef.getRequiredReads(),
                                                                                                                     routingStrategy,
                                                                                                                     key,
                                                                                                                     clientZone);
            case DEFAULT_LOCAL:
                return new ConfigureNodesLocalHost<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                        Event.CONFIGURED,
                                                                                                                        failureDetector,
                                                                                                                        storeDef.getRequiredReads(),
                                                                                                                        routingStrategy,
                                                                                                                        key);
            case BYZONE_LOCAL:
                return new ConfigureNodesLocalHostByZone<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                              Event.CONFIGURED,
                                                                                                                              failureDetector,
                                                                                                                              storeDef.getRequiredReads(),
                                                                                                                              routingStrategy,
                                                                                                                              key,
                                                                                                                              clientZone);
            default:
                return null;
        }

    }

    @Override
    public List<Versioned<byte[]>> get(final ByteArray key, final byte[] transforms) {
        return get(key, transforms, timeoutConfig.getOperationTimeout(VoldemortOpCode.GET_OP_CODE));
    }

    public List<Versioned<byte[]>> get(final ByteArray key,
                                       final byte[] transforms,
                                       final long getOpTimeout) {
        StoreUtils.assertValidKey(key);

        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        BasicPipelineData<List<Versioned<byte[]>>> pipelineData = new BasicPipelineData<List<Versioned<byte[]>>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStats(stats);

        final Pipeline pipeline = new Pipeline(Operation.GET, getOpTimeout, TimeUnit.MILLISECONDS);
        boolean allowReadRepair = repairReads && transforms == null;

        StoreRequest<List<Versioned<byte[]>>> blockingStoreRequest = new StoreRequest<List<Versioned<byte[]>>>() {

            @Override
            public List<Versioned<byte[]>> request(Store<ByteArray, byte[], byte[]> store) {
                return store.get(key, transforms);
            }

        };

        // Get the correct type of configure nodes action depending on the store
        // requirements
        AbstractConfigureNodes<ByteArray, List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>> configureNodes = makeNodeConfigurationForGet(pipelineData,
                                                                                                                                                            key);

        pipeline.addEventAction(Event.STARTED, configureNodes);

        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelRequests<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                                 allowReadRepair ? Event.RESPONSES_RECEIVED
                                                                                                                                                : Event.COMPLETED,
                                                                                                                                 key,
                                                                                                                                 transforms,
                                                                                                                                 failureDetector,
                                                                                                                                 storeDef.getPreferredReads(),
                                                                                                                                 storeDef.getRequiredReads(),
                                                                                                                                 getOpTimeout,
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
                                                                                               getOpTimeout,
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

        if(logger.isDebugEnabled()) {
            logger.debug("Finished " + pipeline.getOperation().getSimpleName() + " for key "
                         + ByteUtils.toHexString(key.get()) + " keyRef: "
                         + System.identityHashCode(key) + "; started at " + startTimeMs + " took "
                         + (System.nanoTime() - startTimeNs) + " values: "
                         + formatNodeValuesFromGet(pipelineData.getResponses()));
        }

        return results;
    }

    private String formatNodeValuesFromGet(List<Response<ByteArray, List<Versioned<byte[]>>>> results) {
        // log all retrieved values
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(Response<ByteArray, List<Versioned<byte[]>>> r: results) {
            builder.append("(nodeId=" + r.getNode().getId() + ", key=" + r.getKey()
                           + ", retrieved= " + r.getValue() + "), ");
        }
        builder.append("}");

        return builder.toString();
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        return getAll(keys,
                      transforms,
                      timeoutConfig.getOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE));
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms,
                                                          long getAllOpTimeoutInMs)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        boolean allowReadRepair = repairReads && (transforms == null || transforms.size() == 0);

        GetAllPipelineData pipelineData = new GetAllPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStats(stats);

        Pipeline pipeline = new Pipeline(Operation.GET_ALL,
                                         getAllOpTimeoutInMs,
                                         TimeUnit.MILLISECONDS);
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
                                                                  getAllOpTimeoutInMs,
                                                                  nonblockingStores));
        pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
                                new PerformSerialGetAllRequests(pipelineData,
                                                                allowReadRepair ? Event.RESPONSES_RECEIVED
                                                                               : Event.COMPLETED,
                                                                keys,
                                                                failureDetector,
                                                                innerStores,
                                                                storeDef.getPreferredReads(),
                                                                storeDef.getRequiredReads(),
                                                                timeoutConfig.isPartialGetAllAllowed()));

        if(allowReadRepair)
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new GetAllReadRepair(pipelineData,
                                                         Event.COMPLETED,
                                                         storeDef.getPreferredReads(),
                                                         getAllOpTimeoutInMs,
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

        if(logger.isDebugEnabled()) {
            logger.debug("Finished " + pipeline.getOperation().getSimpleName() + "for keys "
                         + ByteArray.toHexStrings(keys) + " keyRef: "
                         + System.identityHashCode(keys) + "; started at " + startTimeMs + " took "
                         + (System.nanoTime() - startTimeNs) + " values: "
                         + formatNodeValuesFromGetAll(pipelineData.getResponses()));
        }

        return pipelineData.getResult();
    }

    private String formatNodeValuesFromGetAll(List<Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>>> list) {
        // log all retrieved values
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> r: list) {
            builder.append("(nodeId=" + r.getNode().getId() + ", keys="
                           + ByteArray.toHexStrings(r.getKey()) + ", retrieved= " + r.getValue()
                           + ")");
            builder.append(", ");
        }
        builder.append("}");

        return builder.toString();
    }

    @Override
    public List<Version> getVersions(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        BasicPipelineData<List<Version>> pipelineData = new BasicPipelineData<List<Version>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStats(stats);
        Pipeline pipeline = new Pipeline(Operation.GET_VERSIONS,
                                         timeoutConfig.getOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE),
                                         TimeUnit.MILLISECONDS);

        StoreRequest<List<Version>> blockingStoreRequest = new StoreRequest<List<Version>>() {

            @Override
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
                                                                                                             timeoutConfig.getOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE),
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
            logger.debug("Operation  " + pipeline.getOperation().getSimpleName() + " Key "
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

        if(logger.isDebugEnabled()) {
            logger.debug("Finished " + pipeline.getOperation().getSimpleName() + " for key "
                         + ByteUtils.toHexString(key.get()) + " keyRef: "
                         + System.identityHashCode(key) + "; started at " + startTimeMs + " took "
                         + (System.nanoTime() - startTimeNs) + " values: "
                         + formatNodeValuesFromGetVersions(pipelineData.getResponses()));
        }

        return results;
    }

    private <R> String formatNodeValuesFromGetVersions(List<Response<ByteArray, List<Version>>> results) {
        // log all retrieved values
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for(Response<ByteArray, List<Version>> r: results) {
            builder.append("(nodeId=" + r.getNode().getId() + ", key="
                           + ByteUtils.toHexString(r.getKey().get()) + ", retrieved= "
                           + r.getValue() + "), ");
        }
        builder.append("}");

        return builder.toString();
    }

    @Override
    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        return delete(key,
                      version,
                      timeoutConfig.getOperationTimeout(VoldemortOpCode.DELETE_OP_CODE));
    }

    protected boolean delete(final ByteArray key, final Version version, long deleteOpTimeout)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        BasicPipelineData<Boolean> pipelineData = new BasicPipelineData<Boolean>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStoreName(getName());
        pipelineData.setStats(stats);

        Pipeline pipeline = new Pipeline(Operation.DELETE, deleteOpTimeout, TimeUnit.MILLISECONDS);
        pipeline.setEnableHintedHandoff(isHintedHandoffEnabled());

        HintedHandoff hintedHandoff = null;

        if(isHintedHandoffEnabled())
            hintedHandoff = new HintedHandoff(failureDetector,
                                              slopStores,
                                              nonblockingSlopStores,
                                              handoffStrategy,
                                              pipelineData.getFailedNodes(),
                                              deleteOpTimeout);

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
                                                                                                       deleteOpTimeout,
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

        if(logger.isDebugEnabled()) {
            logger.debug("Finished " + pipeline.getOperation().getSimpleName() + " for key "
                         + ByteUtils.toHexString(key.get()) + " keyRef: "
                         + System.identityHashCode(key) + "; started at " + startTimeMs + " took "
                         + (System.nanoTime() - startTimeNs));
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

    private AbstractConfigureNodes<ByteArray, Void, PutPipelineData> makeNodeConfigurationForPut(PutPipelineData pipelineData,
                                                                                                 ByteArray key) {
        switch(obtainNodeConfigurationType(pipelineData.getZonesRequired())) {
            case DEFAULT:
                return new ConfigureNodesDefault<Void, PutPipelineData>(pipelineData,
                                                                        Event.CONFIGURED,
                                                                        failureDetector,
                                                                        storeDef.getRequiredWrites(),
                                                                        routingStrategy,
                                                                        key);
            case BYZONE:
                return new ConfigureNodesByZone<Void, PutPipelineData>(pipelineData,
                                                                       Event.CONFIGURED,
                                                                       failureDetector,
                                                                       storeDef.getRequiredWrites(),
                                                                       routingStrategy,
                                                                       key,
                                                                       clientZone);
            case DEFAULT_LOCAL:
                return new ConfigureNodesLocalHost<Void, PutPipelineData>(pipelineData,
                                                                          Event.CONFIGURED,
                                                                          failureDetector,
                                                                          storeDef.getRequiredWrites(),
                                                                          routingStrategy,
                                                                          key);
            case BYZONE_LOCAL:
                return new ConfigureNodesLocalHostByZone<Void, PutPipelineData>(pipelineData,
                                                                                Event.CONFIGURED,
                                                                                failureDetector,
                                                                                storeDef.getRequiredWrites(),
                                                                                routingStrategy,
                                                                                key,
                                                                                clientZone);
            default:
                return null;
        }

    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        put(key,
            versioned,
            transforms,
            timeoutConfig.getOperationTimeout(VoldemortOpCode.PUT_OP_CODE));
    }

    public void put(ByteArray key,
                    Versioned<byte[]> versioned,
                    byte[] transforms,
                    long putOpTimeoutInMs) throws VoldemortException {

        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        StoreUtils.assertValidKey(key);
        PutPipelineData pipelineData = new PutPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStartTimeNs(System.nanoTime());
        pipelineData.setStoreName(getName());
        pipelineData.setStats(stats);

        Pipeline pipeline = new Pipeline(Operation.PUT, putOpTimeoutInMs, TimeUnit.MILLISECONDS);
        pipeline.setEnableHintedHandoff(isHintedHandoffEnabled());

        HintedHandoff hintedHandoff = null;

        // Get the correct type of configure nodes action depending on the store
        // requirements
        AbstractConfigureNodes<ByteArray, Void, PutPipelineData> configureNodes = makeNodeConfigurationForPut(pipelineData,
                                                                                                              key);

        if(isHintedHandoffEnabled())
            hintedHandoff = new HintedHandoff(failureDetector,
                                              slopStores,
                                              nonblockingSlopStores,
                                              handoffStrategy,
                                              pipelineData.getFailedNodes(),
                                              putOpTimeoutInMs);

        pipeline.addEventAction(Event.STARTED, configureNodes);

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
                                                               putOpTimeoutInMs,
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

        if(logger.isDebugEnabled()) {
            logger.debug("Finished " + pipeline.getOperation().getSimpleName() + " for key "
                         + ByteUtils.toHexString(key.get()) + " keyRef: "
                         + System.identityHashCode(key) + "; started at " + startTimeMs + " took "
                         + (System.nanoTime() - startTimeNs) + " value: " + versioned.getValue()
                         + " (size: " + versioned.getValue().length + ")");
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

        if(this.jmxEnabled) {
            JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(stats.getClass()),
                                                               getName() + JmxUtils.getJmxId(jmxId)));
        }

        if(exception != null)
            throw exception;

        super.close();
    }

    @Override
    public List<Versioned<byte[]>> get(CompositeVoldemortRequest<ByteArray, byte[]> request)
            throws VoldemortException {
        return get(request.getKey(), null, request.getRoutingTimeoutInMs());
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(CompositeVoldemortRequest<ByteArray, byte[]> request)
            throws VoldemortException {
        return getAll(request.getIterableKeys(), null, request.getRoutingTimeoutInMs());
    }

    @Override
    public void put(CompositeVoldemortRequest<ByteArray, byte[]> request) throws VoldemortException {
        put(request.getKey(), request.getValue(), null, request.getRoutingTimeoutInMs());
    }

    @Override
    public boolean delete(CompositeVoldemortRequest<ByteArray, byte[]> request)
            throws VoldemortException {
        return delete(request.getKey(), request.getVersion(), request.getRoutingTimeoutInMs());
    }
}
