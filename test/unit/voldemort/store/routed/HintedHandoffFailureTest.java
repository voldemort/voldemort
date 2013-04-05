/*
 * Copyright 2013 LinkedIn, Inc
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

import static org.junit.Assert.fail;
import static voldemort.VoldemortTestConstants.getThreeNodeCluster;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.cluster.failuredetector.MutableStoreVerifier;
import voldemort.cluster.failuredetector.ThresholdFailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.ForceFailStore;
import voldemort.store.SleepyStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.AbstractAction;
import voldemort.store.routed.action.AbstractConfigureNodes;
import voldemort.store.routed.action.ConfigureNodesDefault;
import voldemort.store.routed.action.IncrementClock;
import voldemort.store.routed.action.PerformParallelPutRequests;
import voldemort.store.routed.action.PerformPutHintedHandoff;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Test class to ensure that slops are registered for different asynchronous put
 * failures.
 */
public class HintedHandoffFailureTest {

    private final static String SLOP_STORE_NAME = "slop";
    private static int REPLICATION_FACTOR = 2;
    private static int P_READS = 1;
    private static int R_READS = 1;
    private static int P_WRITES = 1;
    private static int R_WRITES = 1;
    private static final int NUM_THREADS = 3;
    private static final int NUM_NODES_TOTAL = 3;
    private static int FAILED_NODE_ID = 0;

    private final String STORE_NAME = "test";
    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ExecutorService routedStoreThreadPool;
    private RoutedStoreFactory routedStoreFactory;
    private RoutedStore store;
    private RoutingStrategy strategy;

    private final static long routingTimeoutInMs = 1000;
    private final static long sleepBeforeFailingInMs = 2000;
    private static long delayBeforeHintedHandoff = 3000;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores = new ConcurrentHashMap<Integer, Store<ByteArray, Slop, byte[]>>();
    private final List<StreamingSlopPusherJob> slopPusherJobs = Lists.newLinkedList();

    private final Logger logger = Logger.getLogger(getClass());

    private StoreDefinition getStoreDef(String storeName,
                                        int replicationFactor,
                                        int preads,
                                        int rreads,
                                        int pwrites,
                                        int rwrites,
                                        String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setReplicationFactor(replicationFactor)
                                           .setPreferredReads(preads)
                                           .setRequiredReads(rreads)
                                           .setPreferredWrites(pwrites)
                                           .setRequiredWrites(rwrites)
                                           .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                           .build();
    }

    private void setFailureDetector(Map<Integer, Store<ByteArray, byte[], byte[]>> subStores)
            throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        // Using Threshold FD
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig();
        failureDetectorConfig.setImplementationClassName(ThresholdFailureDetector.class.getName());
        failureDetectorConfig.setCluster(cluster);
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }

    /**
     * Setup a cluster with 3 nodes, with the following characteristics:
     * 
     * - 1st replica node: Sleepy force failing store (will throw an exception
     * after a delay)
     * 
     * - Pseudo master and other replicas: Standard In-memory store (wrapped by
     * Logging store)
     * 
     * - In memory slop stores
     * 
     * @param key The ByteArray representation of the key
     * 
     * @throws Exception
     */
    public void customSetup(ByteArray key) throws Exception {

        cluster = getThreeNodeCluster();
        storeDef = getStoreDef(STORE_NAME,
                               REPLICATION_FACTOR,
                               P_READS,
                               R_READS,
                               P_WRITES,
                               R_WRITES,
                               RoutingStrategyType.CONSISTENT_STRATEGY);

        strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

        InMemoryStorageEngine<ByteArray, byte[], byte[]> inMemoryStorageEngine = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(STORE_NAME);
        LoggingStore<ByteArray, byte[], byte[]> loggingStore = new LoggingStore<ByteArray, byte[], byte[]>(inMemoryStorageEngine);

        VoldemortException e = new UnreachableStoreException("Node down");
        ForceFailStore<ByteArray, byte[], byte[]> failureStore = new ForceFailStore<ByteArray, byte[], byte[]>(loggingStore,
                                                                                                               e);
        SleepyStore<ByteArray, byte[], byte[]> sleepyFailureStore = new SleepyStore<ByteArray, byte[], byte[]>(sleepBeforeFailingInMs,
                                                                                                               failureStore);
        failureStore.setFail(true);

        // Get the first replica node for the given key
        // This will act as the sleepy failing node
        Node failingNode = strategy.routeRequest(key.get()).get(1);
        FAILED_NODE_ID = failingNode.getId();

        subStores.clear();
        for(int i = 0; i < NUM_NODES_TOTAL; i++) {
            if(i == FAILED_NODE_ID) {
                subStores.put(i, sleepyFailureStore);
            } else {
                subStores.put(i, loggingStore);
            }
        }
        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
        routedStoreFactory = new RoutedStoreFactory(true,
                                                    routedStoreThreadPool,
                                                    new TimeoutConfig(routingTimeoutInMs, false));

        Map<Integer, NonblockingStore> nonblockingSlopStores = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            StoreRepository storeRepo = new StoreRepository();
            storeRepo.addLocalStore(subStores.get(nodeId));

            for(int i = 0; i < NUM_NODES_TOTAL; i++) {
                storeRepo.addNodeStore(i, subStores.get(i));
            }

            SlopStorageEngine slopStorageEngine = new SlopStorageEngine(new InMemoryStorageEngine<ByteArray, byte[], byte[]>(SLOP_STORE_NAME),
                                                                        cluster);
            StorageEngine<ByteArray, Slop, byte[]> storageEngine = slopStorageEngine.asSlopStore();
            storeRepo.setSlopStore(slopStorageEngine);
            nonblockingSlopStores.put(nodeId,
                                      routedStoreFactory.toNonblockingStore(slopStorageEngine));
            slopStores.put(nodeId, storageEngine);

            MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster,
                                                                              Lists.newArrayList(storeDef));
            StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(storeRepo,
                                                                       metadataStore,
                                                                       failureDetector,
                                                                       ServerTestUtils.createServerConfigWithDefs(false,
                                                                                                                  nodeId,
                                                                                                                  TestUtils.createTempDir()
                                                                                                                           .getAbsolutePath(),
                                                                                                                  cluster,
                                                                                                                  Lists.newArrayList(storeDef),
                                                                                                                  new Properties()),
                                                                       new ScanPermitWrapper(1));
            slopPusherJobs.add(pusher);
        }

        Map<Integer, NonblockingStore> nonblockingStores = Maps.newHashMap();
        for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> entry: subStores.entrySet())
            nonblockingStores.put(entry.getKey(),
                                  routedStoreFactory.toNonblockingStore(entry.getValue()));

        store = new DelayedPutPipelineRoutedStore(subStores,
                                                  nonblockingStores,
                                                  slopStores,
                                                  nonblockingSlopStores,
                                                  cluster,
                                                  storeDef,
                                                  failureDetector);
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null) {
            failureDetector.destroy();
        }

        if(routedStoreThreadPool != null) {
            routedStoreThreadPool.shutdown();
        }
    }

    /**
     * Function to create a set of slop keys for the FAILED_NODE_ID for PUT
     * operation
     * 
     * @param failedKeys Set of keys that the put should've failed for
     * @return Set of slop keys based on the failed keys and the FAILED_NODE_ID
     */
    private Set<ByteArray> makeSlopKeys(Set<ByteArray> failedKeys) {
        Set<ByteArray> slopKeys = Sets.newHashSet();

        for(ByteArray failedKey: failedKeys) {
            byte[] opCode = new byte[] { Slop.Operation.PUT.getOpCode() };
            byte[] spacer = new byte[] { (byte) 0 };
            byte[] storeNameBytes = ByteUtils.getBytes(STORE_NAME, "UTF-8");
            byte[] nodeIdBytes = new byte[ByteUtils.SIZE_OF_INT];
            ByteUtils.writeInt(nodeIdBytes, FAILED_NODE_ID, 0);
            ByteArray slopKey = new ByteArray(ByteUtils.cat(opCode,
                                                            spacer,
                                                            storeNameBytes,
                                                            spacer,
                                                            nodeIdBytes,
                                                            spacer,
                                                            failedKey.get()));
            slopKeys.add(slopKey);
        }
        return slopKeys;
    }

    /**
     * Test to ensure that when an asynchronous put completes (with a failure)
     * after PerformParallelPut has finished processing the responses and before
     * the hinted handoff actually begins, a slop is still registered for the
     * same.
     * 
     * This is for the 2-1-1 configuration.
     */
    @Test
    public void testSlopOnDelayedFailingAsyncPut_2_1_1() {

        String key = "a";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());

        // Set the correct replication config
        REPLICATION_FACTOR = 2;
        P_READS = 1;
        R_READS = 1;
        P_WRITES = 1;
        R_WRITES = 1;

        try {
            customSetup(keyByteArray);
        } catch(Exception e) {
            fail("Error in setup.");
        }

        this.store.put(keyByteArray, versionedVal, null);

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys);

        Set<Slop> registeredSlops = Sets.newHashSet();
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                registeredSlops.add(slop);
                logger.info(slop);
            }
        }

        if(registeredSlops.size() == 0) {
            fail("Should have seen some slops. But could not find any.");
        } else if(registeredSlops.size() != 1) {
            fail("Number of slops registered != 1");
        }
    }

    /**
     * Test to ensure that when an asynchronous put completes (with a failure)
     * after PerformParallelPut has finished processing the responses and before
     * the hinted handoff actually begins, a slop is still registered for the
     * same.
     * 
     * This is for the 3-2-2 configuration.
     */
    @Test
    public void testSlopOnDelayedFailingAsyncPut_3_2_2() {

        String key = "a";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());

        // Set the correct replication config
        REPLICATION_FACTOR = 3;
        P_READS = 2;
        R_READS = 2;
        P_WRITES = 2;
        R_WRITES = 2;

        try {
            customSetup(keyByteArray);
        } catch(Exception e) {
            fail("Error in setup.");
        }

        this.store.put(keyByteArray, versionedVal, null);

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys);

        Set<Slop> registeredSlops = Sets.newHashSet();
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                registeredSlops.add(slop);
                logger.info(slop);
            }
        }

        if(registeredSlops.size() == 0) {
            fail("Should have seen some slops. But could not find any.");
        } else if(registeredSlops.size() != 1) {
            fail("Number of slops registered != 1");
        }
    }

    /**
     * Test to ensure that when an asynchronous put completes (with a failure)
     * after the pipeline completes, a slop is still registered (via a serial
     * hint).
     * 
     * This is for the 2-1-1 configuration
     */
    @Test
    public void testSlopViaSerialHint_2_1_1() {

        String key = "a";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());

        // Set the correct replication config
        REPLICATION_FACTOR = 2;
        P_READS = 1;
        R_READS = 1;
        P_WRITES = 1;
        R_WRITES = 1;

        try {
            customSetup(keyByteArray);
        } catch(Exception e) {
            fail("Error in setup.");
        }

        // We remove the delay in the pipeline so that the pipeline will finish
        // before the failing async put returns. At this point it should do a
        // serial hint.
        delayBeforeHintedHandoff = 0;

        this.store.put(keyByteArray, versionedVal, null);

        // Give enough time for the serial hint to work.
        try {
            logger.info("Sleeping for 5 seconds to wait for the serial hint to finish");
            Thread.sleep(5000);
        } catch(Exception e) {}

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys);

        Set<Slop> registeredSlops = Sets.newHashSet();
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                registeredSlops.add(slop);
                logger.info(slop);
            }
        }

        if(registeredSlops.size() == 0) {
            fail("Should have seen some slops. But could not find any.");
        } else if(registeredSlops.size() != 1) {
            fail("Number of slops registered != 1");
        }
    }

    /**
     * Test to ensure that when an asynchronous put completes (with a failure)
     * after the pipeline completes, a slop is still registered (via a serial
     * hint).
     * 
     * This is for the 3-2-2 configuration
     */
    @Test
    public void testSlopViaSerialHint_3_2_2() {

        String key = "a";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());

        // Set the correct replication config
        REPLICATION_FACTOR = 3;
        P_READS = 2;
        R_READS = 2;
        P_WRITES = 2;
        R_WRITES = 2;

        try {
            customSetup(keyByteArray);
        } catch(Exception e) {
            fail("Error in setup.");
        }

        // We remove the delay in the pipeline so that the pipeline will finish
        // before the failing async put returns. At this point it should do a
        // serial hint.
        delayBeforeHintedHandoff = 0;

        this.store.put(keyByteArray, versionedVal, null);

        // Give enough time for the serial hint to work.
        try {
            logger.info("Sleeping for 5 seconds to wait for the serial hint to finish");
            Thread.sleep(5000);
        } catch(Exception e) {}

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys);

        Set<Slop> registeredSlops = Sets.newHashSet();
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                registeredSlops.add(slop);
                logger.info(slop);
            }
        }

        if(registeredSlops.size() == 0) {
            fail("Should have seen some slops. But could not find any.");
        } else if(registeredSlops.size() != 1) {
            fail("Number of slops registered != 1");
        }
    }

    /**
     * An action within a pipeline which sleeps for the specified time duration.
     * 
     */
    private class DelayAction extends AbstractAction<ByteArray, Void, PutPipelineData> {

        private long sleepTimeInMs;

        protected DelayAction(PutPipelineData pipelineData, Event completeEvent, long sleepTimeInMs) {
            super(pipelineData, completeEvent);
            this.sleepTimeInMs = sleepTimeInMs;
        }

        @Override
        public void execute(Pipeline pipeline) {
            try {
                logger.info("Delayed pipeline action now sleeping for : " + sleepTimeInMs);
                Thread.sleep(sleepTimeInMs);
                logger.info("Now moving on to doing actual hinted handoff. Current time = "
                            + new Date(System.currentTimeMillis()));
            } catch(Exception e) {}
            pipeline.addEvent(completeEvent);
        }

    }

    /**
     * A custom implementation of the PipelineRoutedStore with an extra (sleep)
     * action between PerformParallelPutRequests stage and
     * PerformPutHintedHandoff stage
     * 
     */
    private class DelayedPutPipelineRoutedStore extends PipelineRoutedStore {

        public DelayedPutPipelineRoutedStore(Map<Integer, Store<ByteArray, byte[], byte[]>> innerStores,
                                             Map<Integer, NonblockingStore> nonblockingStores,
                                             Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                                             Map<Integer, NonblockingStore> nonblockingSlopStores,
                                             Cluster cluster,
                                             StoreDefinition storeDef,
                                             FailureDetector failureDetector) {
            super(storeDef.getName(),
                  innerStores,
                  nonblockingStores,
                  slopStores,
                  nonblockingSlopStores,
                  cluster,
                  storeDef,
                  false,
                  Zone.DEFAULT_ZONE_ID,
                  new TimeoutConfig(routingTimeoutInMs, false),
                  failureDetector,
                  false,
                  0);

        }

        /**
         * A custom put implementation. Here we add an extra action to the
         * pipeline to sleep before doing the actual handoff
         */
        @Override
        public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
                throws VoldemortException {
            PutPipelineData pipelineData = new PutPipelineData();
            pipelineData.setZonesRequired(null);
            pipelineData.setStartTimeNs(System.nanoTime());
            pipelineData.setStoreName(getName());

            long putOpTimeoutInMs = routingTimeoutInMs;
            Pipeline pipeline = new Pipeline(Operation.PUT, putOpTimeoutInMs, TimeUnit.MILLISECONDS);
            pipeline.setEnableHintedHandoff(true);
            HintedHandoff hintedHandoff = null;

            AbstractConfigureNodes<ByteArray, Void, PutPipelineData> configureNodes = new ConfigureNodesDefault<Void, PutPipelineData>(pipelineData,
                                                                                                                                       Event.CONFIGURED,
                                                                                                                                       failureDetector,
                                                                                                                                       storeDef.getRequiredWrites(),
                                                                                                                                       routingStrategy,
                                                                                                                                       key);

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

            pipeline.addEventAction(Event.ABORTED, new PerformPutHintedHandoff(pipelineData,
                                                                               Event.ERROR,
                                                                               key,
                                                                               versioned,
                                                                               transforms,
                                                                               hintedHandoff,
                                                                               time));

            // We use INSUFFICIENT_SUCCESSES as the next event (since there is
            // no specific delay event)
            pipeline.addEventAction(Event.RESPONSES_RECEIVED,
                                    new DelayAction(pipelineData,
                                                    Event.INSUFFICIENT_SUCCESSES,
                                                    delayBeforeHintedHandoff));

            pipeline.addEventAction(Event.INSUFFICIENT_SUCCESSES,
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

            pipeline.addEvent(Event.STARTED);
            if(logger.isDebugEnabled()) {
                logger.debug("Operation " + pipeline.getOperation().getSimpleName() + " Key "
                             + ByteUtils.toHexString(key.get()));
            }
            try {
                pipeline.execute();
            } catch(VoldemortException e) {
                throw e;
            }

            if(pipelineData.getFatalError() != null)
                throw pipelineData.getFatalError();
        }
    }

}
