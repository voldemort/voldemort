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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.client.ZoneAffinity;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.cluster.failuredetector.MutableStoreConnectionVerifier;
import voldemort.cluster.failuredetector.ThresholdFailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
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
    private static final int NUM_THREADS = 3;
    private static final int NUM_NODES_TOTAL = 3;

    private final String STORE_NAME = "test";
    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ExecutorService routedStoreThreadPool;
    private RoutedStoreFactory routedStoreFactory;
    private RoutedStore store;
    private RoutingStrategy strategy;

    private static final long ROUTING_TIMEOUT_MS = 100;
    private static final long SLEEP_STORE_TIME = 200;
    private static final long HINT_DELAY_TIME_MS = 300;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores = new ConcurrentHashMap<Integer, Store<ByteArray, Slop, byte[]>>();
    private final List<StreamingSlopPusherJob> slopPusherJobs = Lists.newLinkedList();

    private final Logger logger = Logger.getLogger(getClass());

    private enum FAILURE_MODE {
        FAIL_FIRST_REPLICA_NODE,
        FAIL_ALL_REPLICAS
    }

    static class ReplicaFactor {
        int replicationFactor;
        int preferredReads;
        int requiredReads;
        int preferredWrites;
        int requiredWrites;
        
        
        public ReplicaFactor(int replicationFactor,
                             int preferredReads,
                             int requiredReads,
                             int preferredWrites,
                             int requiredWrites) {
            this.replicationFactor = replicationFactor;
            this.preferredReads = preferredReads;
            this.requiredReads = requiredReads;
            this.preferredWrites = preferredWrites;
            this.requiredWrites = requiredWrites;
        }
    }

    public static ReplicaFactor get322Replica() {
        return new ReplicaFactor(3, 2, 2, 2, 2);
    }

    public static ReplicaFactor get211Replica() {
        return new ReplicaFactor(2, 1, 1, 1, 1);
    }


    private StoreDefinition getStoreDef(String storeName,
                                        ReplicaFactor replicaFactor,
                                        String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setReplicationFactor(replicaFactor.replicationFactor)
                                           .setPreferredReads(replicaFactor.preferredReads)
                                           .setRequiredReads(replicaFactor.requiredReads)
                                           .setPreferredWrites(replicaFactor.preferredWrites)
                                           .setRequiredWrites(replicaFactor.requiredWrites)
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
        failureDetectorConfig.setConnectionVerifier(MutableStoreConnectionVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }

    /**
     * A wrapper for the actual customSetup method with the default failure mode
     * as FAIL_FIRST_REPLICA_NODE
     * 
     * @param key The ByteArray representation of the key
     * 
     * @throws Exception
     */
    public List<Integer> customSetup(ByteArray key,
                                     ReplicaFactor replicaFactor,
                                     long hintDelayTimeMs) throws Exception {
        return customSetup(key,
                           FAILURE_MODE.FAIL_FIRST_REPLICA_NODE,
                           replicaFactor,
                           SLEEP_STORE_TIME,
                           hintDelayTimeMs);
    }

    /**
     * Setup a cluster with 3 nodes, with the following characteristics:
     * 
     * If FAILURE_MODE is FAIL_FIRST_REPLICA_NODE set the first replica store to
     * a sleepy force failing store
     * 
     * If FAILURE_MODE is FAIL_ALL_REPLICAS: set all replicas to sleepy force
     * failing store
     * 
     * Pseudo master : Standard In-memory store (wrapped by Logging store)
     * 
     * In memory slop stores
     * 
     * @param key The ByteArray representation of the key
     * @param failureMode The Failure mode for the replicas
     * 
     * @throws Exception
     */
    public List<Integer> customSetup(ByteArray key,
                                     FAILURE_MODE failureMode,
                                     ReplicaFactor replicaFactor,
                                     long sleepTime,
                                     long hintDelayTimeMs)
            throws Exception {

        cluster = getThreeNodeCluster();
        storeDef = getStoreDef(STORE_NAME, replicaFactor, RoutingStrategyType.CONSISTENT_STRATEGY);

        strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

        InMemoryStorageEngine<ByteArray, byte[], byte[]> inMemoryStorageEngine = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(STORE_NAME);
        LoggingStore<ByteArray, byte[], byte[]> loggingStore = new LoggingStore<ByteArray, byte[], byte[]>(inMemoryStorageEngine);

        VoldemortException e = new UnreachableStoreException("Node down");
        ForceFailStore<ByteArray, byte[], byte[]> failureStore = new ForceFailStore<ByteArray, byte[], byte[]>(loggingStore,
                                                                                                               e);
        SleepyStore<ByteArray, byte[], byte[]> sleepyFailureStore = new SleepyStore<ByteArray, byte[], byte[]>(sleepTime,
                                                                                                               failureStore);
        failureStore.setFail(true);

        List<Integer> failingNodeIdList = Lists.newArrayList();
        List<Node> replicaList = strategy.routeRequest(key.get());

        switch(failureMode) {
            case FAIL_FIRST_REPLICA_NODE:
                failingNodeIdList.add(replicaList.get(1).getId());
                break;

            case FAIL_ALL_REPLICAS:
                for(int nodeId = 1; nodeId < replicaList.size(); nodeId++) {
                    failingNodeIdList.add(nodeId);
                }
                break;
        }

        subStores.clear();
        for(int i = 0; i < NUM_NODES_TOTAL; i++) {
            if(failingNodeIdList.contains(i)) {
                subStores.put(i, sleepyFailureStore);
            } else {
                subStores.put(i, loggingStore);
            }
        }
        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
        routedStoreFactory = new RoutedStoreFactory(routedStoreThreadPool);

        Map<Integer, NonblockingStore> nonblockingSlopStores = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();

            SlopStorageEngine slopStorageEngine = new SlopStorageEngine(new InMemoryStorageEngine<ByteArray, byte[], byte[]>(SLOP_STORE_NAME),
                                                                        cluster);
            StorageEngine<ByteArray, Slop, byte[]> storageEngine = slopStorageEngine.asSlopStore();
            nonblockingSlopStores.put(nodeId,
                                      routedStoreFactory.toNonblockingStore(slopStorageEngine));
            slopStores.put(nodeId, storageEngine);
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
                                                  failureDetector,
                                                  hintDelayTimeMs);
        return failingNodeIdList;
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null) {
            failureDetector.destroy();
        }

        for(Store subStore: subStores.values()) {
            subStore.close();
        }

        for(Store subStore: slopStores.values()) {
            subStore.close();
        }

        if(routedStoreThreadPool != null) {
            routedStoreThreadPool.shutdown();
            routedStoreThreadPool.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    /**
     * Function to create a set of slop keys for the FAILED_NODE_ID for PUT
     * operation
     * 
     * @param failedKeys Set of keys that the put should've failed for
     * @return Set of slop keys based on the failed keys and the FAILED_NODE_ID
     */
    private Set<ByteArray> makeSlopKeys(ByteArray failedKey, List<Integer> failingNodeIdList) {
        Set<ByteArray> slopKeys = Sets.newHashSet();

        for(int failingNodeId: failingNodeIdList) {
            byte[] opCode = new byte[] { Slop.Operation.PUT.getOpCode() };
            byte[] spacer = new byte[] { (byte) 0 };
            byte[] storeNameBytes = ByteUtils.getBytes(STORE_NAME, "UTF-8");
            byte[] nodeIdBytes = new byte[ByteUtils.SIZE_OF_INT];
            ByteUtils.writeInt(nodeIdBytes, failingNodeId, 0);
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
     * A function to fetch all the registered slops
     * 
     * @param slopKeys Keys for the registered slops in the slop store
     * @return Set of all the registered Slops
     */
    public Set<Slop> getAllSlops(Iterable<ByteArray> slopKeys) {
        Set<Slop> registeredSlops = Sets.newHashSet();
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                registeredSlops.add(slop);
                logger.info(slop);
            }
        }
        return registeredSlops;
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
    public void testSlopOnDelayedFailingAsyncPut_2_1_1() throws Exception {

        String key = "testSlopOnDelayedFailingAsyncPut_2_1_1";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());
        List<Integer> failingNodeIdList = null;


        try {
            failingNodeIdList = customSetup(keyByteArray, get211Replica(), HINT_DELAY_TIME_MS);
        } catch(Exception e) {
            logger.info(e.getMessage());
            fail("Error in setup.");
        }

        this.store.put(keyByteArray, versionedVal, null);

        Thread.sleep(HINT_DELAY_TIME_MS + 100);
        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(keyByteArray, failingNodeIdList);
        Set<Slop> registeredSlops = getAllSlops(slopKeys);

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
    public void testSlopOnDelayedFailingAsyncPut_3_2_2() throws Exception {

        String key = "testSlopOnDelayedFailingAsyncPut_3_2_2";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());
        List<Integer> failingNodeIdList = null;

        try {
            failingNodeIdList = customSetup(keyByteArray, get322Replica(), HINT_DELAY_TIME_MS);
        } catch(Exception e) {
            logger.info(e.getMessage());
            fail("Error in setup.");
        }

        this.store.put(keyByteArray, versionedVal, null);

        Thread.sleep(HINT_DELAY_TIME_MS + 100);

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(keyByteArray, failingNodeIdList);
        Set<Slop> registeredSlops = getAllSlops(slopKeys);

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

        String key = "testSlopViaSerialHint_2_1_1";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());
        List<Integer> failingNodeIdList = null;

        try {
            failingNodeIdList = customSetup(keyByteArray, get211Replica(), 0);
        } catch(Exception e) {
            logger.info(e.getMessage());
            fail("Error in setup.");
        }

        this.store.put(keyByteArray, versionedVal, null);

        // Give enough time for the serial hint to work.
        try {
            logger.info("Sleeping for 5 seconds to wait for the serial hint to finish");
            Thread.sleep(1000);
        } catch(Exception e) {}

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(keyByteArray, failingNodeIdList);
        Set<Slop> registeredSlops = getAllSlops(slopKeys);

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

        String key = "testSlopViaSerialHint_3_2_2";
        String val = "xyz";
        Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        ByteArray keyByteArray = new ByteArray(key.getBytes());
        List<Integer> failingNodeIdList = null;

        try {
            failingNodeIdList = customSetup(keyByteArray, get322Replica(), 0);
        } catch(Exception e) {
            logger.info(e.getMessage());
            fail("Error in setup.");
        }

        this.store.put(keyByteArray, versionedVal, null);

        // Give enough time for the serial hint to work.
        try {
            logger.info("Sleeping for 5 seconds to wait for the serial hint to finish");
            Thread.sleep(1000);
        } catch(Exception e) {}

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(keyByteArray, failingNodeIdList);
        Set<Slop> registeredSlops = getAllSlops(slopKeys);

        if(registeredSlops.size() == 0) {
            fail("Should have seen some slops. But could not find any.");
        } else if(registeredSlops.size() != 1) {
            fail("Number of slops registered != 1");
        }
    }

    /**
     * Test to do a put with a 3-2-2 config such that both the replica nodes do
     * not respond at all. This test is to make sure that the main thread
     * returns with an error and that no slops are registered.
     */
    @Test
    public void testNoSlopsOnAllReplicaFailures() throws Exception {

        String key = "testNoSlopsOnAllReplicaFailures";
        String val = "xyz";
        final Versioned<byte[]> versionedVal = new Versioned<byte[]>(val.getBytes());
        final ByteArray keyByteArray = new ByteArray(key.getBytes());
        List<Integer> failingNodeIdList = null;

        long sleepTime = ROUTING_TIMEOUT_MS + 100;
        failingNodeIdList = customSetup(keyByteArray,
                                        FAILURE_MODE.FAIL_ALL_REPLICAS,
                                        get322Replica(),
                                        sleepTime,
                                        0);

        PerformAsyncPut asyncPutThread = new PerformAsyncPut(this.store, keyByteArray, versionedVal);
        Executors.newFixedThreadPool(1).submit(asyncPutThread);

        // Sleep for the routing timeout with some headroom
        try {
            Thread.sleep(sleepTime + 100);
        } catch(Exception e) {
            fail("Unknown error while doing a put: " + e);
        }

        // Check the slop stores
        Set<ByteArray> failedKeys = Sets.newHashSet();
        failedKeys.add(keyByteArray);
        Set<ByteArray> slopKeys = makeSlopKeys(keyByteArray, failingNodeIdList);
        Set<Slop> registeredSlops = getAllSlops(slopKeys);

        if(registeredSlops.size() != 0) {
            fail("Should not have seen any slops.");
        }
    }

    /**
     * A runnable class to do a Voldemort Put operation. This becomes important
     * in the scenario that the put operation might hang / deadlock.
     * 
     */
    private class PerformAsyncPut implements Runnable {

        private Versioned<byte[]> versionedVal = null;
        private ByteArray keyByteArray = null;
        private RoutedStore asyncPutStore = null;
        private boolean isDone = false;
        private Exception ex = null;

        public PerformAsyncPut(RoutedStore asyncPutStore,
                               ByteArray keyByteArray,
                               Versioned<byte[]> versionedVal) {
            this.asyncPutStore = asyncPutStore;
            this.keyByteArray = keyByteArray;
            this.versionedVal = versionedVal;
        }

        @Override
        public void run() {
            try {
                asyncPutStore.put(keyByteArray, versionedVal, null);
                fail("A put with required writes 2 should've failed for this setup");
            } catch(VoldemortException ve) {
                // This is expected. Nothing to do.
                logger.info("Error occurred as expected : " + ve.getMessage());
            } catch(Exception e) {
                ex = e;
            } finally {
                markAsDone(true);
            }
        }

        @SuppressWarnings("unused")
        public boolean isDone() throws Exception {
            if(ex != null) {
                throw ex;
            }
            return isDone;
        }

        public void markAsDone(boolean isDone) {
            this.isDone = isDone;
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

        private long hintDelayTimeMs = 0;
        public DelayedPutPipelineRoutedStore(Map<Integer, Store<ByteArray, byte[], byte[]>> innerStores,
                                             Map<Integer, NonblockingStore> nonblockingStores,
                                             Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                                             Map<Integer, NonblockingStore> nonblockingSlopStores,
                                             Cluster cluster,
                                             StoreDefinition storeDef,
                                             FailureDetector failureDetector,
                                             long hintDelayTimeMs) {
            super(innerStores,
                  nonblockingStores,
                  slopStores,
                  nonblockingSlopStores,
                  cluster,
                  storeDef,
                  failureDetector,
                  true,
                  new TimeoutConfig(ROUTING_TIMEOUT_MS),
                  Zone.UNSET_ZONE_ID,
                  false,
                  new PipelineRoutedStats(storeDef.getName()),
                  new ZoneAffinity());
            this.hintDelayTimeMs = hintDelayTimeMs;

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

            long putOpTimeoutInMs = ROUTING_TIMEOUT_MS;
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
                                                    hintDelayTimeMs));

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
