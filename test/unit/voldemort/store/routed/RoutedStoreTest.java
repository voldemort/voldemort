/*
 * Copyright 2008-2013 LinkedIn, Inc
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

import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.FailureDetectorTestUtils.recordSuccess;
import static voldemort.TestUtils.getClock;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;
import static voldemort.cluster.failuredetector.MutableStoreVerifier.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.common.VoldemortOpCode;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.FailingReadsStore;
import voldemort.store.FailingStore;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.Tracked;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Basic tests for RoutedStore
 * 
 */
public class RoutedStoreTest extends AbstractByteArrayStoreTest {

    public static final int BANNAGE_PERIOD = 1000;
    public static final int SLEEPY_TIME = 200;
    public static final int OPERATION_TIMEOUT = 60;

    private Cluster cluster;
    private StoreDefinition storeDef;
    private final ByteArray aKey = TestUtils.toByteArray("jay");
    private final byte[] aValue = "kreps".getBytes();
    private final byte[] aTransform = "transform".getBytes();
    private FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;

    public RoutedStoreTest() {}

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        cluster = getNineNodeCluster();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

        if(failureDetector != null)
            failureDetector.destroy();

        if(routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();
    }

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() throws Exception {
        return new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(getStore(cluster,
                                                                                   cluster.getNumberOfNodes(),
                                                                                   cluster.getNumberOfNodes(),
                                                                                   4,
                                                                                   0),
                                                                          new VectorClockInconsistencyResolver<byte[]>());
    }

    private RoutedStoreFactory createFactory() {
        return new RoutedStoreFactory(this.routedStoreThreadPool);
    }

    private RoutedStoreConfig createConfig(TimeoutConfig timeoutConfig) {
        return new RoutedStoreConfig().setTimeoutConfig(timeoutConfig);
    }

    private RoutedStoreConfig createConfig(long timeout) {
        return new RoutedStoreConfig().setTimeoutConfig(new TimeoutConfig(timeout));
    }

    private RoutedStore getStore(Cluster cluster, int reads, int writes, int threads, int failing)
            throws Exception {
        return getStore(cluster,
                        reads,
                        writes,
                        threads,
                        failing,
                        0,
                        RoutingStrategyType.CONSISTENT_STRATEGY,
                        new VoldemortException());
    }

    private RoutedStore getStore(Cluster cluster,
                                 int reads,
                                 int writes,
                                 int threads,
                                 int failing,
                                 int sleepy,
                                 String strategy,
                                 VoldemortException e) throws Exception {
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        int count = 0;
        for(Node n: cluster.getNodes()) {
            if(count >= cluster.getNumberOfNodes())
                throw new IllegalArgumentException(failing + " failing nodes, " + sleepy
                                                   + " sleepy nodes, but only "
                                                   + cluster.getNumberOfNodes()
                                                   + " nodes in the cluster.");

            Store<ByteArray, byte[], byte[]> subStore = null;

            if(count < failing)
                subStore = new FailingStore<ByteArray, byte[], byte[]>("test", e);
            else if(count < failing + sleepy)
                subStore = new SleepyStore<ByteArray, byte[], byte[]>(Long.MAX_VALUE,
                                                                      new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            else
                subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            subStores.put(n.getId(), subStore);

            count += 1;
        }

        setFailureDetector(subStores);
        this.storeDef = ServerTestUtils.getStoreDef("test",
                                                    reads + writes,
                                                    reads,
                                                    reads,
                                                    writes,
                                                    writes,
                                                    strategy);
        routedStoreThreadPool = Executors.newFixedThreadPool(threads);

        RoutedStoreFactory routedStoreFactory = createFactory();

        return routedStoreFactory.create(cluster,
                                         storeDef,
                                         subStores,
                                         failureDetector,
                                         createConfig(BANNAGE_PERIOD));
    }

    public Store<ByteArray, byte[], byte[]> getZonedStore() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();
        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        return new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(getStore(cluster,
                                                                                   cluster.getNumberOfNodes(),
                                                                                   cluster.getNumberOfNodes(),
                                                                                   cluster.getNumberOfZones() - 1,
                                                                                   cluster.getNumberOfZones() - 1,
                                                                                   4,
                                                                                   zoneReplicationFactor),
                                                                          new VectorClockInconsistencyResolver<byte[]>());
    }

    private RoutedStore getStore(Cluster cluster,
                                 int reads,
                                 int writes,
                                 int zonereads,
                                 int zonewrites,
                                 int threads,
                                 HashMap<Integer, Integer> zoneReplicationFactor) throws Exception {
        return getStore(cluster,
                        reads,
                        writes,
                        zonereads,
                        zonewrites,
                        threads,
                        null,
                        null,
                        zoneReplicationFactor,
                        RoutingStrategyType.ZONE_STRATEGY,
                        0,
                        BANNAGE_PERIOD,
                        null);

    }

    private RoutedStore getStore(Cluster cluster,
                                 int reads,
                                 int writes,
                                 int zonereads,
                                 int zonewrites,
                                 int threads,
                                 Set<Integer> failing,
                                 Set<Integer> sleepy,
                                 HashMap<Integer, Integer> zoneReplicationFactor,
                                 String strategy,
                                 long sleepMs,
                                 long timeOutMs,
                                 VoldemortException e) throws Exception {
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        for(Node n: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> subStore = null;

            if(failing != null && failing.contains(n.getId()))
                subStore = new FailingStore<ByteArray, byte[], byte[]>("test", e);
            else if(sleepy != null && sleepy.contains(n.getId()))
                subStore = new SleepyStore<ByteArray, byte[], byte[]>(sleepMs,
                                                                      new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            else
                subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            subStores.put(n.getId(), subStore);
        }

        setFailureDetector(subStores);
        this.storeDef = ServerTestUtils.getStoreDef("test",
                                                    reads,
                                                    reads,
                                                    writes,
                                                    writes,
                                                    zonereads,
                                                    zonewrites,
                                                    zoneReplicationFactor,
                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                    strategy);
        routedStoreThreadPool = Executors.newFixedThreadPool(threads);
        RoutedStoreFactory routedStoreFactory = createFactory();

        return routedStoreFactory.create(cluster,
                                         storeDef,
                                         subStores,
                                         failureDetector,
                                         createConfig(timeOutMs));
    }

    private int countOccurances(RoutedStore routedStore, ByteArray key, Versioned<byte[]> value) {
        int count = 0;
        for(Store<ByteArray, byte[], byte[]> store: routedStore.getInnerStores().values())
            try {
                if(store.get(key, null).size() > 0
                   && Utils.deepEquals(store.get(key, null).get(0), value))
                    count += 1;
            } catch(VoldemortException e) {
                // This is normal for the failing store...
            }
        return count;
    }

    private void assertNEqual(RoutedStore routedStore,
                              int expected,
                              ByteArray key,
                              Versioned<byte[]> value) {
        int count = countOccurances(routedStore, key, value);
        assertEquals("Expected " + expected + " occurances of '" + key + "' with value '" + value
                     + "', but found " + count + ".", expected, count);
    }

    private void assertNOrMoreEqual(RoutedStore routedStore,
                                    int expected,
                                    ByteArray key,
                                    Versioned<byte[]> value) {
        int count = countOccurances(routedStore, key, value);
        assertTrue("Expected " + expected + " or more occurances of '" + key + "' with value '"
                   + value + "', but found " + count + ".", expected <= count);
    }

    /**
     * In case of Zoned cluster, there is a non trivial time required for the
     * delete. The custom timeout is used to account for this delay.
     */
    private void waitForOperationToComplete(long customSleepTime) {
        if(customSleepTime > 0) {
            try {
                Thread.sleep(customSleepTime);
            } catch(Exception e) {}
        }

    }

    private void testBasicOperations(int reads,
                                     int writes,
                                     int failures,
                                     int threads,
                                     RoutedStore customRoutedStore,
                                     long customSleepTime) throws Exception {

        RoutedStore routedStore = null;
        if(customRoutedStore == null) {
            routedStore = getStore(cluster, reads, writes, threads, failures);
        } else {
            routedStore = customRoutedStore;
        }

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        VectorClock clock = getClock(1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);
        routedStore.put(aKey, versioned, aTransform);

        waitForOperationToComplete(customSleepTime);
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);

        List<Versioned<byte[]>> found = store.get(aKey, aTransform);
        assertEquals(1, found.size());
        assertEquals(versioned, found.get(0));

        waitForOperationToComplete(customSleepTime);
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);

        assertTrue(routedStore.delete(aKey, versioned.getVersion()));

        waitForOperationToComplete(customSleepTime);
        assertNEqual(routedStore, 0, aKey, versioned);

        assertTrue(!routedStore.delete(aKey, versioned.getVersion()));
    }

    @Test
    public void testBasicOperationsSingleThreaded() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 0, 1, null, 0);
    }

    /**
     * Test to ensure that the basic operations can be performed successfully
     * against a 3 zone cluster with a single thread.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicOperationsZZZSingleThreaded() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = #nodes in a zone
        // PW = RW = #nodes in a zone
        // Zone Reads = # Zones - 1
        // Zone Writes = # Zones - 1
        // Threads = 1
        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodesInZone(0),
                                           cluster.getNumberOfNodesInZone(0),
                                           cluster.getNumberOfZones() - 1,
                                           cluster.getNumberOfZones() - 1,
                                           1,
                                           zoneReplicationFactor);

        testBasicOperations(cluster.getNumberOfNodes(),
                            cluster.getNumberOfNodes(),
                            0,
                            0,
                            routedStore,
                            500);
    }

    @Test
    public void testBasicOperationsMultiThreaded() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 0, 4, null, 0);
    }

    /**
     * Test to ensure that the basic operations can be performed successfully
     * against a 3 zone cluster with multiple threads.
     * 
     * @throws Exception
     */

    @Test
    public void testBasicOperationsZZZMultiThreaded() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = #nodes in a zone
        // PW = RW = #nodes in a zone
        // Zone Reads = # Zones - 1
        // Zone Writes = # Zones - 1
        // Threads = 4
        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodesInZone(0),
                                           cluster.getNumberOfNodesInZone(0),
                                           cluster.getNumberOfZones() - 1,
                                           cluster.getNumberOfZones() - 1,
                                           4,
                                           zoneReplicationFactor);

        testBasicOperations(cluster.getNumberOfNodes(),
                            cluster.getNumberOfNodes(),
                            0,
                            0,
                            routedStore,
                            500);
    }

    @Test
    public void testBasicOperationsMultiThreadedWithFailures() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes() - 2,
                            cluster.getNumberOfNodes() - 2,
                            2,
                            4,
                            null,
                            0);
    }

    /**
     * Test to ensure that the basic operations can be performed successfully
     * against a 3 zone cluster in the presence of failures.
     * 
     * @throws Exception
     */

    @Test
    public void testBasicOperationsZZZMultiThreadedWithFailures() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // This configuration uses zone reads = 0 in order to avoid getting
        // InsufficientZoneResponsesException. Please check the comment at the
        // end of PerformSerialRequests.java to understand this.

        // PR = RR = #nodes in a zone - 1
        // PW = RW = #nodes in a zone - 1
        // Zone Reads = 0
        // Zone Writes = # Zones - 1
        // Failing nodes = 1 from each zone
        // Threads = 4
        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodesInZone(0) - 1,
                                           cluster.getNumberOfNodesInZone(0) - 1,
                                           0,
                                           cluster.getNumberOfZones() - 1,
                                           4,
                                           Sets.newHashSet(1, 5, 6),
                                           null,
                                           zoneReplicationFactor,
                                           RoutingStrategyType.ZONE_STRATEGY,
                                           0,
                                           BANNAGE_PERIOD,
                                           new VoldemortException());

        testBasicOperations(cluster.getNumberOfNodes(),
                            cluster.getNumberOfNodes(),
                            3,
                            0,
                            routedStore,
                            1000);
    }

    /**
     * Test to ensure that the basic operations occur correctly, in the presence
     * of some sleepy nodes. NOTE: For some selection of the sleepy nodes, it is
     * possible that the operation will timeout. This particular set of sleepy
     * nodes is designed for the chosen key and the chosen global timeout.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicOperationsZZZMultiThreadedWithDelays() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = #nodes in a zone - 1
        // PW = RW = #nodes in a zone - 1
        // Zone Reads = 1
        // Zone Writes = 1
        // Sleepy nodes = 1 from each zone
        // Threads = 4
        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodesInZone(0) - 1,
                                           cluster.getNumberOfNodesInZone(0) - 1,
                                           1,
                                           1,
                                           4,
                                           null,
                                           Sets.newHashSet(2, 4, 8),
                                           zoneReplicationFactor,
                                           RoutingStrategyType.ZONE_STRATEGY,
                                           SLEEPY_TIME,
                                           100,
                                           new VoldemortException());

        testBasicOperations(cluster.getNumberOfNodes(),
                            cluster.getNumberOfNodes(),
                            3,
                            0,
                            routedStore,
                            1000);
    }

    private void testBasicOperationFailure(int reads,
                                           int writes,
                                           int failures,
                                           int threads,
                                           RoutedStore customRoutedStore) throws Exception {
        VectorClock clock = getClock(1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);

        RoutedStore routedStore = null;
        if(customRoutedStore == null) {
            routedStore = getStore(cluster,
                                   reads,
                                   writes,
                                   threads,
                                   failures,
                                   0,
                                   RoutingStrategyType.TO_ALL_STRATEGY,
                                   new UnreachableStoreException("no go"));
        } else {
            routedStore = customRoutedStore;
        }

        try {
            routedStore.put(aKey, versioned, aTransform);
            fail("Put succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            // expected
        }
        try {
            routedStore.get(aKey, aTransform);
            fail("Get succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            // expected
        }
        try {
            routedStore.delete(aKey, versioned.getVersion());
            fail("Get succeeded with too few operational nodes.");
        } catch(InsufficientOperationalNodesException e) {
            // expected
        }
    }

    @Test
    public void testBasicOperationFailureMultiThreaded() throws Exception {
        testBasicOperationFailure(cluster.getNumberOfNodes() - 2,
                                  cluster.getNumberOfNodes() - 2,
                                  4,
                                  4,
                                  null);
    }

    /**
     * Test to ensure that the basic operations fail in the presence of bad
     * nodes in a 3 zone cluster.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicOperationFailureZZZMultiThreaded() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = 7
        // PW = RW = 7
        // Zone Reads = # Zones - 1
        // Zone Writes = # Zones - 1
        // Failing nodes = 1 from each zone
        // Threads = 4
        RoutedStore zonedRoutedStore = getStore(cluster,
                                                7,
                                                7,
                                                cluster.getNumberOfZones() - 1,
                                                cluster.getNumberOfZones() - 1,
                                                4,
                                                Sets.newHashSet(1, 5, 6),
                                                null,
                                                zoneReplicationFactor,
                                                RoutingStrategyType.ZONE_STRATEGY,
                                                0,
                                                BANNAGE_PERIOD,
                                                new UnreachableStoreException("no go"));

        testBasicOperationFailure(cluster.getNumberOfNodes() - 2,
                                  cluster.getNumberOfNodes() - 2,
                                  0,
                                  0,
                                  zonedRoutedStore);
    }

    /**
     * Test to ensure that the basic operations fail, in the presence of some
     * sleepy nodes and zone count reads and writes = 2 in a 3 zone cluster.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicOperationsFailureZZZMultiThreadedWithDelays() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = #nodes in a zone - 1
        // PW = RW = #nodes in a zone - 1
        // Zone Reads = # Zones - 1
        // Zone Writes = # Zones - 1
        // Sleepy nodes = 1 from each zone
        // Threads = 4
        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodesInZone(0) - 1,
                                           cluster.getNumberOfNodesInZone(0) - 1,
                                           cluster.getNumberOfZones() - 1,
                                           cluster.getNumberOfZones() - 1,
                                           4,
                                           null,
                                           Sets.newHashSet(2, 4, 8),
                                           zoneReplicationFactor,
                                           RoutingStrategyType.ZONE_STRATEGY,
                                           SLEEPY_TIME,
                                           OPERATION_TIMEOUT,
                                           new VoldemortException());

        try {
            testBasicOperations(cluster.getNumberOfNodes(),
                                cluster.getNumberOfNodes(),
                                3,
                                0,
                                routedStore,
                                1000);
            fail("Too few successful zone responses. Should've failed.");
        } catch(InsufficientZoneResponsesException ize) {
            // Expected
        }
    }

    @Test
    public void testPutIncrementsVersion() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getStore();
        VectorClock clock = new VectorClock();
        VectorClock copy = clock.clone();
        store.put(aKey, new Versioned<byte[]>(getValue(), clock), aTransform);
        List<Versioned<byte[]>> found = store.get(aKey, aTransform);
        assertEquals("Invalid number of items found.", 1, found.size());
        assertEquals("Version not incremented properly",
                     Occurred.BEFORE,
                     copy.compare(found.get(0).getVersion()));
    }

    @Test
    public void testPutIncrementsVersionZZZ() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getZonedStore();
        VectorClock clock = new VectorClock();
        VectorClock copy = clock.clone();
        store.put(aKey, new Versioned<byte[]>(getValue(), clock), aTransform);
        List<Versioned<byte[]>> found = store.get(aKey, aTransform);
        assertEquals("Invalid number of items found.", 1, found.size());
        assertEquals("Version not incremented properly",
                     Occurred.BEFORE,
                     copy.compare(found.get(0).getVersion()));
    }

    @Test
    public void testObsoleteMasterFails() {
        // write me
    }

    @Test
    public void testZoneRouting() throws Exception {
        cluster = VoldemortTestConstants.getEightNodeClusterWithZones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, 2);
        zoneReplicationFactor.put(1, 2);

        long start;
        Versioned<byte[]> versioned = new Versioned<byte[]>(new byte[] { 1 });

        // Basic put with zone read = 0, zone write = 0 and timeout < cross-zone
        // latency
        Store<ByteArray, byte[], byte[]> s1 = getStore(cluster,
                                                       1,
                                                       1,
                                                       0,
                                                       0,
                                                       8,
                                                       null,
                                                       Sets.newHashSet(4, 5, 6, 7),
                                                       zoneReplicationFactor,
                                                       RoutingStrategyType.ZONE_STRATEGY,
                                                       SLEEPY_TIME,
                                                       OPERATION_TIMEOUT,
                                                       new VoldemortException());

        start = System.nanoTime();
        try {
            s1.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }
        // Putting extra key to test getAll
        s1.put(new ByteArray("test2".getBytes()), versioned, null);

        start = System.nanoTime();
        try {
            s1.get(new ByteArray("test".getBytes()), null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        start = System.nanoTime();
        try {
            List<Version> versions = s1.getVersions(new ByteArray("test".getBytes()));
            for(Version version: versions) {
                assertEquals(version.compare(versioned.getVersion()), Occurred.BEFORE);
            }
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        start = System.nanoTime();
        try {
            s1.delete(new ByteArray("test".getBytes()), versioned.getVersion());
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        // make sure sleepy stores processed the delete before checking,
        // otherwise, we might be bailing
        // out of the test too early for the delete to be processed.
        Thread.sleep(SLEEPY_TIME * 2);
        List<ByteArray> keys = Lists.newArrayList(new ByteArray("test".getBytes()),
                                                  new ByteArray("test2".getBytes()));

        Map<ByteArray, List<Versioned<byte[]>>> values = s1.getAll(keys, null);
        assertFalse("'test' did not get deleted.",
                    values.containsKey(new ByteArray("test".getBytes())));
        ByteUtils.compare(values.get(new ByteArray("test2".getBytes())).get(0).getValue(),
                          new byte[] { 1 });

        // Basic put with zone read = 1, zone write = 1
        Store<ByteArray, byte[], byte[]> s2 = getStore(cluster,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       8,
                                                       null,
                                                       Sets.newHashSet(4, 5, 6, 7),
                                                       zoneReplicationFactor,
                                                       RoutingStrategyType.ZONE_STRATEGY,
                                                       SLEEPY_TIME,
                                                       BANNAGE_PERIOD,
                                                       new VoldemortException());

        start = System.nanoTime();

        try {
            s2.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " > " + SLEEPY_TIME, elapsed >= SLEEPY_TIME);
        }
        s2.put(new ByteArray("test2".getBytes()), versioned, null);

        try {
            s2.get(new ByteArray("test".getBytes()), null);
            fail("Should have shown exception");
        } catch(InsufficientZoneResponsesException e) {
            /*
             * Why would you want responses from two zones and wait for only one
             * response...
             */
        }

        try {
            s2.getVersions(new ByteArray("test".getBytes()));
            fail("Should have shown exception");
        } catch(InsufficientZoneResponsesException e) {
            /*
             * Why would you want responses from two zones and wait for only one
             * response...
             */
        }

        try {
            s2.delete(new ByteArray("test".getBytes()), null);
        } catch(InsufficientZoneResponsesException e) {
            /*
             * Why would you want responses from two zones and wait for only one
             * response...
             */
        }

        values = s2.getAll(keys, null);
        assertFalse("'test' did not get deleted.",
                    values.containsKey(new ByteArray("test".getBytes())));
        ByteUtils.compare(values.get(new ByteArray("test2".getBytes())).get(0).getValue(),
                          new byte[] { 1 });

        // Basic put with zone read = 0, zone write = 0 and failures in other
        // dc, but should still work
        Store<ByteArray, byte[], byte[]> s3 = getStore(cluster,
                                                       1,
                                                       1,
                                                       0,
                                                       0,
                                                       8,
                                                       Sets.newHashSet(4, 5, 6, 7),
                                                       null,
                                                       zoneReplicationFactor,
                                                       RoutingStrategyType.ZONE_STRATEGY,
                                                       SLEEPY_TIME,
                                                       BANNAGE_PERIOD,
                                                       new VoldemortException());

        start = System.nanoTime();
        try {
            s3.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }
        // Putting extra key to test getAll
        s3.put(new ByteArray("test2".getBytes()), versioned, null);

        start = System.nanoTime();
        try {
            List<Version> versions = s3.getVersions(new ByteArray("test".getBytes()));
            for(Version version: versions) {
                assertEquals(version.compare(versioned.getVersion()), Occurred.BEFORE);
            }
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        start = System.nanoTime();
        try {
            s3.get(new ByteArray("test".getBytes()), null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        start = System.nanoTime();
        try {
            s3.delete(new ByteArray("test".getBytes()), versioned.getVersion());
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        // Basic put with zone read = 1, zone write = 1 and failures in other
        // dc, should not work
        Store<ByteArray, byte[], byte[]> s4 = getStore(cluster,
                                                       2,
                                                       2,
                                                       1,
                                                       1,
                                                       8,
                                                       Sets.newHashSet(4, 5, 6, 7),
                                                       null,
                                                       zoneReplicationFactor,
                                                       RoutingStrategyType.ZONE_STRATEGY,
                                                       SLEEPY_TIME,
                                                       BANNAGE_PERIOD,
                                                       new VoldemortException());

        try {
            s4.put(new ByteArray("test".getBytes()), new Versioned<byte[]>(new byte[] { 1 }), null);
            fail("Should have shown exception");
        } catch(InsufficientZoneResponsesException e) {
            /*
             * The other zone is down and you expect a result from both zones
             */
        }

        try {
            s4.getVersions(new ByteArray("test".getBytes()));
            fail("Should have shown exception");
        } catch(InsufficientZoneResponsesException e) {
            /*
             * The other zone is down and you expect a result from both zones
             */
        }

        try {
            s4.get(new ByteArray("test".getBytes()), null);
            fail("Should have shown exception");
        } catch(InsufficientZoneResponsesException e) {
            /*
             * The other zone is down and you expect a result from both zones
             */
        }

        try {
            s4.delete(new ByteArray("test".getBytes()), versioned.getVersion());
            fail("Should have shown exception");
        } catch(InsufficientZoneResponsesException e) {
            /*
             * The other zone is down and you expect a result from both zones
             */
        }

    }

    @Test
    public void testOnlyNodeFailuresDisableNode() throws Exception {
        // test put
        cluster = getNineNodeCluster();

        Store<ByteArray, byte[], byte[]> s1 = getStore(cluster,
                                                       1,
                                                       9,
                                                       9,
                                                       9,
                                                       0,
                                                       RoutingStrategyType.TO_ALL_STRATEGY,
                                                       new VoldemortException());
        try {
            s1.put(aKey, new Versioned<byte[]>(aValue), aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
        }
        assertOperationalNodes(9);

        cluster = getNineNodeCluster();

        Store<ByteArray, byte[], byte[]> s2 = getStore(cluster,
                                                       1,
                                                       9,
                                                       9,
                                                       9,
                                                       0,
                                                       RoutingStrategyType.TO_ALL_STRATEGY,
                                                       new UnreachableStoreException("no go"));
        try {
            s2.put(aKey, new Versioned<byte[]>(aValue), aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
        }
        assertOperationalNodes(0);

        // test get
        cluster = getNineNodeCluster();

        s1 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new VoldemortException());
        try {
            s1.get(aKey, aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
        }
        assertOperationalNodes(9);

        cluster = getNineNodeCluster();

        s2 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new UnreachableStoreException("no go"));
        try {
            s2.get(aKey, aTransform);
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
        }
        assertOperationalNodes(0);

        // test delete
        cluster = getNineNodeCluster();

        s1 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new VoldemortException());
        try {
            s1.delete(aKey, new VectorClock());
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
        }
        assertOperationalNodes(9);

        cluster = getNineNodeCluster();

        s2 = getStore(cluster,
                      1,
                      9,
                      9,
                      9,
                      0,
                      RoutingStrategyType.TO_ALL_STRATEGY,
                      new UnreachableStoreException("no go"));
        try {
            s2.delete(aKey, new VectorClock());
            fail("Failure is expected");
        } catch(InsufficientOperationalNodesException e) { /* expected */
        }
        assertOperationalNodes(0);
    }

    @Test
    public void testGetVersions2() throws Exception {
        List<ByteArray> keys = getKeys(2);
        ByteArray key = keys.get(0);
        byte[] value = getValue();
        Store<ByteArray, byte[], byte[]> store = getStore();
        store.put(key, Versioned.value(value), null);
        List<Versioned<byte[]>> versioneds = store.get(key, null);
        List<Version> versions = store.getVersions(key);
        assertEquals(1, versioneds.size());
        assertEquals(9, versions.size());
        for(int i = 0; i < versions.size(); i++)
            assertEquals(versioneds.get(0).getVersion(), versions.get(i));

        assertEquals(0, store.getVersions(keys.get(1)).size());
    }

    @Test
    public void testGetVersions2ZZZ() throws Exception {
        List<ByteArray> keys = getKeys(2);
        ByteArray key = keys.get(0);
        byte[] value = getValue();
        Store<ByteArray, byte[], byte[]> store = getZonedStore();
        store.put(key, Versioned.value(value), null);
        List<Versioned<byte[]>> versioneds = store.get(key, null);
        List<Version> versions = store.getVersions(key);
        assertEquals(1, versioneds.size());
        assertEquals(9, versions.size());
        for(int i = 0; i < versions.size(); i++)
            assertEquals(versioneds.get(0).getVersion(), versions.get(i));

        assertEquals(0, store.getVersions(keys.get(1)).size());
    }

    /**
     * Util function to test getAll with one node down
     * 
     * @param store The Routed store object used to perform the put and getall
     * @throws Exception
     */
    private void getAllWithNodeDown(Store<ByteArray, byte[], byte[]> store) throws Exception {

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }

        recordException(failureDetector, cluster.getNodes().iterator().next());

        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet(), null);
        assertEquals(expectedValues.size(), all.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: all.entrySet()) {
            byte[] value = expectedValues.get(mapEntry.getKey());
            assertEquals(new ByteArray(value), new ByteArray(mapEntry.getValue().get(0).getValue()));
        }
    }

    /**
     * Tests that getAll works correctly with a node down in a two node cluster.
     */
    @Test
    public void testGetAllWithNodeDown() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        RoutedStore routedStore = getStore(cluster, 1, 2, 1, 0);
        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        getAllWithNodeDown(store);
    }

    /**
     * Tests that getAll works correctly with a node down in a three node three
     * zone cluster.
     */
    @Test
    public void testGetAllWithNodeDownZZZ() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeClusterWith3Zones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = 2
        // PW = RW = 3
        // Zone Reads = 0
        // Zone Writes = 0
        // Threads = 1
        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodes() - 1,
                                           cluster.getNumberOfNodes(),
                                           0,
                                           0,
                                           1,
                                           zoneReplicationFactor);

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        getAllWithNodeDown(store);
    }

    /**
     * Tests that getAll returns partial results
     */
    @Test
    public void testPartialGetAll() throws Exception {
        // create a store with rf=1 i.e disjoint partitions
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(1)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();

        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        // create nodes with varying speeds - 100ms, 200ms, 300ms
        for(int i = 0; i < 3; i++) {
            Store<ByteArray, byte[], byte[]> store = new SleepyStore<ByteArray, byte[], byte[]>(100 * (i + 1),
                                                                                                new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, 0, partitions));
        }
        setFailureDetector(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);

        TimeoutConfig timeoutConfig = new TimeoutConfig(1500, true);
        // This means, the getall will only succeed on two of the nodes
        timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE, 250);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            failureDetector,
                                                            createConfig(timeoutConfig));
        /* do some puts so we have some data to test getalls */
        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            routedStore.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }

        /* 1. positive test; if partial is on, should get something back */
        Map<ByteArray, List<Versioned<byte[]>>> all = routedStore.getAll(expectedValues.keySet(),
                                                                         null);
        assert (expectedValues.size() > all.size());

        /* 2. negative test; if partial is off, should fail the whole operation */
        timeoutConfig.setPartialGetAllAllowed(false);
        try {
            all = routedStore.getAll(expectedValues.keySet(), null);
            fail("Should have failed");
        } catch(Exception e) {

        }
    }

    /**
     * Tests that getAll returns partial results in a 3 zone cluster (with a
     * node down).
     */
    @Test
    public void testPartialGetAllZZZ() throws Exception {

        // Set replication factors for a 3 zone cluster
        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, 1);
        zoneReplicationFactor.put(1, 1);
        zoneReplicationFactor.put(2, 1);

        // Create a store with RF=3, Required reads = 3 and zone count reads = 2
        // This ensures that a GET operation requires a response from all 3
        // nodes (from the respective 3 zones)
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                 .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(3)
                                                                 .setRequiredReads(3)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .setZoneCountReads(2)
                                                                 .setZoneCountWrites(1)
                                                                 .setZoneReplicationFactor(zoneReplicationFactor)
                                                                 .build();

        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        // create nodes with varying speeds - 100ms, 200ms, 300ms
        for(int i = 0; i < 3; i++) {
            Store<ByteArray, byte[], byte[]> store = new SleepyStore<ByteArray, byte[], byte[]>(100 * (i + 1),
                                                                                                new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);

            // Create zoned nodes - one in each zone (0, 1, 2)
            nodes.add(new Node(i, "none", 0, 0, 0, i, partitions));
        }
        setFailureDetector(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);

        TimeoutConfig timeoutConfig = new TimeoutConfig(1500, true);
        // This means, the getall will only succeed on two of the nodes
        timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE, 250);
        RoutedStoreFactory routedStoreFactory = createFactory();

        List<Zone> zones = Lists.newArrayList();

        for(int i = 0; i < 3; i++) {
            LinkedList<Integer> zoneProximityList = Lists.newLinkedList();
            Set<Integer> zoneIds = Sets.newHashSet(0, 1, 2);
            zoneIds.remove(i);
            zoneProximityList.addAll(zoneIds);
            zones.add(new Zone(i, zoneProximityList));
        }

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes, zones),
                                                            definition,
                                                            stores,
                                                            failureDetector,
                                                            createConfig(timeoutConfig));
        /* do some puts so we have some data to test getalls */
        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            routedStore.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }

        /* 1. positive test; if partial is on, should get something back */
        Map<ByteArray, List<Versioned<byte[]>>> all = routedStore.getAll(expectedValues.keySet(),
                                                                         null);
        assert (expectedValues.size() > all.size());

        /* 2. negative test; if partial is off, should fail the whole operation */
        timeoutConfig.setPartialGetAllAllowed(false);
        try {
            all = routedStore.getAll(expectedValues.keySet(), null);
            fail("Should have failed");
        } catch(Exception e) {
            // Expected
        }
    }

    @Test
    public void testGetAllWithFailingStore() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               2,
                                                               1,
                                                               1,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        int id1 = Iterables.get(cluster.getNodes(), 0).getId();
        int id2 = Iterables.get(cluster.getNodes(), 1).getId();
        subStores.put(id1, new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
        subStores.put(id2, new FailingReadsStore<ByteArray, byte[], byte[]>("test"));

        setFailureDetector(subStores);
        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(BANNAGE_PERIOD));

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }

        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet(), null);
        assertEquals(expectedValues.size(), all.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: all.entrySet()) {
            byte[] value = expectedValues.get(mapEntry.getKey());
            assertEquals(new ByteArray(value), new ByteArray(mapEntry.getValue().get(0).getValue()));
        }
    }

    /**
     * Test to ensure that get all works in a 3 zone cluster with 2 nodes per
     * zone and 1 node down in each zone.
     */
    @Test
    public void testGetAllWithFailingStoreZZZ() throws Exception {
        cluster = VoldemortTestConstants.getSixNodeClusterWith3Zones();
        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = 3
        // PW = RW = 6
        // Zone Reads = 2
        // Zone Writes = 2
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               6,
                                                               6,
                                                               2,
                                                               2,
                                                               zoneReplicationFactor,
                                                               HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                               RoutingStrategyType.ZONE_STRATEGY);

        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        for(int id = 0; id < 6; id++) {
            // Mark all the even nodes as normal and odd as read-failing
            // This ensures that one node in each zone is read-failing and the
            // other is normal
            if(id % 2 == 0) {
                subStores.put(id, new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            } else {
                subStores.put(id, new FailingReadsStore<ByteArray, byte[], byte[]>("test"));
            }
        }

        setFailureDetector(subStores);
        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = createFactory();
        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(BANNAGE_PERIOD));

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        Map<ByteArray, byte[]> expectedValues = Maps.newHashMap();
        for(byte i = 1; i < 11; ++i) {
            ByteArray key = new ByteArray(new byte[] { i });
            byte[] value = new byte[] { (byte) (i + 50) };
            store.put(key, Versioned.value(value), null);
            expectedValues.put(key, value);
        }

        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(expectedValues.keySet(), null);
        assertEquals(expectedValues.size(), all.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: all.entrySet()) {
            byte[] value = expectedValues.get(mapEntry.getKey());
            assertEquals(new ByteArray(value), new ByteArray(mapEntry.getValue().get(0).getValue()));
        }
    }

    /**
     * One node up, two preferred reads and one required read. See:
     * 
     * http://github.com/voldemort/voldemort/issues#issue/18
     */
    @Test
    public void testGetAllWithMorePreferredReadsThanNodes() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               2,
                                                               2,
                                                               1,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        int id1 = Iterables.get(cluster.getNodes(), 0).getId();
        int id2 = Iterables.get(cluster.getNodes(), 1).getId();
        subStores.put(id1, new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
        subStores.put(id2, new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));

        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(BANNAGE_PERIOD));

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, Versioned.value(aValue), aTransform);
        recordException(failureDetector, cluster.getNodes().iterator().next());
        Map<ByteArray, List<Versioned<byte[]>>> all = store.getAll(Arrays.asList(aKey),
                                                                   Collections.singletonMap(aKey,
                                                                                            aTransform));
        assertEquals(1, all.size());
        assertTrue(Arrays.equals(aValue, all.values().iterator().next().get(0).getValue()));
    }

    /**
     * See Issue #89: Sequential retrieval in RoutedStore.get doesn't consider
     * repairReads.
     */
    @Test
    public void testReadRepairWithFailures() throws Exception {
        cluster = getNineNodeCluster();

        RoutedStore routedStore = getStore(cluster, 2, 2, 1, 0);
        BaseStoreRoutingPlan routingPlan = new BaseStoreRoutingPlan(cluster, this.storeDef);
        List<Integer> replicatingNodes = routingPlan.getReplicationNodeList(aKey.get());
        // This is node 1
        Node primaryNode = Iterables.get(cluster.getNodes(), replicatingNodes.get(0));
        // This is node 6
        Node secondaryNode = Iterables.get(cluster.getNodes(), replicatingNodes.get(1));

        // Disable primary node so that the first put happens with 6 as the
        // pseudo master
        recordException(failureDetector, primaryNode);
        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, new Versioned<byte[]>(aValue), null);

        byte[] anotherValue = "john".getBytes();

        /*
         * Disable the secondary node and enable primary node to prevent the
         * secondary from getting the new version
         */
        recordException(failureDetector, secondaryNode);
        recordSuccess(failureDetector, primaryNode);
        // Generate the clock based off secondary so that the resulting clock
        // will be [1:1, 6:1] across the replicas, except for the secondary
        // which will be [6:1]
        VectorClock clock = getClock(6);
        store.put(aKey, new Versioned<byte[]>(anotherValue, clock), null);

        // Enable secondary and disable primary, the following get should cause
        // a read repair on the secondary in the code path that is only executed
        // if there are failures. This should repair the secondary with the
        // superceding clock [1:1,6:1]
        recordException(failureDetector, primaryNode);
        recordSuccess(failureDetector, secondaryNode);
        List<Versioned<byte[]>> versioneds = store.get(aKey, null);
        assertEquals(1, versioneds.size());
        assertEquals(new ByteArray(anotherValue), new ByteArray(versioneds.get(0).getValue()));

        // Read repairs are done asynchronously, so we sleep for a short period.
        // It may be a good idea to use a synchronous executor service.
        Thread.sleep(500);
        for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> innerStoreEntry: routedStore.getInnerStores()
                                                                                             .entrySet()) {
            // Only look at the nodes in the pref list
            if(replicatingNodes.contains(innerStoreEntry.getKey())) {
                List<Versioned<byte[]>> innerVersioneds = innerStoreEntry.getValue()
                                                                         .get(aKey, null);
                assertEquals(1, versioneds.size());
                assertEquals(new ByteArray(anotherValue), new ByteArray(innerVersioneds.get(0)
                                                                                       .getValue()));
            }
        }
    }

    /**
     * Test to ensure that read repair happens correctly across zones in case of
     * inconsistent writes in a 3 zone cluster.
     * 
     * @throws Exception
     */
    @Test
    public void testReadRepairWithFailuresZZZ() throws Exception {
        cluster = VoldemortTestConstants.getSixNodeClusterWith3Zones();
        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // PR = RR = 6
        // PW = RW = 4
        // Zone Reads = # Zones - 1
        // Zone Writes = 1
        // Threads = 1
        RoutedStore routedStore = getStore(cluster,
                                           6,
                                           4,
                                           cluster.getNumberOfZones() - 1,
                                           1,
                                           1,
                                           zoneReplicationFactor);

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        BaseStoreRoutingPlan routingPlan = new BaseStoreRoutingPlan(cluster, this.storeDef);
        List<Integer> replicatingNodes = routingPlan.getReplicationNodeList(aKey.get());

        try {
            // Do the initial put with all nodes up
            store.put(aKey, new Versioned<byte[]>(aValue), null);

            List<Version> initialVersions = store.getVersions(aKey);
            assertEquals(6, initialVersions.size());

            Version mainVersion = initialVersions.get(0);
            for(int i = 1; i < initialVersions.size(); i++) {
                assertEquals(mainVersion, initialVersions.get(i));
            }

            // Do another put with all nodes in the zone 0 marked as
            // unavailable. This will force the put to use a different pseudo
            // master than before.
            byte[] anotherValue = "john".getBytes();

            // In this cluster, nodes 0 and 1 are in Zone 0. Mark them
            // unavailable
            recordException(failureDetector, cluster.getNodeById(0));
            recordException(failureDetector, cluster.getNodeById(1));
            Version newVersion = ((VectorClock) mainVersion).clone();
            store.put(aKey, new Versioned<byte[]>(anotherValue, newVersion), null);

            waitForOperationToComplete(500);

            // Mark the nodes in Zone 0 as available and do a get. The Required
            // reads = 4 and Zone count reads = 2 will force the client to read
            // from all the zones and do the essential read repairs.
            recordSuccess(failureDetector, cluster.getNodeById(0));
            recordSuccess(failureDetector, cluster.getNodeById(1));
            List<Versioned<byte[]>> versioneds = store.get(aKey, null);
            assertEquals(1, versioneds.size());
            assertEquals(new ByteArray(anotherValue), new ByteArray(versioneds.get(0).getValue()));

            // Read repairs are done asynchronously, so we sleep for a short
            // period. It may be a good idea to use a synchronous executor
            // service.
            Thread.sleep(500);
            for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> innerStoreEntry: routedStore.getInnerStores()
                                                                                                 .entrySet()) {
                // Only look at the nodes in the pref list
                if(replicatingNodes.contains(innerStoreEntry.getKey())) {
                    List<Versioned<byte[]>> innerVersioneds = innerStoreEntry.getValue().get(aKey,
                                                                                             null);
                    assertEquals(1, versioneds.size());
                    assertEquals(new ByteArray(anotherValue),
                                 new ByteArray(innerVersioneds.get(0).getValue()));
                }
            }

        } catch(VoldemortException ve) {
            fail("Unexpected error occurred : " + ve);
        }
    }

    /**
     * See issue #134: RoutedStore put() doesn't wait for enough attempts to
     * succeed
     * 
     * This issue would only happen with one node down and another that was slow
     * to respond.
     */
    @Test
    public void testPutWithOneNodeDownAndOneNodeSlow() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               2,
                                                               2,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        /* The key used causes the nodes selected for writing to be [2, 0, 1] */
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        int id1 = Iterables.get(cluster.getNodes(), 0).getId();
        int id2 = Iterables.get(cluster.getNodes(), 1).getId();
        int id3 = Iterables.get(cluster.getNodes(), 2).getId();

        subStores.put(id3, new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
        subStores.put(id1, new FailingStore<ByteArray, byte[], byte[]>("test"));
        /*
         * The bug would only show itself if the second successful required
         * write was slow (but still within the timeout).
         */
        subStores.put(id2,
                      new SleepyStore<ByteArray, byte[], byte[]>(100,
                                                                 new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test")));

        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(BANNAGE_PERIOD));

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, new Versioned<byte[]>(aValue), aTransform);
    }

    /**
     * See issue #134: RoutedStore put() doesn't wait for enough attempts to
     * succeed
     * 
     * This issue would only happen with one node down and another that was slow
     * to respond in a 3 zone cluster.
     */
    @Test
    public void testPutWithOneNodeDownAndOneNodeSlowZZZ() throws Exception {
        cluster = VoldemortTestConstants.getSixNodeClusterWith3Zones();
        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(1, cluster.getNumberOfNodesInZone(0));
        zoneReplicationFactor.put(2, cluster.getNumberOfNodesInZone(0));

        // The replication set for aKey is [1, 2, 0, 3, 5, 4]
        // As per problem statement, set node 2 as the failing node and node 0
        // as the sleepy node

        // PR = RR = 4
        // PW = RW = 4
        // Zone Reads = 0
        // Zone Writes = 0
        // Failing nodes = Node 2
        // Sleepy node = Node 0
        // Threads = 4
        RoutedStore routedStore = getStore(cluster,
                                           4,
                                           4,
                                           0,
                                           0,
                                           4,
                                           Sets.newHashSet(2),
                                           Sets.newHashSet(0),
                                           zoneReplicationFactor,
                                           RoutingStrategyType.ZONE_STRATEGY,
                                           0,
                                           BANNAGE_PERIOD,
                                           new VoldemortException());
        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());

        try {
            store.put(aKey, new Versioned<byte[]>(aValue), aTransform);
        } catch(VoldemortException ve) {
            fail("Unknown exception occurred : " + ve);
        }
    }

    @Test
    public void testPutTimeout() throws Exception {
        int timeout = 50;
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(3)
                                                                 .setRequiredReads(3)
                                                                 .setPreferredWrites(3)
                                                                 .setRequiredWrites(3)
                                                                 .build();
        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        int totalDelay = 0;
        for(int i = 0; i < 3; i++) {
            int delay = 4 + i * timeout;
            totalDelay += delay;
            Store<ByteArray, byte[], byte[]> store = new SleepyStore<ByteArray, byte[], byte[]>(delay,
                                                                                                new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, 0, partitions));
        }

        setFailureDetector(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            failureDetector,
                                                            createConfig(timeout));

        long start = System.nanoTime();
        try {
            routedStore.put(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }),
                            null);
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
        }
    }

    @Test
    public void testGetTimeout() throws Exception {
        int timeout = 50;
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(3)
                                                                 .setRequiredReads(3)
                                                                 .setPreferredWrites(3)
                                                                 .setRequiredWrites(3)
                                                                 .build();
        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        int totalDelay = 0;
        for(int i = 0; i < 3; i++) {
            int delay = 4 + i * timeout;
            totalDelay += delay;
            Store<ByteArray, byte[], byte[]> store = new SleepyStore<ByteArray, byte[], byte[]>(delay,
                                                                                                new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, 0, partitions));
        }

        setFailureDetector(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            failureDetector,
                                                            createConfig(timeout));

        long start = System.nanoTime();
        try {
            routedStore.get(new ByteArray("test".getBytes()), null);
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
        }
    }

    @Test
    public void testGetAndPutTimeoutZZZ() throws Exception {
        int timeout = 50;

        // Set replication factors for a 3 zone cluster
        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, 1);
        zoneReplicationFactor.put(1, 1);
        zoneReplicationFactor.put(2, 1);

        // Create a store with RF=3, Required reads = 3 and zone count reads = 2
        // This ensures that a GET operation requires a response from all 3
        // nodes (from the respective 3 zones)
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                 .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(3)
                                                                 .setRequiredReads(3)
                                                                 .setPreferredWrites(3)
                                                                 .setRequiredWrites(3)
                                                                 .setZoneCountReads(2)
                                                                 .setZoneCountWrites(2)
                                                                 .setZoneReplicationFactor(zoneReplicationFactor)
                                                                 .build();

        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        int totalDelay = 0;
        for(int i = 0; i < 3; i++) {
            int delay = 4 + i * timeout;
            totalDelay += delay;
            Store<ByteArray, byte[], byte[]> store = new SleepyStore<ByteArray, byte[], byte[]>(delay,
                                                                                                new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, 0, i, partitions));
        }

        setFailureDetector(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);
        RoutedStoreFactory routedStoreFactory = createFactory();

        List<Zone> zones = Lists.newArrayList();

        for(int i = 0; i < 3; i++) {
            LinkedList<Integer> zoneProximityList = Lists.newLinkedList();
            Set<Integer> zoneIds = Sets.newHashSet(0, 1, 2);
            zoneIds.remove(i);
            zoneProximityList.addAll(zoneIds);
            zones.add(new Zone(i, zoneProximityList));
        }

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes, zones),
                                                            definition,
                                                            stores,
                                                            failureDetector,
                                                            createConfig(timeout));

        long start = System.nanoTime();
        try {
            routedStore.get(new ByteArray("test".getBytes()), null);
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
        }

        start = System.nanoTime();
        try {
            routedStore.put(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }),
                            null);
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
        }
    }

    @Test
    public void testOperationSpecificTimeouts() throws Exception {
        StoreDefinition definition = new StoreDefinitionBuilder().setName("test")
                                                                 .setType("foo")
                                                                 .setKeySerializer(new SerializerDefinition("test"))
                                                                 .setValueSerializer(new SerializerDefinition("test"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(3)
                                                                 .setPreferredReads(3)
                                                                 .setRequiredReads(3)
                                                                 .setPreferredWrites(3)
                                                                 .setRequiredWrites(3)
                                                                 .build();
        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < 3; i++) {
            Store<ByteArray, byte[], byte[]> store = new SleepyStore<ByteArray, byte[], byte[]>(200,
                                                                                                new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            stores.put(i, store);
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, 0, partitions));
        }

        setFailureDetector(stores);

        routedStoreThreadPool = Executors.newFixedThreadPool(3);
        // with a 500ms general timeout and a 100ms get timeout, only get should
        // fail
        TimeoutConfig timeoutConfig = new TimeoutConfig(1500, false);
        timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_OP_CODE, 100);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            failureDetector,
                                                            createConfig(timeoutConfig));
        try {
            routedStore.put(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }),
                            null);
        } catch(InsufficientOperationalNodesException e) {
            fail("Should not have failed");
        }

        try {
            routedStore.get(new ByteArray("test".getBytes()), null);
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {

        }
    }

    /**
     * See Issue #211: Unnecessary read repairs during getAll with more than one
     * key
     */
    @Test
    public void testNoReadRepair() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               2,
                                                               1,
                                                               3,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        /* We just need to keep a store from one node */
        StatTrackingStore statTrackingStore = null;
        for(int i = 0; i < 3; ++i) {
            int id = Iterables.get(cluster.getNodes(), i).getId();
            statTrackingStore = new StatTrackingStore(new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                                      null);
            subStores.put(id, statTrackingStore);

        }
        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(BANNAGE_PERIOD));

        ByteArray key1 = aKey;
        routedStore.put(key1, Versioned.value("value1".getBytes()), null);
        ByteArray key2 = TestUtils.toByteArray("voldemort");
        routedStore.put(key2, Versioned.value("value2".getBytes()), null);

        long putCount = statTrackingStore.getStats().getCount(Tracked.PUT);
        routedStore.getAll(Arrays.asList(key1, key2), null);
        /* Read repair happens asynchronously, so we wait a bit */
        Thread.sleep(500);
        assertEquals("put count should remain the same if there are no read repairs",
                     putCount,
                     statTrackingStore.getStats().getCount(Tracked.PUT));
    }

    @Test
    public void testTardyResponsesNotIncludedInResult() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               2,
                                                               3,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        int sleepTimeMs = 500;
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[], byte[]>(sleepTimeMs, store);
            }

            subStores.put(node.getId(), store);
        }

        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(10000L));

        routedStore.put(aKey, Versioned.value(aValue), null);

        routedStoreFactory = createFactory();

        routedStore = routedStoreFactory.create(cluster,
                                                storeDef,
                                                subStores,
                                                failureDetector,
                                                createConfig(sleepTimeMs / 2));

        List<Versioned<byte[]>> versioneds = routedStore.get(aKey, null);
        assertEquals(2, versioneds.size());

        // Let's make sure that if the response *does* come in late, that it
        // doesn't alter the return value.
        Thread.sleep(sleepTimeMs * 2);
        assertEquals(2, versioneds.size());
    }

    @Test
    public void testSlowStoreDowngradesFromPreferredToRequired() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               2,
                                                               3,
                                                               1,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        int sleepTimeMs = 500;
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            if(subStores.isEmpty()) {
                store = new SleepyStore<ByteArray, byte[], byte[]>(sleepTimeMs, store);
            }

            subStores.put(node.getId(), store);
        }

        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        RoutedStoreFactory routedStoreFactory = createFactory();

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            failureDetector,
                                                            createConfig(10000L));

        routedStore.put(aKey, Versioned.value(aValue), null);

        routedStoreFactory = createFactory();

        routedStore = routedStoreFactory.create(cluster,
                                                storeDef,
                                                subStores,
                                                failureDetector,
                                                createConfig(sleepTimeMs / 2));

        List<Versioned<byte[]>> versioneds = routedStore.get(aKey, null);
        assertEquals(2, versioneds.size());
    }

    @Test
    public void testPutDeleteZoneRouting() throws Exception {
        cluster = VoldemortTestConstants.getEightNodeClusterWithZones();

        HashMap<Integer, Integer> zoneReplicationFactor = Maps.newHashMap();
        zoneReplicationFactor.put(0, 2);
        zoneReplicationFactor.put(1, 2);

        Versioned<byte[]> versioned = new Versioned<byte[]>(new byte[] { 1 });

        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        Set<Integer> sleepy = Sets.newHashSet(4, 5, 6, 7);
        for(Node n: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> subStore = null;

            if(sleepy != null && sleepy.contains(n.getId()))
                subStore = new SleepyStore<ByteArray, byte[], byte[]>(SLEEPY_TIME,
                                                                      new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            else
                subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            subStores.put(n.getId(), subStore);
        }

        setFailureDetector(subStores);
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               1,
                                                               1,
                                                               1,
                                                               1,
                                                               0,
                                                               0,
                                                               zoneReplicationFactor,
                                                               HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                               RoutingStrategyType.ZONE_STRATEGY);
        routedStoreThreadPool = Executors.newFixedThreadPool(8);
        RoutedStoreFactory routedStoreFactory = createFactory();

        Store<ByteArray, byte[], byte[]> s1 = routedStoreFactory.create(cluster,
                                                                        storeDef,
                                                                        subStores,
                                                                        failureDetector,
                                                                        createConfig(OPERATION_TIMEOUT));

        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                             cluster);

        List<Node> nodesRoutedTo = routingStrategy.routeRequest("test".getBytes());
        long start = System.nanoTime(), elapsed;
        try {
            s1.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        Thread.sleep(SLEEPY_TIME - elapsed);

        for(Node node: nodesRoutedTo) {
            assertEquals(subStores.get(node.getId())
                                  .get(new ByteArray("test".getBytes()), null)
                                  .get(0), versioned);
        }

        // make sure the failure detector adds back any previously failed nodes
        Thread.sleep(BANNAGE_PERIOD + 100);
        start = System.nanoTime();
        try {
            s1.delete(new ByteArray("test".getBytes()), versioned.getVersion());
        } finally {
            elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + SLEEPY_TIME, elapsed < SLEEPY_TIME);
        }

        Thread.sleep(SLEEPY_TIME - elapsed);

        for(Node node: nodesRoutedTo) {
            assertEquals(subStores.get(node.getId())
                                  .get(new ByteArray("test".getBytes()), null)
                                  .size(), 0);
        }

    }

    private void assertOperationalNodes(int expected) {
        int found = 0;
        for(Node n: cluster.getNodes())
            if(failureDetector.isAvailable(n))
                found++;
        assertEquals("Number of operational nodes not what was expected.", expected, found);
    }

    /**
     * Function to set the failure detector class.
     * 
     * Note: We set this to BannagePeriodFailureDetector which is not supported
     * or recommended anymore because of the inefficiency in checking for
     * liveness and the potential issue of marking all the nodes as unavailable.
     * However, for the purpose of these tests, BannagePeriodFailureDetector is
     * useful to deterministically set a node as available or unavailable.
     * 
     * @param subStores Stores used to check if node is up
     * @throws Exception
     */
    private void setFailureDetector(Map<Integer, Store<ByteArray, byte[], byte[]>> subStores)
            throws Exception {
        // Destroy any previous failure detector before creating the next one
        // (the final one is destroyed in tearDown).
        if(failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(BannagePeriodFailureDetector.class.getName())
                                                                                 .setBannagePeriod(BANNAGE_PERIOD)
                                                                                 .setCluster(cluster)
                                                                                 .setStoreVerifier(create(subStores));
        failureDetector = create(failureDetectorConfig, false);
    }
}
