/*
 * Copyright 2008-2009 LinkedIn, Inc
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
import static voldemort.MutableStoreVerifier.create;
import static voldemort.TestUtils.getClock;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
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
 * 
 */
@RunWith(Parameterized.class)
public class RoutedStoreTest extends AbstractByteArrayStoreTest {

    public static final int BANNAGE_PERIOD = 1000;
    public static final int SLEEPY_TIME = 81;
    public static final int OPERATION_TIMEOUT = 60;

    private Cluster cluster;
    private final ByteArray aKey = TestUtils.toByteArray("jay");
    private final byte[] aValue = "kreps".getBytes();
    private final byte[] aTransform = "transform".getBytes();
    private final Class<FailureDetector> failureDetectorClass;
    private final boolean isPipelineRoutedStoreEnabled;
    private FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;

    public RoutedStoreTest(Class<FailureDetector> failureDetectorClass,
                           boolean isPipelineRoutedStoreEnabled) {
        this.failureDetectorClass = failureDetectorClass;
        this.isPipelineRoutedStoreEnabled = isPipelineRoutedStoreEnabled;
    }

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

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class, true },
                { BannagePeriodFailureDetector.class, false } });
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

    private RoutedStore getStore(Cluster cluster, int reads, int writes, int threads, int failing)
            throws Exception {
        return getStore(cluster,
                        reads,
                        writes,
                        threads,
                        failing,
                        0,
                        RoutingStrategyType.TO_ALL_STRATEGY,
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
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               reads + writes,
                                                               reads,
                                                               reads,
                                                               writes,
                                                               writes,
                                                               strategy);
        routedStoreThreadPool = Executors.newFixedThreadPool(threads);
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        return routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);
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
        int count = 0;
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

            count += 1;
        }

        setFailureDetector(subStores);
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(true,
                                                                       routedStoreThreadPool,
                                                                       timeOutMs);

        return routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);
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

    private void testBasicOperations(int reads, int writes, int failures, int threads)
            throws Exception {
        RoutedStore routedStore = getStore(cluster, reads, writes, threads, failures);
        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        VectorClock clock = getClock(1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);
        routedStore.put(aKey, versioned, aTransform);
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);
        List<Versioned<byte[]>> found = store.get(aKey, aTransform);
        assertEquals(1, found.size());
        assertEquals(versioned, found.get(0));
        assertNOrMoreEqual(routedStore, cluster.getNumberOfNodes() - failures, aKey, versioned);
        assertTrue(routedStore.delete(aKey, versioned.getVersion()));
        assertNEqual(routedStore, 0, aKey, versioned);
        assertTrue(!routedStore.delete(aKey, versioned.getVersion()));
    }

    @Test
    public void testBasicOperationsSingleThreaded() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 0, 1);
    }

    @Test
    public void testBasicOperationsMultiThreaded() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes(), cluster.getNumberOfNodes(), 0, 4);
    }

    @Test
    public void testBasicOperationsMultiThreadedWithFailures() throws Exception {
        testBasicOperations(cluster.getNumberOfNodes() - 2, cluster.getNumberOfNodes() - 2, 2, 4);
    }

    private void testBasicOperationFailure(int reads, int writes, int failures, int threads)
            throws Exception {
        VectorClock clock = getClock(1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(aValue, clock);
        RoutedStore routedStore = getStore(cluster,
                                           reads,
                                           writes,
                                           threads,
                                           failures,
                                           0,
                                           RoutingStrategyType.TO_ALL_STRATEGY,
                                           new UnreachableStoreException("no go"));
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
                                  4);
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
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }
        // Putting extra key to test getAll
        s1.put(new ByteArray("test2".getBytes()), versioned, null);

        start = System.nanoTime();
        try {
            s1.get(new ByteArray("test".getBytes()), null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        start = System.nanoTime();
        try {
            List<Version> versions = s1.getVersions(new ByteArray("test".getBytes()));
            for(Version version: versions) {
                assertEquals(version.compare(versioned.getVersion()), Occurred.BEFORE);
            }
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        // make sure the failure detector adds back any previously failed nodes
        Thread.sleep(BANNAGE_PERIOD * 2);
        start = System.nanoTime();
        try {
            s1.delete(new ByteArray("test".getBytes()), versioned.getVersion());
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        // make sure sleepy stores processed the delete before checking,
        // otherwise, we might be bailing
        // out of the test too early for the delete to be processed.
        Thread.sleep(SLEEPY_TIME);
        List<ByteArray> keys = Lists.newArrayList(new ByteArray("test".getBytes()),
                                                  new ByteArray("test2".getBytes()));

        Map<ByteArray, List<Versioned<byte[]>>> values = s1.getAll(keys, null);
        List<Versioned<byte[]>> results = values.get(new ByteArray("test".getBytes()));
        assertEquals("\'test\' did not get deleted.", 0, results.size());
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
                                                       1000,
                                                       new VoldemortException());

        start = System.nanoTime();

        try {
            s2.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " > " + 81, elapsed >= 81);
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
        results = values.get(new ByteArray("test".getBytes()));
        assertEquals("\'test\' did not get deleted.", 0, results.size());
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
                                                       81,
                                                       1000,
                                                       new VoldemortException());

        start = System.nanoTime();
        try {
            s3.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
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
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        start = System.nanoTime();
        try {
            s3.get(new ByteArray("test".getBytes()), null);
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        start = System.nanoTime();
        try {
            s3.delete(new ByteArray("test".getBytes()), versioned.getVersion());
        } finally {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
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
                                                       81,
                                                       1000,
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

    /**
     * Tests that getAll works correctly with a node down in a two node cluster.
     */
    @Test
    public void testGetAllWithNodeDown() throws Exception {
        cluster = VoldemortTestConstants.getTwoNodeCluster();

        RoutedStore routedStore = getStore(cluster, 1, 2, 1, 0);
        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());

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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

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

        RoutedStore routedStore = getStore(cluster,
                                           cluster.getNumberOfNodes() - 1,
                                           cluster.getNumberOfNodes() - 1,
                                           1,
                                           0);
        // Disable node 1 so that the first put also goes to the last node
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 1));
        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, new Versioned<byte[]>(aValue), null);

        byte[] anotherValue = "john".getBytes();

        // Disable the last node and enable node 1 to prevent the last node from
        // getting the new version
        recordException(failureDetector, Iterables.getLast(cluster.getNodes()));
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 1));
        VectorClock clock = getClock(1);
        store.put(aKey, new Versioned<byte[]>(anotherValue, clock), null);

        // Enable last node and disable node 1, the following get should cause a
        // read repair on the last node in the code path that is only executed
        // if there are failures.
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 1));
        recordSuccess(failureDetector, Iterables.getLast(cluster.getNodes()));
        List<Versioned<byte[]>> versioneds = store.get(aKey, null);
        assertEquals(1, versioneds.size());
        assertEquals(new ByteArray(anotherValue), new ByteArray(versioneds.get(0).getValue()));

        // Read repairs are done asynchronously, so we sleep for a short period.
        // It may be a good idea to use a synchronous executor service.
        Thread.sleep(100);
        for(Store<ByteArray, byte[], byte[]> innerStore: routedStore.getInnerStores().values()) {
            List<Versioned<byte[]>> innerVersioneds = innerStore.get(aKey, null);
            assertEquals(1, versioneds.size());
            assertEquals(new ByteArray(anotherValue), new ByteArray(innerVersioneds.get(0)
                                                                                   .getValue()));
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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        Store<ByteArray, byte[], byte[]> store = new InconsistencyResolvingStore<ByteArray, byte[], byte[]>(routedStore,
                                                                                                            new VectorClockInconsistencyResolver<byte[]>());
        store.put(aKey, new Versioned<byte[]>(aValue), aTransform);
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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       timeout);

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            true,
                                                            failureDetector);

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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(true,
                                                                       routedStoreThreadPool,
                                                                       timeout);

        RoutedStore routedStore = routedStoreFactory.create(new Cluster("test", nodes),
                                                            definition,
                                                            stores,
                                                            true,
                                                            failureDetector);

        long start = System.nanoTime();
        try {
            routedStore.get(new ByteArray("test".getBytes()), null);
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       1000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       10000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        routedStore.put(aKey, Versioned.value(aValue), null);

        routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                    routedStoreThreadPool,
                                                    sleepTimeMs / 2);

        routedStore = routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);

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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       10000L);

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        routedStore.put(aKey, Versioned.value(aValue), null);

        routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                    routedStoreThreadPool,
                                                    sleepTimeMs / 2);

        routedStore = routedStoreFactory.create(cluster, storeDef, subStores, true, failureDetector);

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
        int count = 0;
        for(Node n: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> subStore = null;

            if(sleepy != null && sleepy.contains(n.getId()))
                subStore = new SleepyStore<ByteArray, byte[], byte[]>(81,
                                                                      new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"));
            else
                subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

            subStores.put(n.getId(), subStore);

            count += 1;
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
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(true,
                                                                       routedStoreThreadPool,
                                                                       60);

        Store<ByteArray, byte[], byte[]> s1 = routedStoreFactory.create(cluster,
                                                                        storeDef,
                                                                        subStores,
                                                                        true,
                                                                        failureDetector);

        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                             cluster);

        List<Node> nodesRoutedTo = routingStrategy.routeRequest("test".getBytes());
        long start = System.nanoTime(), elapsed;
        try {
            s1.put(new ByteArray("test".getBytes()), versioned, null);
        } finally {
            elapsed = (System.nanoTime() - start) / Time.NS_PER_MS;
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        Thread.sleep(81 - elapsed);

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
            assertTrue(elapsed + " < " + 81, elapsed < 81);
        }

        Thread.sleep(81 - elapsed);

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

    private void setFailureDetector(Map<Integer, Store<ByteArray, byte[], byte[]>> subStores)
            throws Exception {
        // Destroy any previous failure detector before creating the next one
        // (the final one is destroyed in tearDown).
        if(failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(BANNAGE_PERIOD)
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setStoreVerifier(create(subStores));
        failureDetector = create(failureDetectorConfig, false);
    }
}
