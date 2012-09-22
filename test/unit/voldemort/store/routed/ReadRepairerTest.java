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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.FailureDetectorTestUtils.recordSuccess;
import static voldemort.TestUtils.getClock;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;
import static voldemort.cluster.failuredetector.MutableStoreVerifier.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class ReadRepairerTest {

    private ReadRepairer<String, Integer> repairer = new ReadRepairer<String, Integer>();
    private List<NodeValue<String, Integer>> empty = new ArrayList<NodeValue<String, Integer>>();
    private Random random = new Random(1456);

    private Time time = new MockTime();
    private final Class<FailureDetector> failureDetectorClass;
    private final boolean isPipelineRoutedStoreEnabled;
    private FailureDetector failureDetector;
    private ExecutorService routedStoreThreadPool;

    public ReadRepairerTest(Class<FailureDetector> failureDetectorClass,
                            boolean isPipelineRoutedStoreEnabled) {
        this.failureDetectorClass = failureDetectorClass;
        this.isPipelineRoutedStoreEnabled = isPipelineRoutedStoreEnabled;
    }

    @After
    public void tearDown() throws Exception {
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

    @Test
    public void testEmptyList() throws Exception {
        assertEquals(empty, repairer.getRepairs(empty));
    }

    @Test
    public void testSingleValue() throws Exception {
        assertEquals(empty, repairer.getRepairs(asList(getValue(1, 1, new int[] { 1 }))));
    }

    @Test
    public void testAllEqual() throws Exception {
        List<NodeValue<String, Integer>> values = asList(getValue(1, 1, new int[] { 1 }),
                                                         getValue(2, 1, new int[] { 1 }),
                                                         getValue(3, 1, new int[] { 1 }));
        assertEquals(empty, repairer.getRepairs(values));
    }

    /**
     * See Issue 150: Missing keys are not added to node when performing
     * read-repair
     */

    @Test
    public void testMissingKeysAreAddedToNodeWhenDoingReadRepair() throws Exception {
        ByteArray key = TestUtils.toByteArray("key");
        byte[] value = "foo".getBytes();

        Cluster cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               3,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();

        for(int a = 0; a < 3; a++) {
            int id = Iterables.get(cluster.getNodes(), a).getId();
            InMemoryStorageEngine<ByteArray, byte[], byte[]> subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");
            subStores.put(id, subStore);
        }

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(1000)
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setStoreVerifier(create(subStores))
                                                                                 .setTime(time);

        failureDetector = create(failureDetectorConfig, false);

        routedStoreThreadPool = Executors.newFixedThreadPool(1);

        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(isPipelineRoutedStoreEnabled,
                                                                       routedStoreThreadPool,
                                                                       new TimeoutConfig(1000L,
                                                                                         false,
                                                                                         false));

        RoutedStore store = routedStoreFactory.create(cluster,
                                                      storeDef,
                                                      subStores,
                                                      true,
                                                      failureDetector);

        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        store.put(key, new Versioned<byte[]>(value), null);
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        time.sleep(2000);

        assertEquals(2, store.get(key, null).size());
        // Last get should have repaired the missing key from node 0 so all
        // stores should now return a value
        assertEquals(3, store.get(key, null).size());

        ByteArray anotherKey = TestUtils.toByteArray("anotherKey");
        // Try again, now use getAll to read repair
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        store.put(anotherKey, new Versioned<byte[]>(value), null);
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        assertEquals(2, store.getAll(Arrays.asList(anotherKey), null).get(anotherKey).size());
        assertEquals(3, store.get(anotherKey, null).size());
    }

    /**
     * See Issue 92: ReadRepairer.getRepairs should not return duplicates.
     */
    public void testNoDuplicates() throws Exception {
        List<NodeValue<String, Integer>> values = asList(getValue(1, 1, new int[] { 1, 2 }),
                                                         getValue(2, 1, new int[] { 1, 2 }),
                                                         getValue(3, 1, new int[] { 1 }));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(values);
        assertEquals(1, repairs.size());
        assertEquals(getValue(3, 1, new int[] { 1, 2 }), repairs.get(0));
    }

    public void testSingleSuccessor() throws Exception {
        assertVariationsEqual(singletonList(getValue(1, 1, new int[] { 1, 1 })),
                              asList(getValue(1, 1, new int[] { 1 }),
                                     getValue(2, 1, new int[] { 1, 1 })));
    }

    public void testAllConcurrent() throws Exception {
        assertVariationsEqual(asList(getValue(1, 1, new int[] { 2 }),
                                     getValue(1, 1, new int[] { 3 }),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(2, 1, new int[] { 3 }),
                                     getValue(3, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })),
                              asList(getValue(1, 1, new int[] { 1 }),
                                     getValue(2, 1, new int[] { 2 }),
                                     getValue(3, 1, new int[] { 3 })));
    }

    public void testTwoAncestorsToOneSuccessor() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual(asList(getValue(2, 1, expected), getValue(3, 1, expected)),
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })));
    }

    public void testOneAcestorToTwoSuccessors() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual(asList(getValue(2, 1, expected), getValue(3, 1, expected)),
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })));
    }

    public void testEqualObsoleteVersions() throws Exception {
        int[] expected = new int[] { 1, 1 };
        assertVariationsEqual(asList(getValue(1, 1, expected),
                                     getValue(2, 1, expected),
                                     getValue(3, 1, expected)),
                              asList(getValue(1, 1, new int[] {}),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 1 }),
                                     getValue(4, 1, expected)));
    }

    public void testDiamondPattern() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual(asList(getValue(1, 1, expected),
                                     getValue(2, 1, expected),
                                     getValue(3, 1, expected)),
                              asList(getValue(1, 1, new int[] {}),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 }),
                                     getValue(4, 1, expected)));
    }

    public void testConcurrentToOneDoesNotImplyConcurrentToAll() throws Exception {
        assertVariationsEqual(asList(getValue(1, 1, new int[] { 1, 3, 3 }),
                                     getValue(1, 1, new int[] { 1, 2 }),
                                     getValue(2, 1, new int[] { 1, 3, 3 }),
                                     getValue(3, 1, new int[] { 1, 2 })),
                              asList(getValue(1, 1, new int[] { 3, 3 }),
                                     getValue(2, 1, new int[] { 1, 2 }),
                                     getValue(3, 1, new int[] { 1, 3, 3 })));
    }

    public void testLotsOfVersions() throws Exception {
        assertVariationsEqual(asList(getValue(1, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(1, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(2, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(2, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(3, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(3, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(4, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(5, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(6, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(6, 1, new int[] { 1, 2, 3, 3 })),
                              asList(getValue(1, 1, new int[] { 1, 3 }),
                                     getValue(2, 1, new int[] { 1, 2 }),
                                     getValue(3, 1, new int[] { 2, 2 }),
                                     getValue(4, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(5, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(6, 1, new int[] { 3, 3 })));
    }

    /**
     * Test the equality with a few variations on ordering
     * 
     * @param expected List of expected values
     * @param input List of actual values
     */
    public void assertVariationsEqual(List<NodeValue<String, Integer>> expected,
                                      List<NodeValue<String, Integer>> input) {
        List<NodeValue<String, Integer>> copy = new ArrayList<NodeValue<String, Integer>>(input);
        for(int i = 0; i < Math.min(5, copy.size()); i++) {
            int j = random.nextInt(copy.size());
            int k = random.nextInt(copy.size());
            Collections.swap(copy, j, k);
            Set<NodeValue<String, Integer>> expSet = Sets.newHashSet(expected);
            List<NodeValue<String, Integer>> repairs = repairer.getRepairs(copy);
            Set<NodeValue<String, Integer>> repairSet = Sets.newHashSet(repairs);
            assertEquals("Repairs list contains duplicates on iteration" + i + ".",
                         repairs.size(),
                         repairSet.size());
            assertEquals("Expected repairs do not equal found repairs on iteration " + i + " : ",
                         expSet,
                         repairSet);
        }
    }

    /**
     * See Issue #211: Unnecessary read repairs during getAll with more than one
     * key
     */
    @Test
    public void testMultipleKeys() {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(getValue(0, 1, new int[2]));
        nodeValues.add(getValue(0, 2, new int[0]));
        nodeValues.add(getValue(1, 2, new int[0]));
        nodeValues.add(getValue(2, 1, new int[2]));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("There should be no repairs.", 0, repairs.size());
    }

    private NodeValue<String, Integer> getValue(int nodeId, int value, int[] version) {
        return new NodeValue<String, Integer>(nodeId,
                                              Integer.toString(value),
                                              new Versioned<Integer>(value, getClock(version)));
    }

}
