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
import static voldemort.MutableStoreResolver.createFailureDetector;
import static voldemort.MutableStoreResolver.recordException;
import static voldemort.MutableStoreResolver.recordSuccess;
import static voldemort.TestUtils.getClock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author jay
 * 
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class ReadRepairerTest extends TestCase {

    private ReadRepairer<String, Integer> repairer = new ReadRepairer<String, Integer>();
    private List<NodeValue<String, Integer>> empty = new ArrayList<NodeValue<String, Integer>>();
    private Random random = new Random(1456);

    private Time time = new MockTime();
    private final Class<FailureDetector> failureDetectorClass;
    private FailureDetector failureDetector;

    public ReadRepairerTest(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class } });
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
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        for(int a = 0; a < 3; a++) {
            subStores.put(Iterables.get(cluster.getNodes(), a).getId(),
                          new InMemoryStorageEngine<ByteArray, byte[]>("test"));
        }

        failureDetector = createFailureDetector(failureDetectorClass,
                                                cluster.getNodes(),
                                                subStores,
                                                time,
                                                1000L);

        RoutedStore store = new RoutedStore("test",
                                            subStores,
                                            cluster,
                                            storeDef,
                                            1,
                                            true,
                                            1000L,
                                            failureDetector);

        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        store.put(key, new Versioned<byte[]>(value));
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        time.sleep(2000);

        assertEquals(2, store.get(key).size());
        // Last get should have repaired the missing key from node 0 so all
        // stores should now return a value
        assertEquals(3, store.get(key).size());

        ByteArray anotherKey = TestUtils.toByteArray("anotherKey");
        // Try again, now use getAll to read repair
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        store.put(anotherKey, new Versioned<byte[]>(value));
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        assertEquals(2, store.getAll(Arrays.asList(anotherKey)).get(anotherKey).size());
        assertEquals(3, store.get(anotherKey).size());
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
                              asList(getValue(1, 1, new int[] { 1 }), getValue(2, 1, new int[] { 1,
                                      1 })));
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
                              asList(getValue(1, 1, new int[] { 3, 3 }), getValue(2, 1, new int[] {
                                      1, 2 }), getValue(3, 1, new int[] { 1, 3, 3 })));
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

    private NodeValue<String, Integer> getValue(int nodeId, int value, int[] version) {
        return new NodeValue<String, Integer>(nodeId,
                                              Integer.toString(value),
                                              new Versioned<Integer>(value, getClock(version)));
    }

}
