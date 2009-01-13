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
import static voldemort.TestUtils.getClock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;
import voldemort.versioning.Versioned;

import com.google.common.collect.Sets;

/**
 * @author jay
 * 
 */
@SuppressWarnings("unchecked")
public class ReadRepairerTest extends TestCase {

    private ReadRepairer<String, Integer> repairer = new ReadRepairer<String, Integer>();
    private List<NodeValue<String, Integer>> empty = new ArrayList<NodeValue<String, Integer>>();
    private Random random = new Random(1456);

    public void testEmptyList() throws Exception {
        assertEquals(empty, repairer.getRepairs(empty));
    }

    public void testSingleValue() throws Exception {
        assertEquals(empty, repairer.getRepairs(asList(getValue(1, 1, new int[] { 1 }))));
    }

    public void testAllEqual() throws Exception {
        List<NodeValue<String, Integer>> values = asList(getValue(1, 1, new int[] { 1 }),
                                                         getValue(2, 1, new int[] { 1 }),
                                                         getValue(3, 1, new int[] { 1 }));
        assertEquals(empty, repairer.getRepairs(values));
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
