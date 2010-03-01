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

package voldemort.versioning;

import static voldemort.TestUtils.getClock;
import junit.framework.TestCase;
import voldemort.TestUtils;

import com.google.common.collect.Lists;

/**
 * VectorClock tests
 * 
 * 
 */
public class VectorClockTest extends TestCase {

    public void testEqualsAndHashcode() {
        VectorClock one = getClock(1, 2);
        VectorClock other = getClock(1, 2);
        assertEquals(one, other);
        assertEquals(one.hashCode(), other.hashCode());
    }

    public void testComparisons() {
        assertTrue("The empty clock should not happen before itself.",
                   getClock().compare(getClock()) != Occured.CONCURRENTLY);
        assertTrue("A clock should not happen before an identical clock.",
                   getClock(1, 1, 2).compare(getClock(1, 1, 2)) != Occured.CONCURRENTLY);
        assertTrue(" A clock should happen before an identical clock with a single additional event.",
                   getClock(1, 1, 2).compare(getClock(1, 1, 2, 3)) == Occured.BEFORE);
        assertTrue("Clocks with different events should be concurrent.",
                   getClock(1).compare(getClock(2)) == Occured.CONCURRENTLY);
        assertTrue("Clocks with different events should be concurrent.",
                   getClock(1, 1, 2).compare(getClock(1, 1, 3)) == Occured.CONCURRENTLY);
        assertTrue(getClock(2, 2).compare(getClock(1, 2, 2, 3)) == Occured.BEFORE
                   && getClock(1, 2, 2, 3).compare(getClock(2, 2)) == Occured.AFTER);
    }

    public void testMerge() {
        // merging two clocks should create a clock contain the element-wise
        // maximums
        assertEquals("Two empty clocks merge to an empty clock.",
                     getClock().merge(getClock()),
                     getClock());
        assertEquals("Merge of a clock with itself does nothing",
                     getClock(1).merge(getClock(1)),
                     getClock(1));
        assertEquals(getClock(1).merge(getClock(2)), getClock(1, 2));
        assertEquals(getClock(1).merge(getClock(1, 2)), getClock(1, 2));
        assertEquals(getClock(1, 2).merge(getClock(1)), getClock(1, 2));
        assertEquals("Two-way merge fails.",
                     getClock(1, 1, 1, 2, 3, 5).merge(getClock(1, 2, 2, 4)),
                     getClock(1, 1, 1, 2, 2, 3, 4, 5));
        assertEquals(getClock(2, 3, 5).merge(getClock(1, 2, 2, 4, 7)),
                     getClock(1, 2, 2, 3, 4, 5, 7));
    }

    /**
     * See gihub issue #25: Incorrect coersion of version to short before
     * passing to ClockEntry constructor
     */
    public void testMergeWithLargeVersion() {
        VectorClock clock1 = getClock(1);
        VectorClock clock2 = new VectorClock(Lists.newArrayList(new ClockEntry((short) 1,
                                                                               Short.MAX_VALUE + 1)),
                                             System.currentTimeMillis());
        VectorClock mergedClock = clock1.merge(clock2);
        assertEquals(mergedClock.getMaxVersion(), Short.MAX_VALUE + 1);
    }

    public void testSerialization() {
        assertEquals("The empty clock serializes incorrectly.",
                     getClock(),
                     new VectorClock(getClock().toBytes()));
        VectorClock clock = getClock(1, 1, 2, 3, 4, 4, 6);
        assertEquals("This clock does not serialize to itself.",
                     clock,
                     new VectorClock(clock.toBytes()));
    }

    public void testSerializationWraps() {
        VectorClock clock = getClock(1, 1, 2, 3, 3, 6);
        for(int i = 0; i < 300; i++)
            clock.incrementVersion(2, System.currentTimeMillis());
        assertEquals("Clock does not serialize to itself.", clock, new VectorClock(clock.toBytes()));
    }

    public void testIncrementOrderDoesntMatter() {
        // Clocks should have the property that no matter what order the
        // increment operations are done in the resulting clocks are equal
        int numTests = 10;
        int numNodes = 10;
        int numValues = 100;
        VectorClock[] clocks = new VectorClock[numNodes];
        for(int t = 0; t < numTests; t++) {
            int[] test = TestUtils.randomInts(numNodes, numValues);
            for(int n = 0; n < numNodes; n++)
                clocks[n] = getClock(TestUtils.shuffle(test));
            // test all are equal
            for(int n = 0; n < numNodes - 1; n++)
                assertEquals("Clock " + n + " and " + (n + 1) + " are not equal.",
                             clocks[n],
                             clocks[n + 1]);
        }
    }

    public void testIncrementAndSerialize() {
        int node = 1;
        VectorClock vc = getClock(node);
        assertEquals(node, vc.getMaxVersion());
        int increments = 3000;
        for(int i = 0; i < increments; i++) {
            vc.incrementVersion(node, 45);
            // serialize
            vc = new VectorClock(vc.toBytes());
        }
        assertEquals(increments + 1, vc.getMaxVersion());
    }

}
