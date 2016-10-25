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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static voldemort.TestUtils.getClock;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.junit.Test;

import voldemort.TestUtils;

import com.google.common.collect.Lists;

/**
 * VectorClock tests
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class VectorClockTest {

    @Test
    public void testEqualsAndHashcode() {
        VectorClock one = getClock(1, 2);
        VectorClock other = getClock(1, 2);
        assertEquals(one, other);
        assertEquals(one.hashCode(), other.hashCode());
    }

    @Test
    public void testComparisons() {
        assertTrue("The empty clock should not happen before itself.",
                   getClock().compare(getClock()) != Occurred.CONCURRENTLY);
        assertTrue("A clock should not happen before an identical clock.",
                   getClock(1, 1, 2).compare(getClock(1, 1, 2)) != Occurred.CONCURRENTLY);
        assertTrue(" A clock should happen before an identical clock with a single additional event.",
                   getClock(1, 1, 2).compare(getClock(1, 1, 2, 3)) == Occurred.BEFORE);
        assertTrue("Clocks with different events should be concurrent.",
                   getClock(1).compare(getClock(2)) == Occurred.CONCURRENTLY);
        assertTrue("Clocks with different events should be concurrent.",
                   getClock(1, 1, 2).compare(getClock(1, 1, 3)) == Occurred.CONCURRENTLY);
        assertTrue("Clocks with different events should be concurrent.",
                   getClock(1, 2, 3, 3).compare(getClock(1, 1, 2, 3)) == Occurred.CONCURRENTLY);
        assertTrue(getClock(2, 2).compare(getClock(1, 2, 2, 3)) == Occurred.BEFORE
                   && getClock(1, 2, 2, 3).compare(getClock(2, 2)) == Occurred.AFTER);
    }

    @Test
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
     * See github issue #25: Incorrect coercion of version to short before
     * passing to ClockEntry constructor
     */
    @Test
    public void testMergeWithLargeVersion() {
        VectorClock clock1 = getClock(1);
        VectorClock clock2 = new VectorClock(Lists.newArrayList(new ClockEntry((short) 1,
                                                                               Short.MAX_VALUE + 1)),
                                             System.currentTimeMillis());
        VectorClock mergedClock = clock1.merge(clock2);
        assertEquals(mergedClock.getMaxVersion(), Short.MAX_VALUE + 1);
    }

    @Test
    public void testSerialization() {
        assertEquals("The empty clock serializes incorrectly.",
                     getClock(),
                     new VectorClock(getClock().toBytes()));
        VectorClock clock = getClock(1, 1, 2, 3, 4, 4, 6);
        assertEquals("This clock does not serialize to itself.",
                     clock,
                     new VectorClock(clock.toBytes()));
    }

    @Test
    public void testSerializationBackwardCompatibility() {
        assertEquals("The empty clock serializes incorrectly.",
                     getClock(),
                     new VectorClock(getClock().toBytes()));
        VectorClock clock = getClock(1, 1, 2, 3, 4, 4, 6);
        // Old Vector Clock would serialize to this:
        // 0 5 1 0 1 2 0 2 1 0 3 1 0 4 2 0 6 1 [timestamp]
        byte[] knownSerializedHead = { 0, 5, 1, 0, 1, 2, 0, 2, 1, 0, 3, 1, 0, 4, 2, 0, 6, 1 };
        byte[] serialized = clock.toBytes();
        for(int index = 0; index < knownSerializedHead.length; index++) {
            assertEquals("byte at index " + index + " is not equal",
                         knownSerializedHead[index],
                         serialized[index]);
        }

    }

    /**
     * Pre-condition: timestamp is ignored in determine vector clock equality
     */
    @Test
    public void testDeserializationBackwardCompatibility() {
        assertEquals("The empty clock serializes incorrectly.",
                     getClock(),
                     new VectorClock(getClock().toBytes()));
        VectorClock clock = getClock(1, 1, 2, 3, 4, 4, 6);
        // Old Vector Clock would serialize to this:
        // 0 5; 1; 0 1, 2; 0 2, 1; 0 3, 1; 0 4, 2; 0 6, 1; [timestamp=random]
        byte[] knownSerialized = { 0, 5, 1, 0, 1, 2, 0, 2, 1, 0, 3, 1, 0, 4, 2, 0, 6, 1, 0, 0, 1,
                0x3e, 0x7b, (byte) 0x8c, (byte) 0x9d, 0x19 };
        assertEquals("vector clock does not deserialize correctly on given byte array",
                     clock,
                     new VectorClock(knownSerialized));
        
        DataInputStream ds = new DataInputStream(new ByteArrayInputStream(knownSerialized));
        VectorClock clock2 = VectorClock.createVectorClock(ds);
        assertEquals("vector clock does not deserialize correctly on given input stream",
                     clock,
                     clock2);
    }

    @Test
    public void testSerializationWraps() {
        VectorClock clock = getClock(1, 1, 2, 3, 3, 6);
        for(int i = 0; i < 300; i++)
            clock.incrementVersion(2, System.currentTimeMillis());
        assertEquals("Clock does not serialize to itself.", clock, new VectorClock(clock.toBytes()));
    }

    @Test
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

    @Test
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

    /**
     * A test for comparing vector clocks that nodes of clock entries are not
     * sorted In case people insert clock entries without using increment we
     * need to test although it has been deprecated
     */
    @Test
    public void testNodeClockEntryDeprecate() {
        VectorClock vc1 = new VectorClock();
        try {
            vc1.getEntries().add(new ClockEntry((short) 2, 2));
            fail("Did not throw UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {

        }
    }

    @Test
    public void testVersion0NotAcceptable() {
        try {
            ClockEntry clockEntry = new ClockEntry();
            clockEntry.setVersion(0);
            fail("Did not throw IllegalArgumentException");
        } catch(IllegalArgumentException e) {}
    }

    @Test
    public void testNodeLess0NotAcceptable() {
        try {
            ClockEntry clockEntry = new ClockEntry();
            clockEntry.setNodeId((short) -1);
            fail("Did not throw IllegalArgumentException");
        } catch(IllegalArgumentException e) {}
    }
}
