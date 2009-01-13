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

/**
 * VectorClock tests
 * 
 * @author jay
 * 
 */
public class VectorClockTest extends TestCase {

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
        assertEquals("Two empty clocks merge to an empty clock.",
                     getClock().merge(getClock()),
                     getClock());
        assertEquals("Merging two clocks should be the same as taking the max of their event counts.",
                     getClock(1).merge(getClock(2)),
                     getClock(1, 2));
        assertEquals("Merging two clocks should be the same as taking the max of their event counts.",
                     getClock(1, 1, 1, 2, 3).merge(getClock(1, 2, 2, 4)),
                     getClock(1, 1, 1, 2, 2, 3, 4));
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

}
