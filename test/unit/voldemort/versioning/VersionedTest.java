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

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import voldemort.TestUtils;

public class VersionedTest extends TestCase {

    private Versioned<Integer> getVersioned(Integer value, int... versionIncrements) {
        return new Versioned<Integer>(value, TestUtils.getClock(versionIncrements));
    }

    public void mustHaveVersion() {
        try {
            new Versioned<Integer>(1, null);
            fail("Successfully created Versioned with null version.");
        } catch(NullPointerException e) {
            // this is good
        }
    }

    public void testEquals() {
        assertEquals("Null versioneds not equal.", getVersioned(null), getVersioned(null));
        assertEquals("equal versioneds not equal.", getVersioned(1), getVersioned(1));
        assertEquals("equal versioneds not equal.", getVersioned(1, 1, 2), getVersioned(1, 1, 2));

        assertTrue("Equals values with different version are equal!",
                   !getVersioned(1, 1, 2).equals(getVersioned(1, 1, 2, 2)));
        assertTrue("Different values with same version are equal!",
                   !getVersioned(1, 1, 2).equals(getVersioned(2, 1, 2)));
        assertTrue("Different values with different version are equal!",
                   !getVersioned(1, 1, 2).equals(getVersioned(2, 1, 1, 2)));

        // Should work for array types too!
        assertEquals("Equal arrays are not equal!",
                     new Versioned<byte[]>(new byte[] { 1 }),
                     new Versioned<byte[]>(new byte[] { 1 }));
    }

    public void testClone() {
        Versioned<Integer> v1 = getVersioned(2, 1, 2, 3);
        Versioned<Integer> v2 = v1.cloneVersioned();
        assertEquals(v1, v2);
        assertTrue(v1 != v2);
        assertTrue(v1.getVersion() != v2.getVersion());
        ((VectorClock) v2.getVersion()).incrementVersion(1, System.currentTimeMillis());
        assertTrue(!v1.equals(v2));
    }

    public void testListByteArray() {
        byte[] array1 = new byte[] { 1, 2, 3 };
        byte[] array2 = new byte[] { 1, 2, 3 };

        Version clock1 = TestUtils.getClock(1, 2, 3);
        Version clock2 = TestUtils.getClock(1, 2, 3);

        Versioned<byte[]> v1 = new Versioned<byte[]>(array1, clock1);
        Versioned<byte[]> v2 = new Versioned<byte[]>(array2, clock2);

        List<Versioned<byte[]>> versioned1 = new ArrayList<Versioned<byte[]>>();
        versioned1.add(v1);

        List<Versioned<byte[]>> versioned2 = new ArrayList<Versioned<byte[]>>();
        versioned2.add(v2);

        assertEquals(versioned1.indexOf(v2), 0);
        assertEquals(versioned2.indexOf(v1), 0);

        assertEquals(versioned1, versioned2);
    }
}
