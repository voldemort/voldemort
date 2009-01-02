package voldemort.versioning;

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

}
