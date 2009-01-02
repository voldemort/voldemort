package voldemort.versioning;

import junit.framework.TestCase;


/**
 * @author jay
 *
 */
public class ClockEntryTest extends TestCase {

	public void testEquality() {
		ClockEntry v1 = new ClockEntry((short) 0, (short) 1);
		ClockEntry v2 = new ClockEntry((short) 0, (short) 1);
		assertTrue(v1.equals(v1));
		assertTrue(!v1.equals(null));
		assertEquals(v1, v2);
		
		v1 = new ClockEntry((short) 0, (short) 1);
		v2 = new ClockEntry((short) 0, (short) 2);
		assertTrue(!v1.equals(v2));

		v1 = new ClockEntry((short) Short.MAX_VALUE, (short) 256);
		v2 = new ClockEntry((short) Short.MAX_VALUE, (short) 256);
		assertEquals(v1, v2);
	}
	
	public void testIncrement() {
		ClockEntry v = new ClockEntry((short) 0, (short) 1);
		assertEquals(v.getNodeId(), 0);
		assertEquals(v.getVersion(), 1);
		ClockEntry v2 = v.incremented();
		assertEquals(v.getVersion(), 1);
		assertEquals(v2.getVersion(), 2);
	}
	
}
