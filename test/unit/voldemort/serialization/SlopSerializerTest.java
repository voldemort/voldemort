package voldemort.serialization;

import java.util.Date;

import junit.framework.TestCase;
import voldemort.store.slop.Slop;

public class SlopSerializerTest extends TestCase {
	
	private SlopSerializer serializer;
	
	public void setUp() {
		this.serializer = new SlopSerializer();
	}
	
	private void assertSerializes(Slop slop) {
		assertEquals(slop, this.serializer.toObject(this.serializer.toBytes(slop)));
	}
	
	public void testSerialization() {
		assertSerializes(new Slop("test", Slop.Operation.DELETE, "hello".getBytes(), new byte[]{1}, 1, new Date()));
		assertSerializes(new Slop("test", Slop.Operation.GET, "hello there".getBytes(), new byte[]{1, 3, 23,4,5,6,6}, 1, new Date(0)));
		assertSerializes(new Slop("test", Slop.Operation.PUT, "".getBytes(), new byte[]{}, 1, new Date()));
		assertSerializes(new Slop("test", Slop.Operation.PUT, "hello".getBytes(), null, 1, new Date()));
	}
}
