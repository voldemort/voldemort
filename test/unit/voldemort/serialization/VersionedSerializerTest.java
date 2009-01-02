package voldemort.serialization;

import static voldemort.TestUtils.getClock;
import junit.framework.TestCase;
import voldemort.versioning.Versioned;

public class VersionedSerializerTest extends TestCase {
	
	private VersionedSerializer<String> serializer;
	
	public void setUp() {
		this.serializer = new VersionedSerializer<String>(new StringSerializer("UTF-8"));
	}
	
	private void assertSerializes(String message, Versioned<String> obj) {
		assertEquals(obj, this.serializer.toObject(this.serializer.toBytes(obj)));
	}
	
	public void testSerialization() {
		// assertSerializes("Empty Versioned should equal itself.",
		//			     new Versioned<String>(null, getClock()));
		assertSerializes("Empty Versioned should equal itself.",
			     new Versioned<String>("", getClock()));
		assertSerializes("Normal string should equal itself.",
			     new Versioned<String>("hello", getClock()));
		assertSerializes("Normal string should equal itself.",
			     new Versioned<String>("hello", getClock(1,1,1,2,3)));
		assertSerializes("Normal string should equal itself.",
			     new Versioned<String>("hello", getClock(1)));
		assertSerializes("Normal string should equal itself.",
			     new Versioned<String>("hello", null));
	}
}
