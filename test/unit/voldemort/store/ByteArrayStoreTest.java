package voldemort.store;

import java.util.List;

import voldemort.TestUtils;
import voldemort.versioning.Versioned;

/**
 * @author jay
 *
 */
public abstract class ByteArrayStoreTest extends BasicStoreTest<byte[],byte[]> {

    public List<byte[]> getKeys(int numValues) {
        return this.getByteValues(numValues, 8);
    }
    
	@Override
	public List<byte[]> getValues(int numValues) {
		return this.getByteValues(numValues, 10);
	}

	@Override
	protected boolean valuesEqual(byte[] t1, byte[] t2) {
		return TestUtils.bytesEqual(t1, t2);
	}
	
	public void testEmptyByteArray() {
	    Store<byte[],byte[]> store = getStore();
	    Versioned<byte[]> bytes = new Versioned<byte[]>(new byte[0]);
	    store.put(new byte[0], bytes);
	    List<Versioned<byte[]>> found = store.get(new byte[0]);
	    assertEquals("Incorrect number of results.", 1, found.size());
	    assertEquals("Get doesn't equal put.", bytes, found.get(0));
	}

}
