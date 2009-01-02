package voldemort.store.memory;

import java.util.ArrayList;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineTest;

public class InMemoryStorageEngineTest extends StorageEngineTest {

	private InMemoryStorageEngine<byte[],byte[]> store;
	
	@Override
	public StorageEngine<byte[],byte[]> getStorageEngine() {
		return store;
	}
	
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.store = new InMemoryStorageEngine<byte[],byte[]>("test");
	}

    @Override
    public List<byte[]> getKeys(int numKeys) {
        List<byte[]> keys = new ArrayList<byte[]>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(TestUtils.randomBytes(10));
        return keys;
    }

}
