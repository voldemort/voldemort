package voldemort.store.serialized;

import java.util.List;

import voldemort.serialization.StringSerializer;
import voldemort.store.BasicStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;

/**
 * @author jay
 *
 */
public class SerializingStoreTest extends BasicStoreTest<String, String> {

    @Override
    public List<String> getKeys(int numKeys) {
        return getStrings(numKeys, 10);
    }

    @Override
    public Store<String, String> getStore() {
        return new SerializingStore<String, String>(
                new InMemoryStorageEngine<byte[], byte[]>("test"), 
                new StringSerializer(), 
                new StringSerializer());
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 12);
    }

}
