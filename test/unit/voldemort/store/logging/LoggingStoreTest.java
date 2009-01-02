package voldemort.store.logging;

import java.util.List;

import voldemort.store.BasicStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;

/**
 * @author jay
 *
 */
public class LoggingStoreTest extends BasicStoreTest<String, String> {

    @Override
    public List<String> getKeys(int numKeys) {
        return getStrings(numKeys, 8);
    }

    @Override
    public Store<String, String> getStore() {
        return new LoggingStore<String,String>(new InMemoryStorageEngine<String,String>("test-store"));
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 8);
    }

}
