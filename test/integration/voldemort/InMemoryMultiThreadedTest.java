package voldemort;

import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.store.versioned.VersionIncrementingStore;
import voldemort.utils.SystemTime;
import voldemort.versioning.VectorClockInconsistencyResolver;

/**
 * Test the InMemoryStorageEngine with threads
 * 
 * @author jay
 * 
 */
public class InMemoryMultiThreadedTest {

    public static void main(String[] args) throws Exception {
        Store<byte[], byte[]> store = new VersionIncrementingStore<byte[], byte[]>(new InMemoryStorageEngine<byte[], byte[]>("test"),
                                                                                   0,
                                                                                   new SystemTime());
        store = new InconsistencyResolvingStore<byte[], byte[]>(store,
                                                                new VectorClockInconsistencyResolver<byte[]>());
        new MultithreadedStressTest(store, 5, 1000000, 10).testGetAndPut();
    }

}
