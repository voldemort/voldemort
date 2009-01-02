package voldemort.store.memory;

import java.util.List;

import voldemort.TestUtils;
import voldemort.store.StorageEngine;
import voldemort.versioning.Versioned;

/**
 * Does all the normal tests but also uses a high memory pressure test to make
 * sure that values are collected.
 * 
 * @author jkreps
 * 
 */
public class CacheStorageEngineTest extends InMemoryStorageEngineTest {

    private static final int NUM_OBJECTS = 1000;

    public void setUp() throws Exception {
        super.setUp();
        System.gc();
    }

    public StorageEngine<byte[], byte[]> getStorageEngine() {
        return new CacheStorageConfiguration().getStore("test");
    }

    public void testNoPressureBehavior() {
        StorageEngine<byte[], byte[]> engine = getStorageEngine();
        byte[] bytes = "abc".getBytes();
        engine.put(bytes, new Versioned<byte[]>(bytes));
        List<Versioned<byte[]>> found = engine.get(bytes);
        assertEquals(1, found.size());
    }

    public void testHighMemoryCollection() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        int objectSize = Math.max((int) maxMemory / NUM_OBJECTS, 1);
        StorageEngine<byte[], byte[]> engine = getStorageEngine();
        for(int i = 0; i < NUM_OBJECTS; i++)
            engine.put(Integer.toString(i).getBytes(),
                       new Versioned<byte[]>(TestUtils.randomBytes(objectSize)));
    }

}
