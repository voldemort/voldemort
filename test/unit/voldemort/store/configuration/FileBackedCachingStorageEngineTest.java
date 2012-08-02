package voldemort.store.configuration;

import static voldemort.TestUtils.getClock;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.store.AbstractStoreTest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class FileBackedCachingStorageEngineTest extends
        AbstractStoreTest<ByteArray, byte[], byte[]> {

    private File tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if(null != tempDir && tempDir.exists())
            FileDeleteStrategy.FORCE.delete(tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if(null != tempDir && tempDir.exists())
            FileDeleteStrategy.FORCE.delete(tempDir);
    }

    /*
     * Calling getStrings to make it readable (easier debugging)
     */
    @Override
    public List<ByteArray> getKeys(int numKeys) {
        List<String> keyList = getStrings(numKeys, 10);
        List<ByteArray> byteArrayKeyList = new ArrayList<ByteArray>();
        for(String s: keyList) {
            byteArrayKeyList.add(new ByteArray(s.getBytes()));
        }
        return byteArrayKeyList;
    }

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() {
        if(null == tempDir || !tempDir.exists()) {
            tempDir = TestUtils.createTempDir();
        }
        return new FileBackedCachingStorageEngine("file-backed-test", tempDir.getAbsolutePath());
    }

    @Override
    protected boolean allowConcurrentOperations() {
        return false;
    }

    @Override
    protected boolean valuesEqual(byte[] t1, byte[] t2) {
        return Arrays.equals(t1, t2);
    }

    @Override
    public List<byte[]> getValues(int numValues) {
        List<String> keyList = getStrings(numValues, 10);
        List<byte[]> byteArrayKeyList = new ArrayList<byte[]>();
        for(String s: keyList) {
            byteArrayKeyList.add(s.getBytes());
        }
        return byteArrayKeyList;
    }

    @Override
    public void testDelete() {
        ByteArray key = getKey();
        Store<ByteArray, byte[], byte[]> store = getStore();
        VectorClock c1 = getClock(1, 1);
        byte[] value = getValue();

        // can't delete something that isn't there
        assertTrue(!store.delete(key, c1));

        store.put(key, new Versioned<byte[]>(value, c1), null);
        assertEquals(1, store.get(key, null).size());

        // now delete that version too
        assertTrue("Delete failed!", store.delete(key, c1));
        assertEquals(0, store.get(key, null).size());
    }

    @Override
    @Test
    public void testGetAll() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getStore();
        int putCount = 10;
        List<ByteArray> keys = getKeys(putCount);
        List<byte[]> values = getValues(putCount);
        assertEquals(putCount, values.size());
        VectorClock clock = new VectorClock();
        for(int i = 0; i < putCount; i++) {
            store.put(keys.get(i), new Versioned<byte[]>(values.get(i), clock), null);
            clock = clock.incremented(0, System.currentTimeMillis());
        }

        int countForGet = putCount / 2;
        List<ByteArray> keysForGet = keys.subList(0, countForGet);
        List<byte[]> valuesForGet = values.subList(0, countForGet);
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keysForGet, null);
        assertEquals(countForGet, result.size());
        for(int i = 0; i < keysForGet.size(); ++i) {
            ByteArray key = keysForGet.get(i);
            byte[] expectedValue = valuesForGet.get(i);
            List<Versioned<byte[]>> versioneds = result.get(key);
            assertGetAllValues(expectedValue, versioneds);
        }
    }

    @Test
    public void testConcurrentWriteFailure() {
        ByteArray key = getKey();
        Store<ByteArray, byte[], byte[]> store = getStore();
        VectorClock c1 = getClock(1, 1);
        VectorClock c2 = getClock(1, 2);
        byte[] value = getValue();

        // put two conflicting versions, then delete one
        Versioned<byte[]> v1 = new Versioned<byte[]>(value, c1);
        Versioned<byte[]> v2 = new Versioned<byte[]>(value, c2);
        store.put(key, v1, null);
        try {
            store.put(key, v2, null);
            fail("Concurrent write succeeded in FileBackedCachingStorageEngine. Should not be allowed.");
        } catch(VoldemortException ve) {
            // This is OK
        }
    }

}
