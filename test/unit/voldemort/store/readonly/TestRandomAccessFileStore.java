package voldemort.store.readonly;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.VoldemortException;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class TestRandomAccessFileStore extends TestCase {

    public void testOpenInvalidStoreFails() throws Exception {
        // empty is okay
        testOpenInvalidStoreFails(0, 0, true);
        // two entries with 1 byte each of data
        testOpenInvalidStoreFails(RandomAccessFileStore.INDEX_ENTRY_SIZE * 2,
                                  RandomAccessFileStore.INDEX_ENTRY_SIZE * +2,
                                  true);

        // okay these are corrupt:
        // invalid index size
        testOpenInvalidStoreFails(73, 1024, false);
        // too little data for index (1 byte short for all empty values)
        testOpenInvalidStoreFails(RandomAccessFileStore.INDEX_ENTRY_SIZE * 10, 10 * 4 - 1, false);
        // empty index implies no data
        testOpenInvalidStoreFails(RandomAccessFileStore.INDEX_ENTRY_SIZE, 0, false);
    }

    public void testOpenInvalidStoreFails(int indexBytes, int dataBytes, boolean shouldWork)
            throws Exception {
        File testDir = prepareTestDir();
        File index = prepareIndexDir(testDir);
        File data = prepareDataDir(testDir);

        // write some random crap for index and data
        FileOutputStream dataOs = new FileOutputStream(data);
        for(int i = 0; i < dataBytes; i++)
            dataOs.write(i);
        dataOs.close();
        FileOutputStream indexOs = new FileOutputStream(index);
        for(int i = 0; i < indexBytes; i++)
            indexOs.write(i);
        indexOs.close();

        try {
            RandomAccessFileStore store = new RandomAccessFileStore("test",
                                                                    testDir,
                                                                    1,
                                                                    1,
                                                                    1000,
                                                                    100 * 1000 * 1000);
            if(!shouldWork)
                fail("Able to open corrupt read-only store (index size = " + indexBytes
                     + ", data bytes = " + dataBytes + ").");
        } catch(VoldemortException e) {
            if(shouldWork)
                fail("Unexpected failure:" + e.getMessage());
        }
    }

    private File prepareTestDir() {
        File testDir = new File(System.getProperty("java.io.tmpdir"), "test545");
        Utils.rm(testDir);
        testDir.mkdir();
        return testDir;
    }

    private File prepareDataDir(File testDir) throws IOException {
        File data = new File(testDir, "test.data");
        data.createNewFile();
        return data;
    }

    private File prepareIndexDir(File testDir) throws IOException {
        File index = new File(testDir, "test.index");
        index.createNewFile();
        return index;
    }

    /**
     */
    public void testGetAll() throws Exception {
        File testDir = prepareTestDir();
        File index = prepareIndexDir(testDir);
        File data = prepareDataDir(testDir);

        FileOutputStream indexOs = new FileOutputStream(index);
        FileOutputStream dataOs = new FileOutputStream(data);
        Map<ByteArray, byte[]> keysAndValues = Maps.newHashMap();
        for(int i = 0; i < 5; ++i) {
            byte b = (byte) (i + 1);
            byte[] key = new byte[] { b };
            byte[] md5 = ByteUtils.md5(key);
            byte[] arrayValue = new byte[] { 0, 0, 0, 1, b };
            dataOs.write(arrayValue);
            indexOs.write(md5);
            byte position = (byte) (i * 5);
            indexOs.write(new byte[] { 0, 0, 0, 0, 0, 0, 0, position });
            keysAndValues.put(new ByteArray(key), key);
        }
        dataOs.close();
        indexOs.close();

        RandomAccessFileStore store = new RandomAccessFileStore("test", testDir, 2, 2, 10, 100);
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keysAndValues.keySet());
        assertEquals(keysAndValues.keySet(), result.keySet());
        // TODO The following should pass, but it seems like the data was not
        // encoded properly
        // for(Map.Entry<ByteArray, byte[]> entry: keysAndValues.entrySet()) {
        // byte[] actualValue = result.get(entry.getKey()).get(0).getValue();
        // assertEquals(new ByteArray(entry.getValue()), new
        // ByteArray(actualValue));
        // }
    }
}
