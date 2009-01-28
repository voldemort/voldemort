package voldemort.store.readonly;

import java.io.File;
import java.io.FileOutputStream;

import junit.framework.TestCase;
import voldemort.VoldemortException;
import voldemort.utils.Utils;

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
        File testDir = new File(System.getProperty("java.io.tmpdir"), "test545");
        Utils.rm(testDir);
        testDir.mkdir();
        File index = new File(testDir, "test.index");
        index.createNewFile();
        File data = new File(testDir, "test.data");

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
            RandomAccessFileStore store = new RandomAccessFileStore("test", testDir, 1, 1, 1000);
            if(!shouldWork)
                fail("Able to open corrupt read-only store (index size = " + indexBytes
                     + ", data bytes = " + dataBytes + ").");
        } catch(VoldemortException e) {
            if(shouldWork)
                fail("Unexpected failure:" + e.getMessage());
        }
    }

}
