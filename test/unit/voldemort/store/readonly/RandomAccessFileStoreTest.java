package voldemort.store.readonly;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class RandomAccessFileStoreTest extends TestCase {

    private static int TEST_SIZE = 100;

    private File dir;

    @Override
    public void setUp() {
        this.dir = TestUtils.createTempDir();
    }

    @Override
    public void tearDown() {
        Utils.rm(dir);
    }

    /**
     * For each key/value pair we built into the store, look it up and test that
     * the correct value is returned
     */
    public void testCanGetGoodValues() throws Exception {
        RandomAccessStoreTestInstance testData = RandomAccessStoreTestInstance.create(dir,
                                                                                      TEST_SIZE,
                                                                                      2,
                                                                                      2);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String> store = testData.getNodeStores().get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey());
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                }
            }
        }

        testData.delete();
    }

    /**
     * Do lookups on keys not in the store and test that the keys are not found.
     */
    public void testCantGetBadValues() throws Exception {
        RandomAccessStoreTestInstance testData = RandomAccessStoreTestInstance.create(dir,
                                                                                      TEST_SIZE,
                                                                                      2,
                                                                                      2);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(int j = 0; j < TEST_SIZE; j++) {
                String key = TestUtils.randomLetters(10);
                if(!testData.getData().containsKey(key)) {
                    for(int k = 0; k < testData.getNodeStores().size(); k++)
                        assertEquals("Found key in store where it should not be.",
                                     0,
                                     testData.getNodeStores().get(k).get(key).size());
                }
            }
        }
        testData.delete();
    }

    public void testCanMultigetGoodValues() throws Exception {
        RandomAccessStoreTestInstance testData = RandomAccessStoreTestInstance.create(dir,
                                                                                      TEST_SIZE,
                                                                                      2,
                                                                                      2);
        Set<String> keys = testData.getData().keySet();
        Set<String> gotten = new HashSet<String>();
        for(Map.Entry<Integer, Store<String, String>> entry: testData.getNodeStores().entrySet()) {
            Set<String> queryKeys = new HashSet<String>();
            for(String key: keys)
                for(Node node: testData.routeRequest(key))
                    if(Integer.valueOf(node.getId()).equals(entry.getKey()))
                        queryKeys.add(key);
            Map<String, List<Versioned<String>>> values = entry.getValue().getAll(queryKeys);
            assertEquals("Returned fewer keys than expected.", queryKeys.size(), values.size());
            for(Map.Entry<String, List<Versioned<String>>> returned: values.entrySet()) {
                assertTrue(queryKeys.contains(returned.getKey()));
                assertEquals(1, returned.getValue().size());
                Versioned<String> val = returned.getValue().get(0);
                assertEquals(testData.getData().get(returned.getKey()), val.getValue());
                gotten.add(returned.getKey());
            }
        }
        assertEquals(keys, gotten);
        testData.delete();
    }

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
        File index = createFile("test.index");
        File data = createFile("test.data");

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
                                                                    dir,
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

    private File createFile(String name) throws IOException {
        File data = new File(dir, name);
        data.createNewFile();
        return data;
    }

}
