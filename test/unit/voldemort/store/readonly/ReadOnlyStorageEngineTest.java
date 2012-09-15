package voldemort.store.readonly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.Attempt;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.Compression;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class ReadOnlyStorageEngineTest {

    private static int TEST_SIZE = 100;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { new BinarySearchStrategy(), ReadOnlyStorageFormat.READONLY_V0 },
                { new InterpolationSearchStrategy(), ReadOnlyStorageFormat.READONLY_V0 },
                { new BinarySearchStrategy(), ReadOnlyStorageFormat.READONLY_V1 },
                { new InterpolationSearchStrategy(), ReadOnlyStorageFormat.READONLY_V1 },
                { new BinarySearchStrategy(), ReadOnlyStorageFormat.READONLY_V2 },
                { new InterpolationSearchStrategy(), ReadOnlyStorageFormat.READONLY_V2 } });
    }

    private File dir;
    private SearchStrategy strategy;
    private SerializerDefinition serDef;
    private SerializerDefinition lzfSerDef;
    private StoreDefinition storeDef;
    private Node node;
    private RoutingStrategy routingStrategy;
    private ReadOnlyStorageFormat storageType;
    private int indexEntrySize;

    public ReadOnlyStorageEngineTest(SearchStrategy strategy, ReadOnlyStorageFormat storageType) {
        this.strategy = strategy;
        this.dir = TestUtils.createTempDir();
        this.serDef = new SerializerDefinition("json", "'string'");
        this.lzfSerDef = new SerializerDefinition("json",
                                                  ImmutableMap.of(0, "'string'"),
                                                  true,
                                                  new Compression("lzf", null));
        this.storeDef = ServerTestUtils.getStoreDef("test",
                                                    1,
                                                    1,
                                                    1,
                                                    1,
                                                    1,
                                                    RoutingStrategyType.CONSISTENT_STRATEGY);
        Cluster cluster = ServerTestUtils.getLocalCluster(1);
        this.node = cluster.getNodeById(0);
        this.storageType = storageType;

        switch(this.storageType) {
            case READONLY_V0:
            case READONLY_V1:
                // 16 (md5) + 4 (position)
                this.indexEntrySize = 20;
                break;
            case READONLY_V2:
                // 8 (upper 8 bytes of md5) + 4 (position)
                this.indexEntrySize = 12;
                break;
            default:
                throw new VoldemortException("Unsupported storage format type");

        }
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
    }

    @After
    public void tearDown() {
        Utils.rm(dir);
    }

    /**
     * For each key/value pair we built into the store, look it up and test that
     * the correct value is returned
     */
    @Test
    public void canGetGoodValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              serDef,
                                                                                              storageType);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String, String> store = testData.getNodeStores()
                                                                  .get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey(), null);
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                    assertEquals(store.hasKeys(Lists.newArrayList(entry.getKey()), true)
                                      .get(entry.getKey()),
                                 true);
                }
            }
        }

        testData.delete();
    }

    @Test
    public void canGetGoodCompressedValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              lzfSerDef,
                                                                                              storageType);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String, String> store = testData.getNodeStores()
                                                                  .get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey(), null);
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                    assertEquals(store.hasKeys(Lists.newArrayList(entry.getKey()), true)
                                      .get(entry.getKey()),
                                 true);
                }
            }
        }

        testData.delete();
    }

    @Test
    public void canGetGoodCompressedKeys() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              lzfSerDef,
                                                                                              serDef,
                                                                                              storageType);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String, String> store = testData.getNodeStores()
                                                                  .get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey(), null);
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                    assertEquals(store.hasKeys(Lists.newArrayList(entry.getKey()), true)
                                      .get(entry.getKey()),
                                 true);
                }
            }
        }

        testData.delete();
    }

    /**
     * Do lookups on keys not in the store and test that the keys are not found.
     */
    @Test
    public void cantGetBadValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              serDef,
                                                                                              storageType);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(int j = 0; j < TEST_SIZE; j++) {
                String key = TestUtils.randomLetters(10);
                if(!testData.getData().containsKey(key)) {
                    for(int k = 0; k < testData.getNodeStores().size(); k++)
                        assertEquals("Found key in store where it should not be.",
                                     0,
                                     testData.getNodeStores().get(k).get(key, null).size());
                }
            }
        }
        testData.delete();
    }

    @Test
    public void canMultigetGoodValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              serDef,
                                                                                              storageType);
        Set<String> keys = testData.getData().keySet();
        Set<String> gotten = new HashSet<String>();
        for(Map.Entry<Integer, Store<String, String, String>> entry: testData.getNodeStores()
                                                                             .entrySet()) {
            Set<String> queryKeys = new HashSet<String>();
            for(String key: keys)
                for(Node node: testData.routeRequest(key))
                    if(Integer.valueOf(node.getId()).equals(entry.getKey()))
                        queryKeys.add(key);
            Map<String, List<Versioned<String>>> values = entry.getValue().getAll(queryKeys, null);
            assertEquals("Returned fewer keys than expected.", queryKeys.size(), values.size());
            for(Map.Entry<String, List<Versioned<String>>> returned: values.entrySet()) {
                assertTrue(queryKeys.contains(returned.getKey()));
                assertEquals(1, returned.getValue().size());
                Versioned<String> val = returned.getValue().get(0);
                assertEquals(testData.getData().get(returned.getKey()), val.getValue());
                gotten.add(returned.getKey());
            }
            // hasKeys all good keys
            Map<String, Boolean> found = entry.getValue().hasKeys(queryKeys, true);
            for(String key: queryKeys) {
                assertEquals(found.get(key), true);
            }
            // hasKeys some bad keys
            Random r = new Random();
            Set<String> badKeys = Sets.newHashSet(r.nextInt() + "", r.nextInt() + "");
            Set<String> both = Sets.newHashSet(queryKeys);
            both.addAll(badKeys);
            found = entry.getValue().hasKeys(both, true);
            for(String key: queryKeys) {
                assertEquals(found.get(key), true);
            }
            for(String key: badKeys) {
                assertEquals(!found.containsKey(key), true);
            }

        }
        assertEquals(keys, gotten);
        testData.delete();
    }

    @Test
    public void openInvalidStoreFails() throws Exception {
        // empty is okay
        testOpenInvalidStoreFails(0, 0, true);
        // two entries with 1 byte each of data
        testOpenInvalidStoreFails(this.indexEntrySize * 2, this.indexEntrySize * +2, true);

        // okay these are corrupt:
        // invalid index size
        testOpenInvalidStoreFails(73, 1024, false);
        // too little data for index (1 byte short for all empty values)
        testOpenInvalidStoreFails(this.indexEntrySize * 10, 10 * 4 - 1, false);
        // empty index implies no data
        testOpenInvalidStoreFails(this.indexEntrySize, 0, false);
    }

    public void testOpenInvalidStoreFails(int indexBytes, int dataBytes, boolean shouldWork)
            throws Exception {
        File versionDir = new File(dir, "version-0");
        createStoreFiles(versionDir, indexBytes, dataBytes, node, 2);

        try {
            new ReadOnlyStorageEngine("test", strategy, routingStrategy, 0, dir, 1);
            if(!shouldWork)
                fail("Able to open corrupt read-only store (index size = " + indexBytes
                     + ", data bytes = " + dataBytes + ").");
        } catch(VoldemortException e) {
            if(shouldWork)
                fail("Unexpected failure:" + e.getMessage());
        }
    }

    @Test
    public void testSwap() throws Exception {
        File versionDir = new File(dir, "version-0");
        createStoreFiles(versionDir, this.indexEntrySize * 5, 4 * 5 * 10, this.node, 2);

        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test",
                                                                 strategy,
                                                                 routingStrategy,
                                                                 0,
                                                                 dir,
                                                                 2);
        assertVersionsExist(dir, 0);

        // swap to a new version with latest present
        File newDirv1 = new File(dir, "version-1");
        createStoreFiles(newDirv1, 0, 0, this.node, 2);
        engine.swapFiles(newDirv1.getAbsolutePath());
        assertVersionsExist(dir, 0, 1);

        // swap to a new version with no latest present
        File latestSymLink = new File(dir, "latest");
        latestSymLink.delete();
        File newDirv2 = new File(dir, "version-2");
        createStoreFiles(newDirv2, 0, 0, this.node, 2);
        engine.swapFiles(newDirv2.getAbsolutePath());
        assertVersionsExist(dir, 0, 1, 2);

        // rollback
        engine.rollback(versionDir);
        TestUtils.assertWithBackoff(100, 5000, new Attempt() {

            public void checkCondition() throws Exception, AssertionError {
                assertVersionsExist(dir, 0);
            }
        });

        // test initial open without latest
        engine.close();
        latestSymLink.delete();
        File newDirv100 = new File(dir, "version-100");
        createStoreFiles(newDirv100, 0, 0, this.node, 2);
        File newDirv534 = new File(dir, "version-534");
        createStoreFiles(newDirv534, 0, 0, this.node, 2);
        engine.open(null);
        assertTrue(latestSymLink.getCanonicalPath().contains("version-534"));
        engine.close();

        // test initial open with latest pointing at intermediate version folder
        Utils.symlink(newDirv100.getAbsolutePath(), latestSymLink.getAbsolutePath());
        engine.open(null);

    }

    @Test
    public void testSwapRollbackFail() throws IOException {
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test",
                                                                 strategy,
                                                                 routingStrategy,
                                                                 0,
                                                                 dir,
                                                                 1);
        assertVersionsExist(dir, 0);

        // try to rollback nothing
        try {
            engine.rollback((File) null);
            fail("Should have thrown an exception since null is passed");
        } catch(VoldemortException e) {}

        // try to rollback nothing
        engine.rollback(new File(dir, "version-0"));

        // swap to a new version
        File newDir = new File(dir, "version-100");
        createStoreFiles(newDir, 0, 0, node, 2);
        engine.swapFiles(newDir.getAbsolutePath());
        assertVersionsExist(dir, 0, 100);

        // try to swap to a version with version-id less than current max
        File newDir2 = new File(dir, "version-99");
        createStoreFiles(newDir2, 0, 0, node, 2);
        engine.swapFiles(newDir2.getAbsolutePath());

        // try to swap a version with wrong name format
        File newDir3 = new File(dir, "version-1a3");
        createStoreFiles(newDir3, 0, 0, node, 2);
        try {
            engine.swapFiles(newDir3.getAbsolutePath());
            fail("Should have thrown an exception since version directory name format is incorrect");
        } catch(VoldemortException e) {}

    }

    @Test
    public void testBadSwapNameThrows() throws IOException {
        File versionDir = new File(dir, "version-0");
        createStoreFiles(versionDir, this.indexEntrySize * 5, 4 * 5 * 10, node, 2);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test",
                                                                 strategy,
                                                                 routingStrategy,
                                                                 0,
                                                                 dir,
                                                                 2);
        assertVersionsExist(dir, 0);

        // swap to a directory with an incorrect parent directory
        File newDir = TestUtils.createTempDir();
        createStoreFiles(newDir, 73, 1024, node, 2);
        try {
            engine.swapFiles(newDir.getAbsolutePath());
            fail("Swap files should have failed since parent directory is incorrect");
        } catch(VoldemortException e) {}

        // swap to a directory with incorrect name
        newDir = new File(dir, "blah");
        createStoreFiles(newDir, 73, 1024, node, 2);
        try {
            engine.swapFiles(newDir.getAbsolutePath());
            fail("Swap files should have failed since name is incorrect");
        } catch(VoldemortException e) {}
    }

    @Test
    public void testBackupLogic() throws Exception {
        File dirv0 = new File(dir, "version-0");
        createStoreFiles(dirv0, this.indexEntrySize * 5, 4 * 5 * 10, node, 2);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test",
                                                                 strategy,
                                                                 routingStrategy,
                                                                 0,
                                                                 dir,
                                                                 0);
        assertVersionsExist(dir, 0);

        // create directory to imitate a fetch state happening concurrently
        // with swap
        File dirv2 = new File(dir, "version-2");
        createStoreFiles(dirv2, this.indexEntrySize * 5, 4 * 5 * 10, node, 2);

        // swap in directory 1
        File dirv1 = new File(dir, "version-1");
        createStoreFiles(dirv1, this.indexEntrySize * 5, 4 * 5 * 10, node, 2);
        engine.swapFiles(dirv1.getAbsolutePath());

        // check latest symbolic link exists
        File latest = new File(dir, "latest");
        assertTrue(latest.exists());

        // ...and points to 1
        assertTrue(latest.getCanonicalPath().contains("version-1"));

        // ...and version-2 is still in fetch state. Assert with backoff since
        // delete may take time
        TestUtils.assertWithBackoff(100, 5000, new Attempt() {

            public void checkCondition() throws Exception, AssertionError {
                assertEquals(ReadOnlyUtils.getVersionDirs(dir).length, 2);
            }
        });

    }

    @Test(expected = VoldemortException.class)
    public void testBadSwapDataThrows() throws IOException {
        File versionDir = new File(dir, "version-0");
        createStoreFiles(versionDir, this.indexEntrySize * 5, 4 * 5 * 10, node, 2);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test",
                                                                 strategy,
                                                                 routingStrategy,
                                                                 0,
                                                                 dir,
                                                                 2);
        assertVersionsExist(dir, 0);

        // swap to a directory with bad data, rollback should kick-in
        File newDir = new File(dir, "version-1");
        createStoreFiles(newDir, 73, 1024, node, 2);
        engine.swapFiles(newDir.getAbsolutePath());
    }

    @Test
    public void testTruncate() throws IOException {
        createStoreFiles(dir, this.indexEntrySize * 5, 4 * 5 * 10, node, 2);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test",
                                                                 strategy,
                                                                 routingStrategy,
                                                                 0,
                                                                 dir,
                                                                 2);
        assertVersionsExist(dir, 0);

        engine.truncate();
        assertEquals(dir.exists(), false);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testIteration() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              10,
                                                                                              3,
                                                                                              serDef,
                                                                                              serDef,
                                                                                              storageType);
        ListMultimap<Integer, Pair<String, String>> nodeToEntries = ArrayListMultimap.create();
        for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
            for(Node node: testData.routeRequest(entry.getKey())) {
                nodeToEntries.put(node.getId(), Pair.create(entry.getKey(), entry.getValue()));
            }
        }
        SerializerFactory factory = new DefaultSerializerFactory();
        Serializer<String> serializer = (Serializer<String>) factory.getSerializer(serDef);
        for(Map.Entry<Integer, ReadOnlyStorageEngine> storeEntry: testData.getReadOnlyStores()
                                                                          .entrySet()) {
            List<Pair<String, String>> entries = Lists.newArrayList(nodeToEntries.get(storeEntry.getKey()));
            ClosableIterator<ByteArray> keyIterator = null;
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = null;
            try {
                keyIterator = storeEntry.getValue().keys();
                entryIterator = storeEntry.getValue().entries();
            } catch(Exception e) {
                if(storageType.compareTo(ReadOnlyStorageFormat.READONLY_V2) == 0) {
                    fail("Should not have thrown exception since this version supports iteration");
                } else {
                    return;
                }
            }

            // Generate keys from entries
            List<String> keys = Lists.newArrayList();
            Iterator<Pair<String, String>> pairIterator = entries.iterator();
            while(pairIterator.hasNext()) {
                keys.add(pairIterator.next().getFirst());
            }

            // Test keys
            int keyCount = 0;
            while(keyIterator.hasNext()) {
                String key = serializer.toObject(keyIterator.next().get());
                Assert.assertEquals(keys.contains(key), true);
                keyCount++;
            }
            Assert.assertEquals(keyCount, entries.size());

            // Test entries
            int entriesCount = 0;
            while(entryIterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();

                Pair<String, String> stringEntry = Pair.create(serializer.toObject(entry.getFirst()
                                                                                        .get()),
                                                               serializer.toObject(entry.getSecond()
                                                                                        .getValue()));
                Assert.assertEquals(entries.contains(stringEntry), true);
                entriesCount++;
            }
            Assert.assertEquals(entriesCount, entries.size());
        }
    }

    private void assertVersionsExist(File dir, int... versions) throws IOException {
        int max = 0;
        for(int i = 0; i < versions.length; i++) {
            File versionDir = new File(dir, "version-" + versions[i]);
            if(versions[i] > max)
                max = versions[i];
            assertTrue("Could not find " + dir + "/version-" + versions[i], versionDir.exists());
        }
        // check latest symbolic link exists
        File latest = new File(dir, "latest");
        assertTrue(latest.exists());

        // ...and points to max
        assertTrue(latest.getCanonicalPath().contains("version-" + max));

        // now check that the next higher version does not exist
        File versionDir = new File(dir, "version-" + versions.length);
        assertFalse("Found version directory that should not exist.", versionDir.exists());
    }

    private void createStoreFiles(File dir, int indexBytes, int dataBytes, Node node, int numChunks)
            throws IOException, FileNotFoundException {
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, storageType.getCode());

        File metadataFile = createFile(dir, ".metadata");
        BufferedWriter writer = new BufferedWriter(new FileWriter(metadataFile));
        writer.write(metadata.toJsonString());
        writer.close();

        switch(storageType) {
            case READONLY_V0: {
                for(int chunk = 0; chunk < numChunks; chunk++) {
                    File index = createFile(dir, chunk + ".index");
                    File data = createFile(dir, chunk + ".data");
                    // write some random crap for index and data
                    FileOutputStream dataOs = new FileOutputStream(data);
                    for(int i = 0; i < dataBytes; i++)
                        dataOs.write(i);
                    dataOs.close();
                    FileOutputStream indexOs = new FileOutputStream(index);
                    for(int i = 0; i < indexBytes; i++)
                        indexOs.write(i);
                    indexOs.close();
                }
            }
                break;
            case READONLY_V1: {
                for(Integer partitionId: node.getPartitionIds()) {
                    for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                        File index = createFile(dir,
                                                Integer.toString(partitionId) + "_"
                                                        + Integer.toString(chunkId) + ".index");
                        File data = createFile(dir,
                                               Integer.toString(partitionId) + "_"
                                                       + Integer.toString(chunkId) + ".data");
                        // write some random crap for index and data
                        FileOutputStream dataOs = new FileOutputStream(data);
                        for(int i = 0; i < dataBytes; i++)
                            dataOs.write(i);
                        dataOs.close();
                        FileOutputStream indexOs = new FileOutputStream(index);
                        for(int i = 0; i < indexBytes; i++)
                            indexOs.write(i);
                        indexOs.close();
                    }
                }
            }
                break;
            case READONLY_V2: {
                // Assuming number of replicas = 1, since all these tests use a
                // store with replication factor of 1
                for(Integer partitionId: node.getPartitionIds()) {
                    for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                        File index = createFile(dir, Integer.toString(partitionId) + "_0_"
                                                     + Integer.toString(chunkId) + ".index");
                        File data = createFile(dir,
                                               Integer.toString(partitionId) + "_0_"
                                                       + Integer.toString(chunkId) + ".data");
                        // write some random crap for index and data
                        FileOutputStream dataOs = new FileOutputStream(data);
                        for(int i = 0; i < dataBytes; i++)
                            dataOs.write(i);
                        dataOs.close();
                        FileOutputStream indexOs = new FileOutputStream(index);
                        for(int i = 0; i < indexBytes; i++)
                            indexOs.write(i);
                        indexOs.close();
                    }
                }
            }
                break;
            default:
                throw new VoldemortException("Do not support storage type " + storageType);
        }

    }

    private File createFile(File dir, String name) throws IOException {
        dir.mkdirs();
        File data = new File(dir, name);
        data.createNewFile();
        return data;
    }

}
