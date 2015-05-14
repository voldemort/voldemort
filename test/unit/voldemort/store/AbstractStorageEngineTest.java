/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.serialization.StringSerializer;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractStorageEngineTest extends AbstractByteArrayStoreTest {

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() {
        return getStorageEngine();
    }

    public abstract StorageEngine<ByteArray, byte[], byte[]> getStorageEngine();

    public void testGetNoEntries() {
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
            it = engine.entries();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    public void testGetNoKeys() {
        ClosableIterator<ByteArray> it = null;
        try {
            StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
            it = engine.keys();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    public void testKeyIterationWithSerialization() {
        StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();
        StorageEngine<String, String, String> stringStore = new SerializingStorageEngine<String, String, String>(store,
                                                                                                                 new StringSerializer(),
                                                                                                                 new StringSerializer(),
                                                                                                                 new StringSerializer());
        Map<String, String> vals = ImmutableMap.of("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
        for(Map.Entry<String, String> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<String>(entry.getValue()), null);
        ClosableIterator<String> iter = stringStore.keys();
        int count = 0;
        while(iter.hasNext()) {
            String key = iter.next();
            assertTrue(vals.containsKey(key));
            count++;
        }
        assertEquals(count, vals.size());
        iter.close();
    }

    public void testIterationWithSerialization() {
        StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();
        StorageEngine<String, String, String> stringStore = SerializingStorageEngine.wrap(store,
                                                                                          new StringSerializer(),
                                                                                          new StringSerializer(),
                                                                                          new StringSerializer());
        Map<String, String> vals = ImmutableMap.of("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
        for(Map.Entry<String, String> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<String>(entry.getValue()), null);
        ClosableIterator<Pair<String, Versioned<String>>> iter = stringStore.entries();
        int count = 0;
        while(iter.hasNext()) {
            Pair<String, Versioned<String>> keyAndVal = iter.next();
            assertTrue(vals.containsKey(keyAndVal.getFirst()));
            assertEquals(vals.get(keyAndVal.getFirst()), keyAndVal.getSecond().getValue());
            count++;
        }
        assertEquals(count, vals.size());
        iter.close();
    }

    public void testPruneOnWrite() {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 }, TestUtils.getClock(1, 2));
        ByteArray key = new ByteArray((byte) 3);
        engine.put(key, v1, null);
        engine.put(key, v2, null);
        assertEquals(2, engine.get(key, null).size());
        engine.put(key, v3, null);
        assertEquals(1, engine.get(key, null).size());
    }

    public void testTruncate() throws Exception {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 });
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 });
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 });
        ByteArray key1 = new ByteArray((byte) 3);
        ByteArray key2 = new ByteArray((byte) 4);
        ByteArray key3 = new ByteArray((byte) 5);

        engine.put(key1, v1, null);
        engine.put(key2, v2, null);
        engine.put(key3, v3, null);
        engine.truncate();

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            it = engine.entries();
            while(it.hasNext()) {
                fail("There shouldn't be any entries in this store.");
            }
        } finally {
            if(it != null) {
                it.close();
            }
        }
    }

    @Test
    public void testMultiVersionPuts() {

        StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();

        try {
            // Insert with concurrent versions
            ByteArray key = new ByteArray("mvpKey1".getBytes());
            List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();
            vals.add(TestUtils.getVersioned("val1".getBytes(), 1));
            vals.add(TestUtils.getVersioned("val2".getBytes(), 2));
            vals.add(TestUtils.getVersioned("val3".getBytes(), 3));
            List<Versioned<byte[]>> obsoletes = store.multiVersionPut(key, vals);
            assertTrue("Should not be any rejected versions..", obsoletes.size() == 0);
            assertEquals("Should have all 3 versions stored", 3, store.get(key, null).size());
            assertTrue("All concurrent versions expected",
                       TestUtils.areVersionedListsEqual(vals, store.get(key, null)));
            List<Versioned<byte[]>> saveVals = vals;

            // Insert with some concurrent and some obsolete versions
            key = new ByteArray("mvpKey2".getBytes());
            vals = new ArrayList<Versioned<byte[]>>();
            vals.add(TestUtils.getVersioned("val1".getBytes(), 1));
            vals.add(TestUtils.getVersioned("val2".getBytes(), 2));
            vals.add(TestUtils.getVersioned("val3".getBytes(), 1, 1));
            obsoletes = store.multiVersionPut(key, vals);
            assertTrue("Should not be any obsolete versions..", obsoletes.size() == 0);
            assertEquals("Should have 2 versions stored, with 1:2 superceding 1:1",
                         2,
                         store.get(key, null).size());
            vals.remove(0);
            assertTrue("Should have 2 versiones stored,  with 1:2 superceding 1:1",
                       TestUtils.areVersionedListsEqual(vals, store.get(key, null)));

            // Update of concurrent versions, on top of concurrent versions
            key = new ByteArray("mvpKey1".getBytes());
            vals = new ArrayList<Versioned<byte[]>>();
            vals.add(TestUtils.getVersioned("val4".getBytes(), 4));
            vals.add(TestUtils.getVersioned("val5".getBytes(), 5));
            vals.add(TestUtils.getVersioned("val6".getBytes(), 6));
            obsoletes = store.multiVersionPut(key, vals);
            assertTrue("Should not be any rejected versions..", obsoletes.size() == 0);
            assertEquals("Should have all 6 versions stored", 6, store.get(key, null).size());
            vals.addAll(saveVals);
            assertTrue("All 6 concurrent versions expected",
                       TestUtils.areVersionedListsEqual(vals, store.get(key, null)));
            saveVals = vals;

            // Update of some obsolete versions, on top of concurrent versions
            key = new ByteArray("mvpKey1".getBytes());
            vals = new ArrayList<Versioned<byte[]>>();
            // one obsolete version
            Versioned<byte[]> obsoleteVersion = TestUtils.getVersioned("val4-obsolete".getBytes(),
                                                                       4);
            vals.add(obsoleteVersion);
            // one new concurrent version
            vals.add(TestUtils.getVersioned("val7".getBytes(), 7));
            obsoletes = store.multiVersionPut(key, vals);
            assertTrue("Should be one version rejected..", obsoletes.size() == 1);
            assertEquals("Obsolete's version should be 4:1", obsoleteVersion, obsoletes.get(0));
            assertEquals("Should have all 7 versions stored", 7, store.get(key, null).size());
            vals.remove(0);
            vals.addAll(saveVals);
            assertTrue("All 7 concurrent versions expected",
                       TestUtils.areVersionedListsEqual(vals, store.get(key, null)));

            // super version, makes all versions obsolete
            key = new ByteArray("mvpKey1".getBytes());
            vals = new ArrayList<Versioned<byte[]>>();
            vals.add(TestUtils.getVersioned("val1234567".getBytes(), 1, 2, 3, 4, 5, 6, 7));
            obsoletes = store.multiVersionPut(key, vals);
            assertTrue("Should not be any rejected versions..", obsoletes.size() == 0);
            assertEquals("Exactly one version to be stored", 1, store.get(key, null).size());
            assertTrue("Exactly one version to be stored",
                       TestUtils.areVersionedListsEqual(vals, store.get(key, null)));
        } catch(UnsupportedOperationException uoe) {
            // expected if the storage engine does not support multi version
            // puts
            System.err.println("Multi version puts not supported in test "
                               + this.getClass().getName());
        }
    }

    @Test
    public void testEntryIteration() {
        final int numPut = 10000;
        final StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();
        
        for(int i = 0; i < numPut; i++) {
            String key = "key-" + i;
            String value = "Value for " + key;
            store.put(new ByteArray(key.getBytes()), new Versioned<byte[]>(value.getBytes()), null);
        }
        
        int numGet = 0;
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        
        try {
            it = store.entries();
            while (it.hasNext()) {
                it.next();
                numGet++;
            }
        } finally {
            if (it != null) {
                it.close();
            }
        }
        
        assertEquals("Iterator returned by the call to entries() did not contain the expected number of values", numPut, numGet);
    }

    @SuppressWarnings("unused")
    private boolean remove(List<byte[]> list, byte[] item) {
        Iterator<byte[]> it = list.iterator();
        boolean removedSomething = false;
        while(it.hasNext()) {
            if(TestUtils.bytesEqual(item, it.next())) {
                it.remove();
                removedSomething = true;
            }
        }
        return removedSomething;
    }

}
