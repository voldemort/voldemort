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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    public void testHasKeys() {
        StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();
        StorageEngine<String, String, String> stringStore = new SerializingStorageEngine<String, String, String>(store,
                                                                                                                 new StringSerializer(),
                                                                                                                 new StringSerializer(),
                                                                                                                 new StringSerializer());
        Map<String, String> vals = new HashMap<String, String>();
        for(int i = 0; i < 100; i++) {
            vals.put("" + i, "" + i);
            stringStore.put("" + i, new Versioned<String>("" + i), null);
        }

        for(int i = 100; i < 200; i++) {
            vals.put("" + i, "" + i);
        }

        Map<String, Boolean> results = stringStore.hasKeys(vals.keySet(), true);

        for(int i = 0; i < 100; i++) {
            assertEquals(results.get("" + i), new Boolean(true));
        }

        for(int i = 100; i < 200; i++) {
            assertEquals(results.get("" + i), new Boolean(false));
        }
    }

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
