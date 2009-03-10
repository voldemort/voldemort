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
    public Store<ByteArray, byte[]> getStore() {
        return getStorageEngine();
    }

    public abstract StorageEngine<ByteArray, byte[]> getStorageEngine();

    public void testGetNoEntries() {
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
            it = engine.entries();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    public void testIterationWithSerialization() {
        StorageEngine<ByteArray, byte[]> store = getStorageEngine();
        StorageEngine<String, String> stringStore = new SerializingStorageEngine<String, String>(store,
                                                                                                 new StringSerializer(),
                                                                                                 new StringSerializer());
        Map<String, String> vals = ImmutableMap.of("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
        for(Map.Entry<String, String> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<String>(entry.getValue()));
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
        StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 }, TestUtils.getClock(1, 2));
        ByteArray key = new ByteArray((byte) 3);
        engine.put(key, v1);
        engine.put(key, v2);
        assertEquals(2, engine.get(key).size());
        engine.put(key, v3);
        assertEquals(1, engine.get(key).size());
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
