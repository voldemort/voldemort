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

import voldemort.TestUtils;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Versioned;

public abstract class StorageEngineTest extends ByteArrayStoreTest {

    @Override
    public Store<byte[], byte[]> getStore() {
        return getStorageEngine();
    }

    public abstract StorageEngine<byte[], byte[]> getStorageEngine();

    public void testGetNoEntries() {
        ClosableIterator<Entry<byte[], Versioned<byte[]>>> it = null;
        try {
            StorageEngine<byte[], byte[]> engine = getStorageEngine();
            it = engine.entries();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    public void testIterateOverNullValue() {
    // StorageEngine<byte[]> engine = getStorageEngine();
    // engine.put("null", new Versioned<byte[]>(null));
    // ClosableIterator<Entry<Versioned<byte[]>>> it = engine.entries();
    // Versioned<byte[]> v = it.next().getValue();
    // assertNull("Value should remain null.", v.getValue());
    // assertTrue("Two values found?!?", it.hasNext());
    // assertNull(it.next());
    }

    public void testGetEntries() {
        StorageEngine<byte[], byte[]> engine = getStorageEngine();
        int size = 20;
        List<byte[]> values = getValues(size);
        for(int i = 0; i < size; i++)
            engine.put(Integer.toString(i).getBytes(), new Versioned<byte[]>(values.get(i)));

        ClosableIterator<Entry<byte[], Versioned<byte[]>>> it = engine.entries();
        for(int i = 0; i < size; i++) {
            if(!it.hasNext())
                fail("Expected " + size + " items but found only " + i);
            Versioned<byte[]> versioned = it.next().getValue();
            assertTrue("Found value that wasn't put!", remove(values, versioned.getValue()));
        }
        assertEquals("Failed to remove all values!", 0, values.size());
        it.close();
    }

    public void testPruneOnWrite() {
        StorageEngine<byte[], byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 }, TestUtils.getClock(1, 2));
        byte[] key = new byte[] { 3 };
        engine.put(key, v1);
        engine.put(key, v2);
        assertEquals(2, engine.get(key).size());
        engine.put(key, v3);
        assertEquals(1, engine.get(key).size());
    }

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
