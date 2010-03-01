/**
 * See the NOTICE.txt file distributed with this work for information regarding
 * copyright ownership.
 * 
 * The authors license this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.store.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mongodb.driver.MongoDBException;
import org.mongodb.driver.ts.Doc;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.serialization.mongodb.MongoDBDocSerializer;
import voldemort.store.AbstractStoreTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for MongoDBStorageEngine. Copied tests from StorageEngineTest as they
 * made assumptions about key and value types rather than defer to getValues()
 * and getKeys()
 * 
 */
public class MongoDBStorageEngineTest extends AbstractStoreTest<ByteArray, byte[]> {

    MongoDBDocSerializer mds = new MongoDBDocSerializer();

    @Override
    protected boolean valuesEqual(byte[] t1, byte[] t2) {
        if(t1.length != t2.length)
            return false;

        for(int i = 0; i < t1.length; i++) {
            if(t1[i] != t2[i])
                return false;
        }

        return true;
    }

    @Override
    public List<byte[]> getValues(int numValues) {
        List<byte[]> list = new ArrayList<byte[]>();
        for(int i = 0; i < numValues; i++) {
            list.add(mds.toBytes(new Doc("x", i)));
        }
        return list;

    }

    @Override
    public List<ByteArray> getKeys(int numKeys) {
        List<ByteArray> list = new ArrayList<ByteArray>();
        StringBuffer sb = new StringBuffer("key_");
        for(int i = 0; i < numKeys; i++) {
            sb.append(i);
            list.add(new ByteArray(sb.toString().getBytes()));
        }
        return list;
    }

    @Override
    public StorageEngine<ByteArray, byte[]> getStore() {
        try {
            MongoDBStorageEngine e = new MongoDBStorageEngine("engine_tests");
            e.clearStore();
            return e;
        } catch(MongoDBException ee) {
            throw new VoldemortException(ee);
        }
    }

    public void testGetNoEntries() {
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;

        try {
            StorageEngine<ByteArray, byte[]> engine = getStore();
            it = engine.entries();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    public void testIterationWithSerialization() {
        StorageEngine<ByteArray, byte[]> store = getStore();
        Map<String, String> vals = ImmutableMap.of("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
        for(Map.Entry<String, String> entry: vals.entrySet()) {
            store.put(new ByteArray(entry.getKey().getBytes()),
                      new Versioned<byte[]>(mds.toBytes(new Doc(entry.getKey(), entry.getValue()))));
        }

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter = store.entries();
        int count = 0;
        while(iter.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = iter.next();

            String key = new String(entry.getFirst().get());
            assertTrue(vals.containsKey(key));

            assertEquals(vals.get(key), mds.toObject(entry.getSecond().getValue()).getString(key));
            count++;
        }

        assertEquals(count, vals.size());
        iter.close();
    }

    public void testPruneOnWrite() {
        StorageEngine<ByteArray, byte[]> engine = getStore();
        Doc d = new Doc("x", 1);
        Versioned<byte[]> v1 = new Versioned<byte[]>(mds.toBytes(d.add("x", 1)),
                                                     TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(mds.toBytes(d.add("x", 2)),
                                                     TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(mds.toBytes(d.add("x", 3)),
                                                     TestUtils.getClock(1, 2));

        ByteArray key = new ByteArray("foo".getBytes());

        engine.put(key, v1);
        engine.put(key, v2);
        assertEquals(2, engine.get(key).size());
        engine.put(key, v3);
        assertEquals(1, engine.get(key).size());
    }
}
