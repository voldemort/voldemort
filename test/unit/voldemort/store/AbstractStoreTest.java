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

import static voldemort.TestUtils.getClock;
import static voldemort.TestUtils.randomLetters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

public abstract class AbstractStoreTest<K, V> extends TestCase {

    public abstract Store<K, V> getStore();

    public abstract List<V> getValues(int numValues);

    public abstract List<K> getKeys(int numKeys);

    public List<String> getStrings(int numKeys, int size) {
        List<String> ts = new ArrayList<String>(numKeys);
        for(int i = 0; i < numKeys; i++)
            ts.add(randomLetters(size));
        return ts;
    }

    public List<byte[]> getByteValues(int numValues, int size) {
        List<byte[]> values = new ArrayList<byte[]>();
        for(int i = 0; i < numValues; i++)
            values.add(TestUtils.randomBytes(size));
        return values;
    }

    public K getKey() {
        return getKeys(1).get(0);
    }

    public V getValue() {
        return getValues(1).get(0);
    }

    public Version getExpectedVersionAfterPut(Version version) {
        return version;
    }

    protected boolean valuesEqual(V t1, V t2) {
        return Objects.equal(t1, t2);
    }

    protected void assertEquals(String message, Versioned<V> v1, Versioned<V> v2) {
        String assertTrueMessage = v1 + " != " + v2 + ".";
        if(message != null)
            assertTrueMessage += message;
        assertTrue(assertTrueMessage, valuesEqual(v1.getValue(), v2.getValue()));
        assertEquals(message, v1.getVersion(), v2.getVersion());
    }

    protected void assertEquals(Versioned<V> v1, Versioned<V> v2) {
        assertEquals(null, v1, v2);
    }

    public void assertContains(Collection<Versioned<V>> collection, Versioned<V> value) {
        boolean found = false;
        for(Versioned<V> t: collection)
            if(valuesEqual(t.getValue(), value.getValue()))
                found = true;
        assertTrue(collection + " does not contain " + value + ".", found);
    }

    public void testNullKeys() {
        Store<K, V> store = getStore();
        try {
            store.put(null, new Versioned<V>(getValue()));
            fail("Store should not put null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            store.get(null);
            fail("Store should not get null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            store.getAll(null);
            fail("Store should not getAll null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            store.getAll(Collections.<K> singleton(null));
            fail("Store should not getAll null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            store.delete(null, new VectorClock());
            fail("Store should not delete null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    public void testPutNullValue() {
    // Store<K,V> store = getStore();
    // K key = getKey();
    // store.put(key, new Versioned<V>(null));
    // List<Versioned<V>> found = store.get(key);
    // assertEquals("Wrong number of values.", 1, found.size());
    // assertEquals("Returned non-null value.", null, found.get(0).getValue());
    }

    public void testGetAndDeleteNonExistentKey() {
        K key = getKey();
        Store<K, V> store = getStore();
        List<Versioned<V>> found = store.get(key);
        assertEquals("Found non-existent key: " + found, 0, found.size());
        assertTrue("Delete of non-existent key succeeded.", !store.delete(key, getClock(1,
                                                                                        1,
                                                                                        2,
                                                                                        2,
                                                                                        3,
                                                                                        3)));
    }

    private void testObsoletePutFails(String message,
                                      Store<K, V> store,
                                      K key,
                                      Versioned<V> versioned) {
        VectorClock clock = (VectorClock) versioned.getVersion();
        clock = clock.clone();
        try {
            store.put(key, versioned);
            fail(message);
        } catch(ObsoleteVersionException e) {
            // this is good, but check that we didn't fuck with the version
            assertEquals(clock, versioned.getVersion());
        }
    }

    public void testFetchedEqualsPut() {
        K key = getKey();
        Store<K, V> store = getStore();
        VectorClock clock = getClock(1, 1, 2, 3, 3, 4);
        V value = getValue();
        assertEquals("Store not empty at start!", 0, store.get(key).size());
        Versioned<V> versioned = new Versioned<V>(value, clock);
        store.put(key, versioned);
        List<Versioned<V>> found = store.get(key);
        assertEquals("Should only be one version stored.", 1, found.size());
        assertTrue("Values not equal!", valuesEqual(versioned.getValue(), found.get(0).getValue()));
    }

    public void testVersionedPut() {
        K key = getKey();
        Store<K, V> store = getStore();
        VectorClock clock = getClock(1, 1);
        VectorClock clockCopy = clock.clone();
        V value = getValue();
        assertEquals("Store not empty at start!", 0, store.get(key).size());
        Versioned<V> versioned = new Versioned<V>(value, clock);

        // put initial version
        store.put(key, versioned);
        assertContains(store.get(key), versioned);

        // test that putting obsolete versions fails
        testObsoletePutFails("Put of identical version/value succeeded.",
                             store,
                             key,
                             new Versioned<V>(value, clockCopy));
        testObsoletePutFails("Put of identical version succeeded.",
                             store,
                             key,
                             new Versioned<V>(getValue(), clockCopy));
        testObsoletePutFails("Put of obsolete version succeeded.",
                             store,
                             key,
                             new Versioned<V>(getValue(), getClock(1)));
        assertEquals("Should still only be one version in store.", store.get(key).size(), 1);
        assertContains(store.get(key), versioned);

        // test that putting a concurrent version succeeds
        store.put(key, new Versioned<V>(getValue(), getClock(1, 2)));
        assertEquals(2, store.get(key).size());

        // test that putting an incremented version succeeds
        Versioned<V> newest = new Versioned<V>(getValue(), getClock(1, 1, 2, 2));
        store.put(key, newest);
        assertContains(store.get(key), newest);
    }

    public void testDelete() {
        K key = getKey();
        Store<K, V> store = getStore();
        VectorClock c1 = getClock(1, 1);
        VectorClock c2 = getClock(1, 2);
        V value = getValue();

        // can't delete something that isn't there
        assertTrue(!store.delete(key, c1));

        // put two conflicting versions, then delete one
        Versioned<V> v1 = new Versioned<V>(value, c1);
        Versioned<V> v2 = new Versioned<V>(value, c2);
        store.put(key, v1);
        store.put(key, v2);
        assertTrue("Delete failed!", store.delete(key, v1.getVersion()));
        List<Versioned<V>> found = store.get(key);

        // check that there is a single remaining version, namely the
        // non-deleted
        assertEquals(1, found.size());
        assertEquals(v2.getVersion(), found.get(0).getVersion());
        assertTrue(valuesEqual(v2.getValue(), found.get(0).getValue()));

        // now delete that version too
        assertTrue("Delete failed!", store.delete(key, c2));
        assertEquals(0, store.get(key).size());
    }

    public void testGetVersions() throws Exception {
        List<K> keys = getKeys(2);
        K key = keys.get(0);
        V value = getValue();
        Store<K, V> store = getStore();
        store.put(key, Versioned.value(value));
        List<Versioned<V>> versioneds = store.get(key);
        List<Version> versions = store.getVersions(key);
        assertTrue(versioneds.size() > 0);
        assertEquals(versioneds.size(), versions.size());
        for(int i = 0; i < versioneds.size(); i++)
            assertEquals(versioneds.get(i).getVersion(), versions.get(i));

        assertEquals(0, store.getVersions(keys.get(1)).size());
    }

    public void testGetAll() throws Exception {
        Store<K, V> store = getStore();
        int putCount = 10;
        List<K> keys = getKeys(putCount);
        List<V> values = getValues(putCount);
        assertEquals(putCount, values.size());
        for(int i = 0; i < putCount; i++)
            store.put(keys.get(i), new Versioned<V>(values.get(i)));

        int countForGet = putCount / 2;
        List<K> keysForGet = keys.subList(0, countForGet);
        List<V> valuesForGet = values.subList(0, countForGet);
        Map<K, List<Versioned<V>>> result = store.getAll(keysForGet);
        assertEquals(countForGet, result.size());
        for(int i = 0; i < keysForGet.size(); ++i) {
            K key = keysForGet.get(i);
            V expectedValue = valuesForGet.get(i);
            List<Versioned<V>> versioneds = result.get(key);
            assertGetAllValues(expectedValue, versioneds);
        }
    }

    public void testGetAllWithAbsentKeys() {
        Store<K, V> store = getStore();
        Map<K, List<Versioned<V>>> result = store.getAll(getKeys(3));
        assertEquals(0, result.size());
    }

    public void testCloseIsIdempotent() {
        Store<K, V> store = getStore();
        store.close();
        // second close is okay, should not throw an exception
        store.close();
    }

    protected void assertGetAllValues(V expectedValue, List<Versioned<V>> versioneds) {
        assertEquals(1, versioneds.size());
        valuesEqual(expectedValue, versioneds.get(0).getValue());
    }
}
