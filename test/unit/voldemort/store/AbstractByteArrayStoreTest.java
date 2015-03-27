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

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * 
 */
public abstract class AbstractByteArrayStoreTest extends
        AbstractStoreTest<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(AbstractByteArrayStoreTest.class);

    @Override
    public List<ByteArray> getKeys(int numValues) {
        return getKeys(numValues, 8);
    }

    public List<ByteArray> getKeys(int numValues, int size) {
        List<ByteArray> keys = Lists.newArrayList();
        for(byte[] array: this.getByteValues(numValues, size))
            keys.add(new ByteArray(array));
        return keys;
    }

    @Override
    public List<byte[]> getValues(int numValues) {
        return getValues(numValues, 10);
    }

    public List<byte[]> getValues(int numValues, int size) {
        return this.getByteValues(numValues, size);
    }

    @Override
    protected boolean valuesEqual(byte[] t1, byte[] t2) {
        return TestUtils.bytesEqual(t1, t2);
    }

    @Test
    public void testEmptyByteArray() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getStore();
        Versioned<byte[]> bytes = new Versioned<byte[]>(new byte[0]);
        store.put(new ByteArray(new byte[0]), bytes, null);
        List<Versioned<byte[]>> found = store.get(new ByteArray(new byte[0]), null);
        assertEquals("Incorrect number of results.", 1, found.size());
        assertEquals("Get doesn't equal put.", bytes, found.get(0));
    }

    public byte[] getAllPossibleBytes() {
        byte[] allPossibleBytes = new byte[(Byte.MAX_VALUE - Byte.MIN_VALUE) + 1];
        int index = 0;
        for(int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++, index++) {
            allPossibleBytes[index] = (byte) b;
        }
        return allPossibleBytes;
    }

    public void printBytes(byte[] in) {
        System.out.println("Length: " + in.length);
        for(int i = 0; i < in.length; i++)
            System.out.print(in[i] + ",");
        System.out.println("\n");
    }

    public void testGetAllWithBigValueSizes(Store<ByteArray, byte[], byte[]> store,
                                            int keySize,
                                            int valueSize,
                                            int numKeys) throws Exception {

        List<ByteArray> keys = Lists.newArrayList();
        List<byte[]> values = Lists.newArrayList();

        int putCount = numKeys;
        logger.info("numkeys: " + putCount);
        for(ByteArray key: getKeys(putCount, keySize)) {
            keys.add(key);
        }
        for(byte[] val: getValues(putCount, valueSize)) {
            values.add(val);
        }

        assertEquals(putCount, values.size());
        for(int i = 0; i < putCount; i++) {
            VectorClock vc = getClock(0, 0);
            store.put(keys.get(i), new Versioned<byte[]>(values.get(i), vc), null);
        }

        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keys, null);
        assertGetAllValues(keys, values, result);
    }

    public void testGetWithBigValueSizes(Store<ByteArray, byte[], byte[]> store,
                                         int keySize,
                                         int valueSize) throws Exception {

        List<ByteArray> keys = getKeys(1, keySize);
        ByteArray key = keys.get(0);
        VectorClock vc = getClock(0, 0);

        List<byte[]> values = getValues(1, valueSize);
        byte[] value = values.get(0);

        Versioned<byte[]> versioned = new Versioned<byte[]>(value, vc);
        store.put(key, versioned, null);

        List<Versioned<byte[]>> found = store.get(key, null);
        Assert.assertEquals("Should only be one version stored.", 1, found.size());

        logger.info("input: " + versioned.getValue().length + " bytes");
        logger.info("found " + found.get(0).getValue().length + " bytes");

        if(logger.isDebugEnabled()) {
            logger.debug("Input: ");
            printBytes(versioned.getValue());
            logger.debug("found: ");
            printBytes(found.get(0).getValue());
        }

        Assert.assertEquals("clocks not equal!", versioned.getVersion(), found.get(0).getVersion());
        Assert.assertTrue("Values not equal!",
                          valuesEqual(versioned.getValue(), found.get(0).getValue()));
    }
}
