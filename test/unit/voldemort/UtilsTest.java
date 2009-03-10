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

package voldemort;

import java.security.MessageDigest;
import java.util.Arrays;

import junit.framework.TestCase;
import voldemort.utils.ByteUtils;
import voldemort.utils.ReflectUtils;

public class UtilsTest extends TestCase {

    public void testCat() {
        assertTrue("Concatenation of empty arrays is not empty",
                   Arrays.equals(new byte[0], ByteUtils.cat(new byte[0], new byte[0])));
        assertTrue("Concatenation of no arrays is not empty", Arrays.equals(new byte[0],
                                                                            ByteUtils.cat()));
        assertTrue("Concatenation of arrays incorrect.",
                   Arrays.equals("abcdefg".getBytes(), ByteUtils.cat("ab".getBytes(),
                                                                     "".getBytes(),
                                                                     "cdef".getBytes(),
                                                                     "g".getBytes())));
    }

    public void testCopy() {
        assertTrue("Copy of no bytes is not empty array.",
                   Arrays.equals(new byte[0], ByteUtils.copy("hello".getBytes(), 0, 0)));
        assertTrue("Copy of negetive bytes is not empty array.",
                   Arrays.equals(new byte[0], ByteUtils.copy("hello".getBytes(), 2, 0)));
        assertTrue("Bytes copy incorrectly",
                   Arrays.equals("hell".getBytes(), ByteUtils.copy("hello".getBytes(), 0, 4)));
    }

    public void testReadWriteShort() {
        byte[] bytes = new byte[4];
        ByteUtils.writeShort(bytes, (short) 5, 0);
        assertEquals("Read value not equal to written value.",
                     (short) 5,
                     ByteUtils.readShort(bytes, 0));
        ByteUtils.writeShort(bytes, (short) 500, 2);
        assertEquals("Read value not equal to written value.",
                     (short) 500,
                     ByteUtils.readShort(bytes, 2));
    }

    public void testReadWriteLong() {
        byte[] bytes = new byte[8];
        ByteUtils.writeLong(bytes, 5, 0);
        assertEquals("Read value not equal to written value.", 5, ByteUtils.readLong(bytes, 0));
        long value = System.currentTimeMillis();
        ByteUtils.writeLong(bytes, value, 0);
        assertEquals("Read value not equal to written value.", value, ByteUtils.readLong(bytes, 0));
    }

    public void testReadWriteInt() {
        byte[] bytes = new byte[8];
        ByteUtils.writeInt(bytes, 5, 0);
        assertEquals("Read value not equal to written value.", 5, ByteUtils.readInt(bytes, 0));
        int value = (int) System.currentTimeMillis();
        ByteUtils.writeInt(bytes, value, 0);
        assertEquals("Read value not equal to written value.", value, ByteUtils.readInt(bytes, 0));
    }

    public void testConstruct() {
        String str = "hello";
        String str2 = ReflectUtils.construct(String.class, new Object[] { str });
        assertEquals(str, str2);
    }

    public void testReadWriteBytes() {
        byte[] bytes = new byte[8];
        ByteUtils.writeBytes(bytes, 5, 0, 8);
        assertEquals("Read value not equal to written value.", 5, ByteUtils.readBytes(bytes, 0, 8));
        long value = System.currentTimeMillis();
        ByteUtils.writeBytes(bytes, value, 0, 8);
        assertEquals("Read value not equal to written value.", value, ByteUtils.readBytes(bytes,
                                                                                          0,
                                                                                          8));
        bytes = new byte[24];
        ByteUtils.writeBytes(bytes, value, 8, 8);
        assertEquals("Read value not equal to written value.", value, ByteUtils.readBytes(bytes,
                                                                                          8,
                                                                                          8));
    }

    public void testGetNumberOfRequiredBytes() {
        assertEquals(1, ByteUtils.numberOfBytesRequired(0));
        assertEquals(1, ByteUtils.numberOfBytesRequired(1));
        assertEquals(2, ByteUtils.numberOfBytesRequired(257));
        assertEquals(3, ByteUtils.numberOfBytesRequired(3 * Short.MAX_VALUE));

    }

    public void testToString() {
        assertEquals("010203", ByteUtils.toHexString(new byte[] { 1, 2, 3 }));
        assertEquals("afadae", ByteUtils.toHexString(new byte[] { (byte) 0xaf, (byte) 0xad,
                (byte) 0xae }));
        assertEquals("00000001" + "00000010" + "00000011", ByteUtils.toBinaryString(new byte[] { 1,
                2, 3 }));
        assertEquals("10101111" + "10101101" + "10101110", ByteUtils.toBinaryString(new byte[] {
                (byte) 0xaf, (byte) 0xad, (byte) 0xae }));

    }

    public void testNthByte() {
        assertEquals((byte) 0xFF, ByteUtils.readNthByte(Long.MAX_VALUE, 0));
        assertEquals((byte) 0xFF, ByteUtils.readNthByte(Long.MAX_VALUE, 1));
        testNthByteRead("aa", "bbaa", 0);
        testNthByteRead("bb", "bbaa", 1);
        testNthByteRead("bb", "bbaa", 1);
        testNthByteRead("cc", "aabbccddeeff", 3);
    }

    public void testNthByteRead(String theByte, String theLong, int nth) {
        assertEquals(2, theByte.length());
        assertTrue(theByte.length() <= theLong.length());
        long asLong = Long.parseLong(theLong, 16);
        byte asByte = (byte) Short.parseShort(theByte, 16);
        assertEquals(asByte, ByteUtils.readNthByte(asLong, nth));
    }

    public void testMd5() {
        String test = "alskdjflsajflksdjldfsdf";
        MessageDigest digest = ByteUtils.getDigest("MD5");
        assertEquals(0, ByteUtils.compare(ByteUtils.md5(test.getBytes()),
                                          digest.digest(test.getBytes())));
    }

    public void testSha1() {
        ByteUtils.sha1("hello".getBytes());
    }

    public void testByteComparison() {
        assertEquals(0, ByteUtils.compare(new byte[] {}, new byte[] {}));
        assertEquals(1, ByteUtils.compare(new byte[] { 1 }, new byte[] {}));
        assertEquals(-1, ByteUtils.compare(new byte[] {}, new byte[] { 1 }));
        assertEquals(0, ByteUtils.compare(new byte[] { 0, 1, 2 }, new byte[] { 0, 1, 2 }));
        assertEquals(-1, ByteUtils.compare(new byte[] { 0, 1, 1 }, new byte[] { 0, 1, 2 }));
        assertEquals(-1, ByteUtils.compare(new byte[] { 0, 1, 1 }, new byte[] { 0, 1, 1, 2 }));
        assertEquals(1, ByteUtils.compare(new byte[] { 0, 1, 2 }, new byte[] { 0, 1, 1, 1 }));
        assertEquals(-1, ByteUtils.compare(new byte[] { 0, 1, 2 }, new byte[] { 4, 1, 1, 1 }));
        assertEquals(1, ByteUtils.compare(new byte[] { 0, 1, 4 }, new byte[] { 0, 1, 1, 1 }));
        assertEquals(-1, ByteUtils.compare(new byte[] { 1 }, new byte[] { -1 }));
        assertEquals(0, ByteUtils.compare(new byte[] { -1 }, new byte[] { -1 }));
        assertEquals(1, ByteUtils.compare(new byte[] { -1 }, new byte[] { -4 }));
        assertEquals(-1, ByteUtils.compare(new byte[] { -4 }, new byte[] { -1 }));
    }

}
