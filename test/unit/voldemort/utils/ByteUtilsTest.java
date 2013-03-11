/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.utils;

import java.security.MessageDigest;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.commons.codec.DecoderException;

public class ByteUtilsTest extends TestCase {

    public void testCat() {
        assertTrue("Concatenation of empty arrays is not empty",
                   Arrays.equals(new byte[0], ByteUtils.cat(new byte[0], new byte[0])));
        assertTrue("Concatenation of no arrays is not empty",
                   Arrays.equals(new byte[0], ByteUtils.cat()));
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

    public void testReadUnsignedInt() {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) 0xFF;
        bytes[1] = (byte) 0xFF;
        bytes[2] = (byte) 0xFF;
        bytes[3] = (byte) 0xFF;
        assertEquals(0xFFFFFFFF, ByteUtils.readInt(bytes, 0));
        bytes[0] = (byte) 0x01;
        bytes[1] = (byte) 0x23;
        bytes[2] = (byte) 0x45;
        bytes[3] = (byte) 0x67;
        assertEquals(0x01234567, ByteUtils.readInt(bytes, 0));
    }

    public void testReadWriteBytes() {
        byte[] bytes = new byte[8];
        ByteUtils.writeBytes(bytes, 5, 0, 8);
        assertEquals("Read value not equal to written value.", 5, ByteUtils.readBytes(bytes, 0, 8));
        long value = System.currentTimeMillis();
        ByteUtils.writeBytes(bytes, value, 0, 8);
        assertEquals("Read value not equal to written value.",
                     value,
                     ByteUtils.readBytes(bytes, 0, 8));
        bytes = new byte[24];
        ByteUtils.writeBytes(bytes, value, 8, 8);
        assertEquals("Read value not equal to written value.",
                     value,
                     ByteUtils.readBytes(bytes, 8, 8));
    }

    public void testGetNumberOfRequiredBytes() {
        assertEquals(1, ByteUtils.numberOfBytesRequired(0));
        assertEquals(1, ByteUtils.numberOfBytesRequired(1));
        assertEquals(2, ByteUtils.numberOfBytesRequired(257));
        assertEquals(3, ByteUtils.numberOfBytesRequired(3 * Short.MAX_VALUE));

    }

    public void testToString() {
        assertEquals("00", ByteUtils.toHexString(new byte[] { 0 }));
        assertEquals("010203", ByteUtils.toHexString(new byte[] { 1, 2, 3 }));
        assertEquals("afadae",
                     ByteUtils.toHexString(new byte[] { (byte) 0xaf, (byte) 0xad, (byte) 0xae }));
        assertEquals("afadae",
                     ByteUtils.toHexString(new byte[] { (byte) 0xaf, (byte) 0xad, (byte) 0xae }));
        assertEquals("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
                     ByteUtils.toHexString(new byte[] { (byte) 0, (byte) 1, (byte) 2, (byte) 3,
                             (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8, (byte) 9, (byte) 10,
                             (byte) 11, (byte) 12, (byte) 13, (byte) 14, (byte) 15, (byte) 16,
                             (byte) 17, (byte) 18, (byte) 19, (byte) 20, (byte) 21, (byte) 22,
                             (byte) 23, (byte) 24, (byte) 25, (byte) 26, (byte) 27, (byte) 28,
                             (byte) 29, (byte) 30, (byte) 31, (byte) 32, (byte) 33, (byte) 34,
                             (byte) 35, (byte) 36, (byte) 37, (byte) 38, (byte) 39, (byte) 40,
                             (byte) 41, (byte) 42, (byte) 43, (byte) 44, (byte) 45, (byte) 46,
                             (byte) 47, (byte) 48, (byte) 49, (byte) 50, (byte) 51, (byte) 52,
                             (byte) 53, (byte) 54, (byte) 55, (byte) 56, (byte) 57, (byte) 58,
                             (byte) 59, (byte) 60, (byte) 61, (byte) 62, (byte) 63, (byte) 64,
                             (byte) 65, (byte) 66, (byte) 67, (byte) 68, (byte) 69, (byte) 70,
                             (byte) 71, (byte) 72, (byte) 73, (byte) 74, (byte) 75, (byte) 76,
                             (byte) 77, (byte) 78, (byte) 79, (byte) 80, (byte) 81, (byte) 82,
                             (byte) 83, (byte) 84, (byte) 85, (byte) 86, (byte) 87, (byte) 88,
                             (byte) 89, (byte) 90, (byte) 91, (byte) 92, (byte) 93, (byte) 94,
                             (byte) 95, (byte) 96, (byte) 97, (byte) 98, (byte) 99, (byte) 100,
                             (byte) 101, (byte) 102, (byte) 103, (byte) 104, (byte) 105,
                             (byte) 106, (byte) 107, (byte) 108, (byte) 109, (byte) 110,
                             (byte) 111, (byte) 112, (byte) 113, (byte) 114, (byte) 115,
                             (byte) 116, (byte) 117, (byte) 118, (byte) 119, (byte) 120,
                             (byte) 121, (byte) 122, (byte) 123, (byte) 124, (byte) 125,
                             (byte) 126, (byte) 127, (byte) 128, (byte) 129, (byte) 130,
                             (byte) 131, (byte) 132, (byte) 133, (byte) 134, (byte) 135,
                             (byte) 136, (byte) 137, (byte) 138, (byte) 139, (byte) 140,
                             (byte) 141, (byte) 142, (byte) 143, (byte) 144, (byte) 145,
                             (byte) 146, (byte) 147, (byte) 148, (byte) 149, (byte) 150,
                             (byte) 151, (byte) 152, (byte) 153, (byte) 154, (byte) 155,
                             (byte) 156, (byte) 157, (byte) 158, (byte) 159, (byte) 160,
                             (byte) 161, (byte) 162, (byte) 163, (byte) 164, (byte) 165,
                             (byte) 166, (byte) 167, (byte) 168, (byte) 169, (byte) 170,
                             (byte) 171, (byte) 172, (byte) 173, (byte) 174, (byte) 175,
                             (byte) 176, (byte) 177, (byte) 178, (byte) 179, (byte) 180,
                             (byte) 181, (byte) 182, (byte) 183, (byte) 184, (byte) 185,
                             (byte) 186, (byte) 187, (byte) 188, (byte) 189, (byte) 190,
                             (byte) 191, (byte) 192, (byte) 193, (byte) 194, (byte) 195,
                             (byte) 196, (byte) 197, (byte) 198, (byte) 199, (byte) 200,
                             (byte) 201, (byte) 202, (byte) 203, (byte) 204, (byte) 205,
                             (byte) 206, (byte) 207, (byte) 208, (byte) 209, (byte) 210,
                             (byte) 211, (byte) 212, (byte) 213, (byte) 214, (byte) 215,
                             (byte) 216, (byte) 217, (byte) 218, (byte) 219, (byte) 220,
                             (byte) 221, (byte) 222, (byte) 223, (byte) 224, (byte) 225,
                             (byte) 226, (byte) 227, (byte) 228, (byte) 229, (byte) 230,
                             (byte) 231, (byte) 232, (byte) 233, (byte) 234, (byte) 235,
                             (byte) 236, (byte) 237, (byte) 238, (byte) 239, (byte) 240,
                             (byte) 241, (byte) 242, (byte) 243, (byte) 244, (byte) 245,
                             (byte) 246, (byte) 247, (byte) 248, (byte) 249, (byte) 250,
                             (byte) 251, (byte) 252, (byte) 253, (byte) 254, (byte) 255 }));

        assertEquals("00000000", ByteUtils.toBinaryString(new byte[] { 0 }));
        assertEquals("00000001" + "00000010" + "00000011",
                     ByteUtils.toBinaryString(new byte[] { 1, 2, 3 }));
        assertEquals("10101111" + "10101101" + "10101110",
                     ByteUtils.toBinaryString(new byte[] { (byte) 0xaf, (byte) 0xad, (byte) 0xae }));

    }

    public void specificFromHexStringTest(byte[] lhsBytes, String rhsString) {
        try {
            byte[] rhsBytes = ByteUtils.fromHexString(rhsString);
            int offset = 0;
            for(byte b: lhsBytes) {
                assertEquals(b, rhsBytes[offset]);
                ++offset;
            }
            offset = 0;
            for(byte b: rhsBytes) {
                assertEquals(b, lhsBytes[offset]);
                ++offset;
            }
        } catch(DecoderException de) {
            // DecoderException not expected...
            assertTrue(false);
        }
    }

    public void testFromHexString() {
        specificFromHexStringTest(new byte[] { 0 }, "00");
        specificFromHexStringTest(new byte[] { 1, 2, 3 }, "010203");
        specificFromHexStringTest(new byte[] { (byte) 0xaf, (byte) 0xad, (byte) 0xae }, "afadae");
        // Use following commands to determine test input:
        // $ seq -s ", " -f "(byte) %g" 0 255
        // $ printf '%02x' $(seq 0 255)
        specificFromHexStringTest(new byte[] { (byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4,
                                          (byte) 5, (byte) 6, (byte) 7, (byte) 8, (byte) 9,
                                          (byte) 10, (byte) 11, (byte) 12, (byte) 13, (byte) 14,
                                          (byte) 15, (byte) 16, (byte) 17, (byte) 18, (byte) 19,
                                          (byte) 20, (byte) 21, (byte) 22, (byte) 23, (byte) 24,
                                          (byte) 25, (byte) 26, (byte) 27, (byte) 28, (byte) 29,
                                          (byte) 30, (byte) 31, (byte) 32, (byte) 33, (byte) 34,
                                          (byte) 35, (byte) 36, (byte) 37, (byte) 38, (byte) 39,
                                          (byte) 40, (byte) 41, (byte) 42, (byte) 43, (byte) 44,
                                          (byte) 45, (byte) 46, (byte) 47, (byte) 48, (byte) 49,
                                          (byte) 50, (byte) 51, (byte) 52, (byte) 53, (byte) 54,
                                          (byte) 55, (byte) 56, (byte) 57, (byte) 58, (byte) 59,
                                          (byte) 60, (byte) 61, (byte) 62, (byte) 63, (byte) 64,
                                          (byte) 65, (byte) 66, (byte) 67, (byte) 68, (byte) 69,
                                          (byte) 70, (byte) 71, (byte) 72, (byte) 73, (byte) 74,
                                          (byte) 75, (byte) 76, (byte) 77, (byte) 78, (byte) 79,
                                          (byte) 80, (byte) 81, (byte) 82, (byte) 83, (byte) 84,
                                          (byte) 85, (byte) 86, (byte) 87, (byte) 88, (byte) 89,
                                          (byte) 90, (byte) 91, (byte) 92, (byte) 93, (byte) 94,
                                          (byte) 95, (byte) 96, (byte) 97, (byte) 98, (byte) 99,
                                          (byte) 100, (byte) 101, (byte) 102, (byte) 103,
                                          (byte) 104, (byte) 105, (byte) 106, (byte) 107,
                                          (byte) 108, (byte) 109, (byte) 110, (byte) 111,
                                          (byte) 112, (byte) 113, (byte) 114, (byte) 115,
                                          (byte) 116, (byte) 117, (byte) 118, (byte) 119,
                                          (byte) 120, (byte) 121, (byte) 122, (byte) 123,
                                          (byte) 124, (byte) 125, (byte) 126, (byte) 127,
                                          (byte) 128, (byte) 129, (byte) 130, (byte) 131,
                                          (byte) 132, (byte) 133, (byte) 134, (byte) 135,
                                          (byte) 136, (byte) 137, (byte) 138, (byte) 139,
                                          (byte) 140, (byte) 141, (byte) 142, (byte) 143,
                                          (byte) 144, (byte) 145, (byte) 146, (byte) 147,
                                          (byte) 148, (byte) 149, (byte) 150, (byte) 151,
                                          (byte) 152, (byte) 153, (byte) 154, (byte) 155,
                                          (byte) 156, (byte) 157, (byte) 158, (byte) 159,
                                          (byte) 160, (byte) 161, (byte) 162, (byte) 163,
                                          (byte) 164, (byte) 165, (byte) 166, (byte) 167,
                                          (byte) 168, (byte) 169, (byte) 170, (byte) 171,
                                          (byte) 172, (byte) 173, (byte) 174, (byte) 175,
                                          (byte) 176, (byte) 177, (byte) 178, (byte) 179,
                                          (byte) 180, (byte) 181, (byte) 182, (byte) 183,
                                          (byte) 184, (byte) 185, (byte) 186, (byte) 187,
                                          (byte) 188, (byte) 189, (byte) 190, (byte) 191,
                                          (byte) 192, (byte) 193, (byte) 194, (byte) 195,
                                          (byte) 196, (byte) 197, (byte) 198, (byte) 199,
                                          (byte) 200, (byte) 201, (byte) 202, (byte) 203,
                                          (byte) 204, (byte) 205, (byte) 206, (byte) 207,
                                          (byte) 208, (byte) 209, (byte) 210, (byte) 211,
                                          (byte) 212, (byte) 213, (byte) 214, (byte) 215,
                                          (byte) 216, (byte) 217, (byte) 218, (byte) 219,
                                          (byte) 220, (byte) 221, (byte) 222, (byte) 223,
                                          (byte) 224, (byte) 225, (byte) 226, (byte) 227,
                                          (byte) 228, (byte) 229, (byte) 230, (byte) 231,
                                          (byte) 232, (byte) 233, (byte) 234, (byte) 235,
                                          (byte) 236, (byte) 237, (byte) 238, (byte) 239,
                                          (byte) 240, (byte) 241, (byte) 242, (byte) 243,
                                          (byte) 244, (byte) 245, (byte) 246, (byte) 247,
                                          (byte) 248, (byte) 249, (byte) 250, (byte) 251,
                                          (byte) 252, (byte) 253, (byte) 254, (byte) 255 },
                                  "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
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
        assertEquals(0,
                     ByteUtils.compare(ByteUtils.md5(test.getBytes()),
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

        // Tests byte comparison with sub-sequence
        assertEquals(1, ByteUtils.compare(new byte[] { 0 }, new byte[] { 0 }, 0, 0));
        assertEquals(0, ByteUtils.compare(new byte[] { 0 }, new byte[] { 0 }, 0, 1));
        try {
            assertEquals(0, ByteUtils.compare(new byte[] { 0 }, new byte[] { 0 }, 0, 2));
            fail("Should have thrown an exception");
        } catch(IllegalArgumentException e) {}
        assertEquals(-1, ByteUtils.compare(new byte[] { 2 }, new byte[] { 0, 1, 2, 3 }, 3, 4));
        assertEquals(0, ByteUtils.compare(new byte[] { 3 }, new byte[] { 0, 1, 2, 3 }, 3, 4));
        assertEquals(1, ByteUtils.compare(new byte[] { 5 }, new byte[] { 0, 1, 2, 3 }, 3, 4));

        assertEquals(0, ByteUtils.compare(new byte[] { 2, 3 }, new byte[] { 0, 1, 2, 3 }, 2, 4));
        try {
            assertEquals(0, ByteUtils.compare(new byte[] { 2, 3 }, new byte[] { 0, 1, 2, 3 }, 2, 5));
            fail("Should have thrown an exception");
        } catch(IllegalArgumentException e) {}
        assertEquals(1, ByteUtils.compare(new byte[] { 5 }, new byte[] { 0, 1, 2, 3 }, 2, 4));
    }
}
