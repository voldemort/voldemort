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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * Utility functions for munging on bytes
 * 
 * 
 */
public class ByteUtils {

    public static final int SIZE_OF_BYTE = Byte.SIZE / Byte.SIZE;
    public static final int SIZE_OF_SHORT = Short.SIZE / Byte.SIZE;
    public static final int SIZE_OF_INT = Integer.SIZE / Byte.SIZE;
    public static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;

    public static final int MASK_00000000 = Integer.parseInt("00000000", 2);
    public static final int MASK_10000000 = Integer.parseInt("10000000", 2);
    public static final int MASK_11000000 = Integer.parseInt("11000000", 2);
    public static final int MASK_11100000 = Integer.parseInt("11100000", 2);
    public static final int MASK_10111111 = Integer.parseInt("10111111", 2);
    public static final int MASK_11011111 = Integer.parseInt("11011111", 2);
    public static final int MASK_01000000 = Integer.parseInt("10000000", 2);
    public static final int MASK_01100000 = Integer.parseInt("11000000", 2);
    public static final int MASK_01110000 = Integer.parseInt("11100000", 2);
    public static final int MASK_01011111 = Integer.parseInt("10111111", 2);
    public static final int MASK_01101111 = Integer.parseInt("11011111", 2);
    public static final int MASK_11111111 = Integer.parseInt("11111111", 2);
    public static final int MASK_01111111 = Integer.parseInt("01111111", 2);
    public static final int MASK_00111111 = Integer.parseInt("00111111", 2);
    public static final int MASK_00011111 = Integer.parseInt("00011111", 2);

    public static final int BYTES_PER_MB = 1048576;
    public static final long BYTES_PER_GB = 1073741824;

    public static MessageDigest getDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch(NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unknown algorithm: " + algorithm, e);
        }
    }

    /**
     * Translate the given byte array into a hexidecimal string
     * 
     * @param bytes The bytes to translate
     * @return The string
     */
    public static String toHexString(byte[] bytes) {
        return Hex.encodeHexString(bytes);
    }

    public static byte[] fromHexString(String hexString) throws DecoderException {
        return Hex.decodeHex(hexString.toCharArray());
    }

    /**
     * Translate the given byte array into a string of 1s and 0s
     * 
     * @param bytes The bytes to translate
     * @return The string
     */
    public static String toBinaryString(byte[] bytes) {
        StringBuilder buffer = new StringBuilder();
        for(byte b: bytes) {
            String bin = Integer.toBinaryString(0xFF & b);
            bin = bin.substring(0, Math.min(bin.length(), 8));

            for(int j = 0; j < 8 - bin.length(); j++) {
                buffer.append('0');
            }

            buffer.append(bin);
        }
        return buffer.toString();
    }

    /**
     * Concatenate the given arrays TODO: cut and paste for every primative type
     * and Object (yay! java!)
     * 
     * @param arrays The arrays to concatenate
     * @return A concatenated array
     */
    public static byte[] cat(byte[]... arrays) {
        int size = 0;
        for(byte[] a: arrays)
            if(a != null)
                size += a.length;
        byte[] cated = new byte[size];
        int pos = 0;
        for(byte[] a: arrays) {
            if(a != null) {
                System.arraycopy(a, 0, cated, pos, a.length);
                pos += a.length;
            }
        }

        return cated;
    }

    /**
     * Copy the specified bytes into a new array
     * 
     * @param array The array to copy from
     * @param from The index in the array to begin copying from
     * @param to The least index not copied
     * @return A new byte[] containing the copied bytes
     */
    public static byte[] copy(byte[] array, int from, int to) {
        if(to - from < 0) {
            return new byte[0];
        } else {
            byte[] a = new byte[to - from];
            System.arraycopy(array, from, a, 0, to - from);
            return a;
        }
    }

    /**
     * Read a short from the byte array starting at the given offset
     * 
     * @param bytes The byte array to read from
     * @param offset The offset to start reading at
     * @return The short read
     */
    public static short readShort(byte[] bytes, int offset) {
        return (short) ((bytes[offset] << 8) | (bytes[offset + 1] & 0xff));
    }

    /**
     * Read an unsigned short from the byte array starting at the given offset
     * 
     * @param bytes The byte array to read from
     * @param offset The offset to start reading at
     * @return The short read
     */
    public static int readUnsignedShort(byte[] bytes, int offset) {
        return (((bytes[offset] & 0xff) << 8) | (bytes[offset + 1] & 0xff));
    }

    /**
     * Read an int from the byte array starting at the given offset
     * 
     * @param bytes The byte array to read from
     * @param offset The offset to start reading at
     * @return The int read
     */
    public static int readInt(byte[] bytes, int offset) {
        return (((bytes[offset + 0] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff));
    }

    /**
     * Read an unsigned integer from the given byte array
     * 
     * @param bytes The bytes to read from
     * @param offset The offset to begin reading at
     * @return The integer as a long
     */
    public static long readUnsignedInt(byte[] bytes, int offset) {
        return (((bytes[offset + 0] & 0xffL) << 24) | ((bytes[offset + 1] & 0xffL) << 16)
                | ((bytes[offset + 2] & 0xffL) << 8) | (bytes[offset + 3] & 0xffL));
    }

    /**
     * Read a long from the byte array starting at the given offset
     * 
     * @param bytes The byte array to read from
     * @param offset The offset to start reading at
     * @return The long read
     */
    public static long readLong(byte[] bytes, int offset) {
        return (((long) (bytes[offset + 0] & 0xff) << 56)
                | ((long) (bytes[offset + 1] & 0xff) << 48)
                | ((long) (bytes[offset + 2] & 0xff) << 40)
                | ((long) (bytes[offset + 3] & 0xff) << 32)
                | ((long) (bytes[offset + 4] & 0xff) << 24)
                | ((long) (bytes[offset + 5] & 0xff) << 16)
                | ((long) (bytes[offset + 6] & 0xff) << 8) | ((long) bytes[offset + 7] & 0xff));
    }

    /**
     * Read the given number of bytes into a long
     * 
     * @param bytes The byte array to read from
     * @param offset The offset at which to begin reading
     * @param numBytes The number of bytes to read
     * @return The long value read
     */
    public static long readBytes(byte[] bytes, int offset, int numBytes) {
        int shift = 0;
        long value = 0;
        for(int i = offset + numBytes - 1; i >= offset; i--) {
            value |= (bytes[i] & 0xFFL) << shift;
            shift += 8;
        }
        return value;
    }

    /**
     * Write a short to the byte array starting at the given offset
     * 
     * @param bytes The byte array
     * @param value The short to write
     * @param offset The offset to begin writing at
     */
    public static void writeShort(byte[] bytes, short value, int offset) {
        bytes[offset] = (byte) (0xFF & (value >> 8));
        bytes[offset + 1] = (byte) (0xFF & value);
    }

    /**
     * Write an unsigned short to the byte array starting at the given offset
     * 
     * @param bytes The byte array
     * @param value The short to write
     * @param offset The offset to begin writing at
     */
    public static void writeUnsignedShort(byte[] bytes, int value, int offset) {
        bytes[offset] = (byte) (0xFF & (value >> 8));
        bytes[offset + 1] = (byte) (0xFF & value);
    }

    /**
     * Write an int to the byte array starting at the given offset
     * 
     * @param bytes The byte array
     * @param value The int to write
     * @param offset The offset to begin writing at
     */
    public static void writeInt(byte[] bytes, int value, int offset) {
        bytes[offset] = (byte) (0xFF & (value >> 24));
        bytes[offset + 1] = (byte) (0xFF & (value >> 16));
        bytes[offset + 2] = (byte) (0xFF & (value >> 8));
        bytes[offset + 3] = (byte) (0xFF & value);
    }

    /**
     * Write a long to the byte array starting at the given offset
     * 
     * @param bytes The byte array
     * @param value The long to write
     * @param offset The offset to begin writing at
     */
    public static void writeLong(byte[] bytes, long value, int offset) {
        bytes[offset] = (byte) (0xFF & (value >> 56));
        bytes[offset + 1] = (byte) (0xFF & (value >> 48));
        bytes[offset + 2] = (byte) (0xFF & (value >> 40));
        bytes[offset + 3] = (byte) (0xFF & (value >> 32));
        bytes[offset + 4] = (byte) (0xFF & (value >> 24));
        bytes[offset + 5] = (byte) (0xFF & (value >> 16));
        bytes[offset + 6] = (byte) (0xFF & (value >> 8));
        bytes[offset + 7] = (byte) (0xFF & value);
    }

    /**
     * Write the given number of bytes out to the array
     * 
     * @param bytes The array to write to
     * @param value The value to write from
     * @param offset the offset into the array
     * @param numBytes The number of bytes to write
     */
    public static void writeBytes(byte[] bytes, long value, int offset, int numBytes) {
        int shift = 0;
        for(int i = offset + numBytes - 1; i >= offset; i--) {
            bytes[i] = (byte) (0xFF & (value >> shift));
            shift += 8;
        }
    }

    /**
     * The number of bytes required to hold the given number
     * 
     * @param number The number being checked.
     * @return The required number of bytes (must be 8 or less)
     */
    public static byte numberOfBytesRequired(long number) {
        if(number < 0)
            number = -number;
        for(byte i = 1; i <= SIZE_OF_LONG; i++)
            if(number < (1L << (8 * i)))
                return i;
        throw new IllegalStateException("Should never happen.");
    }

    public static long readVarNumber(DataInputStream input) throws IOException {
        int b = 0xFF & input.readByte();
        if((b & MASK_10000000) == 0) {
            // one byte value, mask off the size bit and return
            return MASK_01111111 & b;
        } else if((b & MASK_11000000) == MASK_10000000) {
            // two byte value, mask off first two bits
            long val = (b & MASK_00111111) << Byte.SIZE;
            val |= 0xFF & input.readByte();
            return val;
        } else if((b & MASK_11100000) == MASK_11000000) {
            // four byte value, mask off three bits
            long val = (b & MASK_00011111);
            for(int i = 0; i < 3; i++) {
                val <<= Byte.SIZE;
                val |= 0xFF & input.readByte();
            }
            return val;
        } else if((b & MASK_11100000) == MASK_11100000) {
            // eight byte value, mask off three bits
            long val = (b & MASK_00011111);
            for(int i = 0; i < 7; i++) {
                val <<= Byte.SIZE;
                val |= 0xFF & input.readByte();
            }
            return val;
        } else {
            throw new IllegalArgumentException("Unknown prefix!");
        }
    }

    /**
     * Get the nth byte from the right in the given number
     * 
     * @param l The number from which to extract the byte
     * @param n The byte index
     * @return The given byte
     */
    public static byte readNthByte(long l, int n) {
        return (byte) (0xFF & (l >> (n * Byte.SIZE)));
    }

    /**
     * Read exactly buffer.length bytes from the stream into the buffer
     * 
     * @param stream The stream to read from
     * @param buffer The buffer to read into
     */
    public static void read(InputStream stream, byte[] buffer) throws IOException {
        int read = 0;
        while(read < buffer.length) {
            int newlyRead = stream.read(buffer, read, buffer.length - read);
            if(newlyRead == -1)
                throw new EOFException("Attempt to read " + buffer.length
                                       + " bytes failed due to EOF.");
            read += newlyRead;
        }
    }

    /**
     * Translate the string to bytes using the given encoding
     * 
     * @param string The string to translate
     * @param encoding The encoding to use
     * @return The bytes that make up the string
     */
    public static byte[] getBytes(String string, String encoding) {
        try {
            return string.getBytes(encoding);
        } catch(UnsupportedEncodingException e) {
            throw new IllegalArgumentException(encoding + " is not a known encoding name.", e);
        }
    }

    /**
     * Create a string from bytes using the given encoding
     * 
     * @param bytes The bytes to create a string from
     * @param encoding The encoding of the string
     * @return The created string
     */
    public static String getString(byte[] bytes, String encoding) {
        try {
            return new String(bytes, encoding);
        } catch(UnsupportedEncodingException e) {
            throw new IllegalArgumentException(encoding + " is not a known encoding name.", e);
        }
    }

    /**
     * Compute the md5 hash of the input catching all the irritating exceptions
     * for you
     * 
     * @param input The input to take the hash of
     * @return The MD5 hash of the input bytes
     */
    public static byte[] md5(byte[] input) {
        return getDigest("MD5").digest(input);
    }

    /**
     * Compute the sha1 hash of the input catching all the irritating exceptions
     * for you
     * 
     * @param input The input to take the hash of
     * @return The sha1 hash of the input bytes
     */
    public static byte[] sha1(byte[] input) {
        return getDigest("SHA-1").digest(input);
    }

    /**
     * Compare two byte arrays. Two arrays are equal if they are the same size
     * and have the same contents. Otherwise b1 is smaller iff it is a prefix of
     * b2 or for the first index i for which b1[i] != b2[i], b1[i] < b2[i].
     * <p>
     * <strong> bytes are considered unsigned. passing negative values into byte
     * will cause them to be considered as unsigned two's complement value.
     * </strong>
     * 
     * @param b1 The first array
     * @param b2 The second array
     * @return -1 if b1 < b2, 1 if b1 > b2, and 0 if they are equal
     */
    public static int compare(byte[] b1, byte[] b2) {
        return compare(b1, b2, 0, b2.length);
    }

    /**
     * Compare a byte array ( b1 ) with a sub
     * 
     * @param b1 The first array
     * @param b2 The second array
     * @param offset The offset in b2 from which to compare
     * @param to The least offset in b2 which we don't compare
     * @return -1 if b1 < b2, 1 if b1 > b2, and 0 if they are equal
     */
    public static int compare(byte[] b1, byte[] b2, int offset, int to) {
        int j = offset;
        int b2Length = to - offset;
        if(to > b2.length)
            throw new IllegalArgumentException("To offset (" + to + ") should be <= than length ("
                                               + b2.length + ")");
        for(int i = 0; i < b1.length && j < to; i++, j++) {
            int a = (b1[i] & 0xff);
            int b = (b2[j] & 0xff);
            if(a != b) {
                return (a - b) / (Math.abs(a - b));
            }
        }
        return (b1.length - b2Length) / (Math.max(1, Math.abs(b1.length - b2Length)));

    }

    /**
     * If we have no more room in the current buffer, then double our capacity
     * and copy the current buffer to the new one.
     * <p/>
     * Note: the new buffer is allocated using ByteBuffer.allocate.
     * 
     * @param buffer The buffer from which to copy the contents
     * @param newCapacity The new capacity size
     */

    public static ByteBuffer expand(ByteBuffer buffer, int newCapacity) {
        if(newCapacity < buffer.capacity())
            throw new IllegalArgumentException("newCapacity (" + newCapacity
                                               + ") must be larger than existing capacity ("
                                               + buffer.capacity() + ")");

        ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
        int position = buffer.position();
        buffer.rewind();
        newBuffer.put(buffer);
        newBuffer.position(position);
        return newBuffer;
    }

}
