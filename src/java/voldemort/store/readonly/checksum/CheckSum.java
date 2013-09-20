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

package voldemort.store.readonly.checksum;

import java.security.NoSuchAlgorithmException;

import voldemort.utils.ByteUtils;

public abstract class CheckSum {

    /**
     * Update the checksum buffer to include input with startIndex and length.
     * Following calls to multiple 'update's you need to call 'getCheckSum'
     * which will reset the buffer as well
     * 
     * @param input
     * @param startIndex
     * @param length
     */
    public abstract void update(byte[] input, int startIndex, int length);

    /**
     * Get the checkSum of the buffer till now, after which buffer is reset
     */
    public abstract byte[] getCheckSum();

    /**
     * Reset the checksum generator
     */
    public abstract void reset();

    /**
     * Update and get checksum + reset the digest for next use
     * 
     * @param input The input byte array
     * @param startIndex The index to start reading from
     * @param length The number of bytes to read
     * @return Returns the checksum byte array
     */
    public byte[] updateAndGetCheckSum(byte[] input, int startIndex, int length) {
        update(input, startIndex, length);
        return getCheckSum();
    }

    /**
     * Update the underlying buffer using the integer
     * 
     * @param number number to be stored in checksum buffer
     */
    public void update(int number) {
        byte[] numberInBytes = new byte[ByteUtils.SIZE_OF_INT];
        ByteUtils.writeInt(numberInBytes, number, 0);
        update(numberInBytes);
    }

    /**
     * Update the underlying buffer using the short
     * 
     * @param number number to be stored in checksum buffer
     */
    public void update(short number) {
        byte[] numberInBytes = new byte[ByteUtils.SIZE_OF_SHORT];
        ByteUtils.writeShort(numberInBytes, number, 0);
        update(numberInBytes);
    }

    /**
     * Update the checksum buffer to include input
     * 
     * @param input bytes added to the buffer
     */
    public void update(byte[] input) {
        update(input, 0, input.length);
    }

    public enum CheckSumType {
        NONE,
        ADLER32,
        MD5,
        CRC32;

        public static CheckSumType toType(String val) {
            return CheckSum.fromString(val);
        }

    }

    public static CheckSum getInstance(CheckSumType type) {
        if(type == CheckSumType.ADLER32) {
            return new Adler32CheckSum();
        } else if(type == CheckSumType.CRC32) {
            return new CRC32CheckSum();
        } else if(type == CheckSumType.MD5) {
            try {
                return new MD5CheckSum();
            } catch(NoSuchAlgorithmException e) {
                return null;
            }
        }
        return null;
    }

    public static int checkSumLength(CheckSumType type) {
        if(type == CheckSumType.ADLER32) {
            return ByteUtils.SIZE_OF_LONG;
        } else if(type == CheckSumType.CRC32) {
            return ByteUtils.SIZE_OF_LONG;
        } else if(type == CheckSumType.MD5) {
            return 16;
        }
        return 0;
    }

    public static String toString(CheckSumType type) {
        if(type == CheckSumType.ADLER32) {
            return "adler32";
        } else if(type == CheckSumType.CRC32) {
            return "crc32";
        } else if(type == CheckSumType.MD5) {
            return "md5";
        } else if(type == CheckSumType.NONE) {
            return "none";
        }
        return "none";
    }

    public static CheckSumType fromString(String input) {
        if(input.contains("adler32")) {
            return CheckSumType.ADLER32;
        } else if(input.contains("crc32")) {
            return CheckSumType.CRC32;
        } else if(input.contains("md5")) {
            return CheckSumType.MD5;
        }
        return CheckSumType.NONE;
    }

    public static CheckSum getInstance(String algorithm) {
        return getInstance(fromString(algorithm));
    }

}
