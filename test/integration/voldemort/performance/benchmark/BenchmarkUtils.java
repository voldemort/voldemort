/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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
 * the License. See accompanying LICENSE file.
 */

package voldemort.performance.benchmark;

import java.util.Random;

public class BenchmarkUtils {

    private static Random random = new Random();

    public static String ASCIIString(int length) {
        int interval = '~' - ' ' + 1;

        byte[] buf = new byte[length];
        random.nextBytes(buf);
        for(int i = 0; i < length; i++) {
            if(buf[i] < 0) {
                buf[i] = (byte) ((-buf[i] % interval) + ' ');
            } else {
                buf[i] = (byte) ((buf[i] % interval) + ' ');
            }
        }
        return new String(buf);
    }

    public static int hash(int val) {
        return FNVhash32(val);
    }

    public static final int FNV_offset_basis_32 = 0x811c9dc5;
    public static final int FNV_prime_32 = 16777619;

    public static int FNVhash32(int val) {
        int hashval = FNV_offset_basis_32;

        for(int i = 0; i < 4; i++) {
            int octet = val & 0x00ff;
            val = val >> 8;

            hashval = hashval ^ octet;
            hashval = hashval * FNV_prime_32;
        }
        return Math.abs(hashval);
    }

    public static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
    public static final long FNV_prime_64 = 1099511628211L;

    public static long FNVhash64(long val) {
        long hashval = FNV_offset_basis_64;

        for(int i = 0; i < 8; i++) {
            long octet = val & 0x00ff;
            val = val >> 8;

            hashval = hashval ^ octet;
            hashval = hashval * FNV_prime_64;
        }
        return Math.abs(hashval);
    }
}
