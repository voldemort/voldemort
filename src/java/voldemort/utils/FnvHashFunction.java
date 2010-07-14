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

package voldemort.utils;

/**
 * Taken from http://www.isthe.com/chongo/tech/comp/fnv
 * 
 * hash = basis for each octet_of_data to be hashed hash = hash * FNV_prime hash
 * = hash xor octet_of_data return hash
 * 
 * 
 */
public class FnvHashFunction implements HashFunction {

    private static final long FNV_BASIS = 0x811c9dc5;
    private static final long FNV_PRIME = (1 << 24) + 0x193;
    public static final long FNV_BASIS_64 = 0xCBF29CE484222325L;
    public static final long FNV_PRIME_64 = 1099511628211L;

    public int hash(byte[] key) {
        long hash = FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash ^= 0xFF & key[i];
            hash *= FNV_PRIME;
        }

        return (int) hash;
    }

    public long hash64(long val) {
        long hashval = FNV_BASIS_64;

        for(int i = 0; i < 8; i++) {
            long octet = val & 0x00ff;
            val = val >> 8;

            hashval = hashval ^ octet;
            hashval = hashval * FNV_PRIME_64;
        }
        return Math.abs(hashval);
    }

    public static void main(String[] args) {
        if(args.length != 2)
            Utils.croak("USAGE: java FnvHashFunction iterations buckets");
        int numIterations = Integer.parseInt(args[0]);
        int numBuckets = Integer.parseInt(args[1]);
        int[] buckets = new int[numBuckets];
        HashFunction hash = new FnvHashFunction();
        for(int i = 0; i < numIterations; i++) {
            int val = hash.hash(Integer.toString(i).getBytes());
            buckets[Math.abs(val) % numBuckets] += 1;
        }

        double expected = numIterations / (double) numBuckets;
        for(int i = 0; i < numBuckets; i++)
            System.out.println(i + " " + buckets[i] + " " + (buckets[i] / expected));
    }

}
