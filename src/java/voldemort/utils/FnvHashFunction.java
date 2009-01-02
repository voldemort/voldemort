package voldemort.utils;

/**
 * Taken from http://www.isthe.com/chongo/tech/comp/fnv
 * 
 * hash = basis for each octet_of_data to be hashed hash = hash * FNV_prime hash
 * = hash xor octet_of_data return hash
 * 
 * @author jay
 * 
 */
public class FnvHashFunction implements HashFunction {

    private static final long FNV_BASIS = 0x811c9dc5;
    private static final long FNV_PRIME = (1 << 24) + 0x193;

    public int hash(byte[] key) {
        long hash = FNV_BASIS;
        for (int i = 0; i < key.length; i++) {
            hash ^= 0xFF & key[i];
            hash *= FNV_PRIME;
        }

        return (int) hash;
    }

    public static void main(String[] args) {
        if (args.length != 2)
            Utils.croak("USAGE: java FnvHashFunction iterations buckets");
        int numIterations = Integer.parseInt(args[0]);
        int numBuckets = Integer.parseInt(args[1]);
        int[] buckets = new int[numBuckets];
        HashFunction hash = new FnvHashFunction();
        for (int i = 0; i < numIterations; i++) {
            int val = hash.hash(Integer.toString(i).getBytes());
            buckets[Math.abs(val) % numBuckets] += 1;
        }

        double expected = numIterations / (double) numBuckets;
        for (int i = 0; i < numBuckets; i++)
            System.out.println(i + " " + buckets[i] + " " + (buckets[i] / expected));
    }

}
