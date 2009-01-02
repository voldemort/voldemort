package voldemort.utils;

/**
 * A hash function that always hashes everything to the same value. Useful for
 * testing purposes.
 * 
 * @author jay
 * 
 */
public class ConstantHashFunction implements HashFunction {

    private final int hashCode;

    public ConstantHashFunction(int hashCode) {
        this.hashCode = hashCode;
    }

    public int hash(byte[] key) {
        return hashCode;
    }

}
