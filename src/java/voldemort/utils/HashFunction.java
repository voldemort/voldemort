package voldemort.utils;

/**
 * A hash function for bytes, determinisitically maps bytes into ints
 * 
 * @author jay
 * 
 */
public interface HashFunction {

    public int hash(byte[] key);

}
