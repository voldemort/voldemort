package voldemort.store;

/**
 * Check that the given key is valid
 * 
 * @author jay
 * 
 */
public class StoreUtils {

    public static <K> void assertValidKey(K key) {
        if (key == null)
            throw new IllegalArgumentException("Key cannot be null.");
    }

}
