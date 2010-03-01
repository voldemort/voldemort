package voldemort.store.readonly;

import java.nio.ByteBuffer;

/**
 * A way to search for a key in a file of sorted 16 byte keys and 4 byte
 * position offsets
 * 
 * 
 */
public interface SearchStrategy {

    /**
     * Search for the key in the buffer.
     * 
     * @param index The index buffer
     * @param key The key to search for
     * @param indexSize The size of the index
     * @return The integer offset of the position offset, if the key is found,
     *         else -1
     */
    public int indexOf(ByteBuffer index, byte[] key, int indexSize);

}
