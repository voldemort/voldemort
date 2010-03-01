package voldemort.store.readonly;

import java.nio.ByteBuffer;

import voldemort.utils.ByteUtils;

/**
 * A search strategy that does a simple binary search into the buffer to find
 * the key
 * 
 * 
 */
public class BinarySearchStrategy implements SearchStrategy {

    public int indexOf(ByteBuffer index, byte[] key, int indexFileSize) {
        byte[] keyBuffer = new byte[ReadOnlyUtils.KEY_HASH_SIZE];
        int low = 0;
        int high = indexFileSize / ReadOnlyUtils.INDEX_ENTRY_SIZE - 1;
        while(low <= high) {
            int mid = (low + high) / 2;
            ReadOnlyUtils.readKey(index, mid * ReadOnlyUtils.INDEX_ENTRY_SIZE, keyBuffer);
            int cmp = ByteUtils.compare(keyBuffer, key);
            if(cmp == 0) {
                // they are equal, return the location stored here
                index.position(mid * ReadOnlyUtils.INDEX_ENTRY_SIZE + ReadOnlyUtils.KEY_HASH_SIZE);
                return index.getInt();
            } else if(cmp > 0) {
                // midVal is bigger
                high = mid - 1;
            } else if(cmp < 0) {
                // the keyMd5 is bigger
                low = mid + 1;
            }
        }

        return -1;
    }

}
