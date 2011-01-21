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
        byte[] keyBuffer = new byte[key.length];
        int indexSize = ReadOnlyUtils.POSITION_SIZE + key.length;
        int low = 0;
        int high = indexFileSize / indexSize - 1;
        while(low <= high) {
            int mid = (low + high) / 2;
            ReadOnlyUtils.readKey(index, mid * indexSize, keyBuffer);
            int cmp = ByteUtils.compare(keyBuffer, key);
            if(cmp == 0) {
                // they are equal, return the location stored here
                index.position(mid * indexSize + key.length);
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
