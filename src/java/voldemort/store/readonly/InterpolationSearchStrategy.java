package voldemort.store.readonly;

import java.nio.ByteBuffer;

import voldemort.utils.ByteUtils;

/**
 * A search strategy that uses interpolation to jump to approximately the
 * correct location in the index with as few comparisons as possible. This
 * strategy depends entirely on the uniform distribution of the keys that is
 * guaranteed by the md5 hash.
 * 
 * 
 */
public class InterpolationSearchStrategy implements SearchStrategy {

    public int indexOf(ByteBuffer index, byte[] key, int indexFileSize) {
        int guess;
        int lowIdx = 0;
        int indexSize = ReadOnlyUtils.POSITION_SIZE + key.length;
        int highIdx = indexFileSize / indexSize - 1;
        long lastIdx = highIdx;
        long lowValue = 0;
        long highValue = 0xFFFFFFFFL;
        long keyInt = ByteUtils.readUnsignedInt(key, 0);
        byte[] found = new byte[key.length];
        while(true) {
            if(lowIdx > highIdx || keyInt < lowValue || keyInt > highValue)
                return -1;

            if(highIdx == lowIdx) {
                guess = highIdx;
            } else {
                long size = highIdx - lowIdx;
                long offset = (size - 1) * (keyInt - lowValue) / (highValue - lowValue);
                guess = lowIdx + (int) offset;
            }

            index.position(guess * indexSize);
            index.get(found);
            int compare = ByteUtils.compare(key, found);

            // did we find it?
            if(compare == 0)
                return index.getInt();

            // okay we didn't find it this time, update the min and max
            long foundInt = ByteUtils.readUnsignedInt(found, 0);
            if(compare == -1) {
                // key is less than found
                if(guess == 0)
                    return -1;
                highIdx = guess - 1;
                highValue = foundInt;
            } else {
                // key is greater than found
                if(guess == lastIdx)
                    return -1;
                lowIdx = guess + 1;
                lowValue = foundInt;
            }
        }
    }
}
