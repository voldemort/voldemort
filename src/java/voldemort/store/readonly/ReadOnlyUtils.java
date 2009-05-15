package voldemort.store.readonly;

import voldemort.utils.ByteUtils;

public class ReadOnlyUtils {

    public static int chunk(byte[] key, int numChunks) {
        return Math.abs(ByteUtils.readInt(key, 0)) % numChunks;
    }

}
