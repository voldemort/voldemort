package voldemort.store.readonly;

import java.nio.ByteBuffer;

import voldemort.utils.ByteUtils;

public class ReadOnlyUtils {

    public static final int KEY_HASH_SIZE = 16;
    public static final int POSITION_SIZE = 4;
    public static final int INDEX_ENTRY_SIZE = KEY_HASH_SIZE + POSITION_SIZE;

    public static int chunk(byte[] key, int numChunks) {
        // max handles abs(Integer.MIN_VALUE)
        return Math.max(0, Math.abs(ByteUtils.readInt(key, 0))) % numChunks;
    }

    public static byte[] readKey(ByteBuffer index, int indexByteOffset, byte[] foundKey) {
        index.position(indexByteOffset);
        index.get(foundKey);
        return foundKey;
    }

}
