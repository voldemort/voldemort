package voldemort.store.readonly;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.Random;

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

    public static boolean checkVersionDirName(File dir) {
        return (dir.isDirectory() && dir.getName().contains("version-") && !dir.getName()
                                                                               .endsWith(".bak"));
    }

    public static long getVersionId(File versionDir) {
        return Long.parseLong(versionDir.getName().replace("version-", ""));
    }

    public static File[] versionDirs(File rootDir) {
        return rootDir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return checkVersionDirName(dir);
            }
        });
    }

    public static File findKthVersionedDir(File[] versionDirs, int kth) {
        if(kth < versionDirs.length) {
            return null;
        }
        return findKthVersionedDir(versionDirs, 0, versionDirs.length, kth);
    }

    private static int partition(File[] versionDirs, int left, int right, int pivotIndex) {
        File pivotValue = versionDirs[pivotIndex];
        
                           swap(versionDirs[pivotIndex],versionDirs[right]);
                           int storeIndex = left
                           for i from left to right-1
                               if list[i] < pivotValue
                                   swap list[storeIndex] and list[i]
                                   storeIndex := storeIndex + 1
                           swap list[right] and list[storeIndex]  // Move pivot to its final place
                           return storeIndex

        return 0;
    }

    private static File findKthVersionedDir(File[] versionDirs, int left, int right, int kth) {
        Random randomizer = new Random();
        while(true) {
            int pivotIndex = randomizer.nextInt(right - left) + left;
            int pivotNewIndex = partition(versionDirs, left, right, pivotIndex);
            if(kth == pivotIndex)
                return versionDirs[kth];
            else if(kth < pivotNewIndex)
                right = pivotNewIndex - 1;
            else {
                left = pivotNewIndex + 1;
                kth -= pivotNewIndex;
            }
        }
    }
}
