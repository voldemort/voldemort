package voldemort.store.readonly;

import java.io.File;
import java.io.FileFilter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.log4j.Logger;

import voldemort.utils.ByteUtils;

public class ReadOnlyUtils {

    public static final int KEY_HASH_SIZE = 16;
    public static final int POSITION_SIZE = 4;
    public static final int INDEX_ENTRY_SIZE = KEY_HASH_SIZE + POSITION_SIZE;

    private static Logger logger = Logger.getLogger(ReadOnlyUtils.class);

    public static int chunk(byte[] key, int numChunks) {
        // max handles abs(Integer.MIN_VALUE)
        return Math.max(0, Math.abs(ByteUtils.readInt(key, 0))) % numChunks;
    }

    public static byte[] readKey(ByteBuffer index, int indexByteOffset, byte[] foundKey) {
        index.position(indexByteOffset);
        index.get(foundKey);
        return foundKey;
    }

    /**
     * Checks if the name of the file follows the version-n format
     * 
     * @param versionDir The directory
     * @return Returns true if the name is correct, else false
     */
    public static boolean checkVersionDirName(File versionDir) {
        return (versionDir.isDirectory() && versionDir.getName().contains("version-") && !versionDir.getName()
                                                                                                    .endsWith(".bak"));
    }

    /**
     * Extracts the version id from the directory
     * 
     * @param versionDir The directory
     * @return Returns the version id of the directory, else -1
     */
    public static long getVersionId(File versionDir) {
        try {
            return Long.parseLong(versionDir.getName().replace("version-", ""));
        } catch(NumberFormatException e) {
            logger.error("Cannot parse version directory to obtain id "
                         + versionDir.getAbsolutePath());
            return -1;
        }
    }

    /**
     * Returns all the version directories present in the root directory
     * specified
     * 
     * @param rootDir The parent directory
     * @return An array of version directories
     */
    public static File[] getVersionDirs(File rootDir) {
        return rootDir.listFiles(new FileFilter() {

            public boolean accept(File pathName) {
                return checkVersionDirName(pathName);
            }
        });
    }

    /**
     * Returns all the version directories present in the root directory
     * specified
     * 
     * @param rootDir The parent directory
     * @return An array of version directories
     */
    public static File[] getVersionDirs(File rootDir, final long maxId) {
        return rootDir.listFiles(new FileFilter() {

            public boolean accept(File pathName) {
                if(checkVersionDirName(pathName)) {
                    long versionId = getVersionId(pathName);
                    if(versionId != -1 && versionId <= maxId) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    /**
     * Returns the k-th directory in sorted (version-id) version directories.
     * Value of k should be from 1 to length(versionDirs)
     * 
     * Can be made better using 'selection algorithm'
     * 
     * @param versionDirs The list of files to search in
     * @param kth K-th index [1...len(versionDirs)]
     * @return The kth file from versionDirs
     */
    public static File findKthVersionedDir(File[] versionDirs, int kth) {
        if(versionDirs.length < kth || kth <= 0) {
            logger.error("Incorrect version number requested " + kth);
            return null;
        }
        Collections.sort(Arrays.asList(versionDirs), new Comparator<File>() {

            public int compare(File file1, File file2) {
                long fileId1 = getVersionId(file1), fileId2 = getVersionId(file2);
                if(fileId1 == fileId2) {
                    return 0;
                } else if(fileId1 < fileId2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        return versionDirs[kth - 1];
    }
}
