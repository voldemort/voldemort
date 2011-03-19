package voldemort.store.readonly;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

public class ReadOnlyUtils {

    public static final int POSITION_SIZE = 4;

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
     * Retrieve the dir pointed to by 'latest' symbolic-link or the current
     * version dir
     * 
     * @return Current version directory, else null
     */
    public static File getCurrentVersion(File storeDirectory) {
        File latestDir = getLatestDir(storeDirectory);
        if(latestDir != null)
            return latestDir;

        File[] versionDirs = getVersionDirs(storeDirectory);
        if(versionDirs == null || versionDirs.length == 0) {
            return null;
        } else {
            return findKthVersionedDir(versionDirs, versionDirs.length - 1, versionDirs.length - 1)[0];
        }
    }

    /**
     * Retrieve the directory pointed by latest symbolic link
     * 
     * @param parentDir The root directory
     * @return The directory pointed to by the latest symbolic link, else null
     */
    public static File getLatestDir(File parentDir) {
        File latestSymLink = new File(parentDir, "latest");

        if(latestSymLink.exists() && Utils.isSymLink(latestSymLink)) {
            File canonicalLatestVersion = null;
            try {
                canonicalLatestVersion = latestSymLink.getCanonicalFile();
            } catch(IOException e) {
                return null;
            }

            if(canonicalLatestVersion != null && checkVersionDirName(canonicalLatestVersion))
                return canonicalLatestVersion;
        }
        return null;
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
     * @param versionDir The directory path
     * @return Returns the version id of the directory, else -1
     */
    public static long getVersionId(File versionDir) {
        return getVersionId(versionDir.getName());
    }

    /**
     * Extracts the version id from a string
     * 
     * @param versionDir The string
     * @return Returns the version id of the directory, else -1
     */
    private static long getVersionId(String versionDir) {
        try {
            return Long.parseLong(versionDir.replace("version-", ""));
        } catch(NumberFormatException e) {
            logger.trace("Cannot parse version directory to obtain id " + versionDir);
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
        return getVersionDirs(rootDir, 0, Long.MAX_VALUE);
    }

    /**
     * Returns all the version directories present in the root directory
     * specified
     * 
     * @param rootDir The parent directory
     * @param maxId The
     * @return An array of version directories
     */
    public static File[] getVersionDirs(File rootDir, final long minId, final long maxId) {
        return rootDir.listFiles(new FileFilter() {

            public boolean accept(File pathName) {
                if(checkVersionDirName(pathName)) {
                    long versionId = getVersionId(pathName);
                    if(versionId != -1 && versionId <= maxId && versionId >= minId) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    /**
     * Returns the directories sorted and indexed between [start, end] where
     * start >= 0 and end < len(files)
     * <p>
     * TODO: Can be made better using 'selection algorithm'
     * <p>
     * 
     * @param versionDirs The list of files to search in
     * @param start Starting index
     * @param end End index
     * @return Array of files
     */
    public static File[] findKthVersionedDir(File[] versionDirs, int start, int end) {
        if(start < 0 || end >= versionDirs.length) {
            logger.error("Incorrect version number requested (" + start + "," + end
                         + "). Should be between (0," + (versionDirs.length - 1) + ")");
            return null;
        }
        Collections.sort(Arrays.asList(versionDirs), new Comparator<File>() {

            public int compare(File file1, File file2) {
                long fileId1 = getVersionId(file1), fileId2 = getVersionId(file2);
                if(fileId1 == fileId2) {
                    return 0;
                } else {
                    if(fileId1 == -1) {
                        return 1;
                    }
                    if(fileId2 == -1) {
                        return -1;
                    }
                    if(fileId1 < fileId2) {
                        return -1;
                    } else {
                        return 1;
                    }
                }

            }
        });

        File[] returnedFiles = new File[end - start + 1];
        for(int index = start, index2 = 0; index <= end; index++, index2++) {
            returnedFiles[index2] = versionDirs[index];
        }
        return returnedFiles;
    }

    /**
     * Given a set of partition files returns the number of files per replica
     * type
     * 
     * @param partitionFiles List of partition files
     * @param replicationFactor The maximum replication factor
     * @return Returns an array of counts per replica type
     */
    public static int[] getReplicaCount(FileStatus[] partitionFiles, int replicationFactor) {
        int[] counts = new int[replicationFactor];

        for(FileStatus file: partitionFiles) {
            int replicaType = Integer.getInteger(file.getPath().getName().substring(2, 3));
            if(replicaType < replicationFactor) {
                counts[replicaType]++;
            } else {
                throw new VoldemortException("Found a chunk with replica type (" + replicaType
                                             + ") which doesn't match ");
            }
        }
        return counts;

    }

    /**
     * Given a filesystem and path to a node, gets all the files which belong to
     * a partition
     * 
     * @param fs Underlying filesystem
     * @param path The node directory path
     * @param partitionId The partition id for which we get the files
     * @return Returns list of files of this partition
     * @throws IOException
     */
    public static FileStatus[] getPartitionFiles(FileSystem fs, Path path, final int partitionId)
            throws IOException {
        return fs.listStatus(path, new PathFilter() {

            public boolean accept(Path input) {
                if(input.getName().startsWith(Integer.toString(partitionId) + "_")) {
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Given a filesystem, path and buffer-size, read the file contents and
     * presents it as a string
     * 
     * @param fs Underlying filesystem
     * @param path The file to read
     * @param bufferSize The buffer size to use for reading
     * @return The contents of the file as a string
     * @throws IOException
     */
    public static String readFileContents(FileSystem fs, Path path, int bufferSize)
            throws IOException {
        if(bufferSize <= 0)
            return new String();

        FSDataInputStream input = fs.open(path);
        byte[] buffer = new byte[bufferSize];

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        while(true) {
            int read = input.read(buffer);
            if(read < 0) {
                break;
            } else {
                buffer = ByteUtils.copy(buffer, 0, read);
            }
            stream.write(buffer);
        }

        return new String(stream.toByteArray());
    }
}
