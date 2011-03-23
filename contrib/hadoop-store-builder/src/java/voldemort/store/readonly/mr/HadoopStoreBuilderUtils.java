package voldemort.store.readonly.mr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.chunk.DataFileChunk;
import voldemort.store.readonly.chunk.DataFileChunkSet;
import voldemort.utils.ByteUtils;

import com.google.common.collect.Lists;

public class HadoopStoreBuilderUtils {

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

    /**
     * Given a filesystem and path to a node, gets all the files which belong to
     * a partition and replica type
     * 
     * Works only for {@link ReadOnlyStorageFormat.READONLY_V2}
     * 
     * @param fs Underlying filesystem
     * @param path The node directory path
     * @param partitionId The partition id for which we get the files
     * @param replicaType The replica type
     * @return Returns list of files of this partition, replicaType
     * @throws IOException
     */
    public static FileStatus[] getDataChunkFiles(FileSystem fs,
                                                 Path path,
                                                 final int partitionId,
                                                 final int replicaType) throws IOException {
        return fs.listStatus(path, new PathFilter() {

            public boolean accept(Path input) {
                if(input.getName().matches("^" + Integer.toString(partitionId) + "_"
                                           + Integer.toString(replicaType) + "_[\\d]+\\.data")) {
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Given a filesystem and path to a node, gets all the files which belong to
     * a partition, replica type and chunk id
     * 
     * Works only for {@link ReadOnlyStorageFormat.READONLY_V2}
     * 
     * @param fs Underlying filesystem
     * @param path The node directory path
     * @param partitionId The partition id for which we get the files
     * @param replicaType The replica type
     * @param chunkId The chunk id
     * @return Returns list of files of this partition, replicaType, chunkId
     * @throws IOException
     */
    public static FileStatus[] getDataChunkFiles(FileSystem fs,
                                                 Path path,
                                                 final int partitionId,
                                                 final int replicaType,
                                                 final int chunkId) throws IOException {
        return fs.listStatus(path, new PathFilter() {

            public boolean accept(Path input) {
                if(input.getName().matches("^" + Integer.toString(partitionId) + "_"
                                           + Integer.toString(replicaType) + "_"
                                           + Integer.toString(chunkId) + "\\.data")) {
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Given a file name and read-only storage format, tells whether the file
     * name format is correct
     * 
     * @param fileName The name of the file
     * @param format The RO format
     * @return true if file format is correct, else false
     */
    public static boolean isFormatCorrect(String fileName, ReadOnlyStorageFormat format) {
        switch(format) {
            case READONLY_V0:
            case READONLY_V1:
                if(fileName.matches("^[\\d]+_[\\d]+\\.(data|index)")) {
                    return true;
                } else {
                    return false;
                }

            case READONLY_V2:
                if(fileName.matches("^[\\d]+_[\\d]+_[\\d]+\\.(data|index)")) {
                    return true;
                } else {
                    return false;
                }

            default:
                throw new VoldemortException("Format type not supported");
        }
    }

    /**
     * Returns the chunk id for the file name
     * 
     * @param fileName The file name
     * @return Chunk id
     */
    public static int getChunkId(String fileName) {
        Pattern pattern = Pattern.compile("_[\\d]+\\.");
        Matcher matcher = pattern.matcher(fileName);

        if(matcher.find()) {
            return new Integer(fileName.substring(matcher.start() + 1, matcher.end() - 1));
        } else {
            throw new VoldemortException("Could not extract out chunk id from " + fileName);
        }
    }

    /**
     * Convert list of FileStatus[] files to DataFileChunkSet. The input to this
     * is generally the output of getChunkFiles function.
     * 
     * Works only for {@link ReadOnlyStorageFormat.READONLY_V2}
     * 
     * @param fs Filesystem used
     * @param files List of data chunk files
     * @return DataFileChunkSet Returns the corresponding data chunk set
     * @throws IOException
     */
    public static DataFileChunkSet getDataFileChunkSet(FileSystem fs, FileStatus[] files)
            throws IOException {

        // Make sure it satisfies the partitionId_replicaType format
        List<FileStatus> fileList = Lists.newArrayList();
        for(FileStatus file: files) {
            if(!isFormatCorrect(file.getPath().getName(), ReadOnlyStorageFormat.READONLY_V2)) {
                throw new VoldemortException("Incorrect data file name format for "
                                             + file.getPath().getName() + ". Unsupported by "
                                             + ReadOnlyStorageFormat.READONLY_V2);
            }
            fileList.add(file);
        }

        // Return it in sorted order
        Collections.sort(fileList, new Comparator<FileStatus>() {

            public int compare(FileStatus f1, FileStatus f2) {
                int chunkId1 = getChunkId(f1.getPath().getName());
                int chunkId2 = getChunkId(f2.getPath().getName());

                return chunkId1 - chunkId2;
            }
        });

        List<DataFileChunk> dataFiles = Lists.newArrayList();
        List<Integer> dataFileSizes = Lists.newArrayList();
        for(FileStatus file: fileList) {
            dataFiles.add(new HdfsDataFileChunk(fs, file));
            dataFileSizes.add((int) file.getLen());
        }
        return new DataFileChunkSet(dataFiles, dataFileSizes);
    }
}
