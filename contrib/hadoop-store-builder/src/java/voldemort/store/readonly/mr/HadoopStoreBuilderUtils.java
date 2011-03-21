package voldemort.store.readonly.mr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import voldemort.utils.ByteUtils;

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
     * a partition
     * 
     * @param fs Underlying filesystem
     * @param path The node directory path
     * @param partitionId The partition id for which we get the files
     * @param replicaType The replica type
     * @return Returns list of files of this partition
     * @throws IOException
     */
    public static FileStatus[] getChunkFiles(FileSystem fs,
                                             Path path,
                                             final int partitionId,
                                             final int replicaType) throws IOException {
        return fs.listStatus(path, new PathFilter() {

            public boolean accept(Path input) {
                if(input.getName().startsWith(Integer.toString(partitionId) + "_"
                                              + Integer.toString(replicaType))) {
                    return true;
                } else {
                    return false;
                }
            }
        });
    }
}
