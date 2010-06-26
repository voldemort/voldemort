package voldemort.store.readonly;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.utils.Utils;

/**
 * A set of chunked data and index files for a read-only store
 * 
 * 
 */
public class ChunkedFileSet {

    private static Logger logger = Logger.getLogger(ChunkedFileSet.class);

    private final int numChunks;
    private final File baseDir;
    private final List<Integer> indexFileSizes;
    private final List<Integer> dataFileSizes;
    private final List<MappedByteBuffer> indexFiles;
    private final List<FileChannel> dataFiles;

    public ChunkedFileSet(File directory) {
        this.baseDir = directory;
        if(!Utils.isReadableDir(directory))
            throw new VoldemortException(directory.getAbsolutePath()
                                         + " is not a readable directory.");
        this.indexFileSizes = new ArrayList<Integer>();
        this.dataFileSizes = new ArrayList<Integer>();
        this.indexFiles = new ArrayList<MappedByteBuffer>();
        this.dataFiles = new ArrayList<FileChannel>();

        // if the directory is empty create empty files
        if(baseDir.list() != null && baseDir.list().length == 0) {
            try {
                new File(baseDir, "0.index").createNewFile();
                new File(baseDir, "0.data").createNewFile();
                logger.info("No index or data files found, creating empty files 0.index and 0.data.");
            } catch(IOException e) {
                throw new VoldemortException("Error creating empty read-only files.", e);
            }
        }

        // initialize all the chunks
        int chunkId = 0;
        while(true) {
            File index = new File(baseDir, Integer.toString(chunkId) + ".index");
            File data = new File(baseDir, Integer.toString(chunkId) + ".data");
            if(!index.exists() && !data.exists())
                break;
            else if(index.exists() ^ data.exists())
                throw new VoldemortException("One of the following does not exist: "
                                             + index.toString() + " and " + data.toString() + ".");

            /* Deal with file sizes */
            long indexLength = index.length();
            long dataLength = data.length();
            validateFileSizes(indexLength, dataLength);
            indexFileSizes.add((int) indexLength);
            dataFileSizes.add((int) dataLength);

            /* Add the file channel for data */
            dataFiles.add(openChannel(data));
            indexFiles.add(mapFile(index));
            chunkId++;
        }
        if(chunkId == 0)
            throw new VoldemortException("No data chunks found in directory " + baseDir.toString());
        this.numChunks = chunkId;
        logger.trace("Opened chunked file set for " + baseDir + " with " + indexFileSizes.size()
                     + " chunks.");
    }

    public void validateFileSizes(long indexLength, long dataLength) {
        /* sanity check file sizes */
        if(indexLength > Integer.MAX_VALUE || dataLength > Integer.MAX_VALUE)
            throw new VoldemortException("Index or data file exceeds " + Integer.MAX_VALUE
                                         + " bytes.");
        if(indexLength % ReadOnlyUtils.INDEX_ENTRY_SIZE != 0L)
            throw new VoldemortException("Invalid index file, file length must be a multiple of "
                                         + (ReadOnlyUtils.KEY_HASH_SIZE + ReadOnlyUtils.POSITION_SIZE)
                                         + " but is only " + indexLength + " bytes.");

        if(dataLength < 4 * indexLength / ReadOnlyUtils.INDEX_ENTRY_SIZE)
            throw new VoldemortException("Invalid data file, file length must not be less than num_index_entries * 4 bytes, but data file is only "
                                         + dataLength + " bytes.");
    }

    public void close() {
        for(int chunk = 0; chunk < this.numChunks; chunk++) {
            FileChannel channel = dataFileFor(chunk);
            try {
                channel.close();
            } catch(IOException e) {
                logger.error("Error while closing file.", e);
            }
        }
    }

    private FileChannel openChannel(File file) {
        try {
            return new FileInputStream(file).getChannel();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    private MappedByteBuffer mapFile(File file) {
        try {
            FileChannel channel = new FileInputStream(file).getChannel();
            MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, file.length());
            channel.close();
            return buffer;
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    public int getNumChunks() {
        return this.numChunks;
    }

    public int getChunkForKey(byte[] key) {
        return ReadOnlyUtils.chunk(key, numChunks);
    }

    public ByteBuffer indexFileFor(int chunk) {
        return indexFiles.get(chunk).duplicate();
    }

    public FileChannel dataFileFor(int chunk) {
        return dataFiles.get(chunk);
    }

    public int getIndexFileSize(int chunk) {
        return this.indexFileSizes.get(chunk);
    }

    public int getDataFileSize(int chunk) {
        return this.indexFileSizes.get(chunk);
    }

}
