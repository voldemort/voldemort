package voldemort.store.readonly;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.utils.ByteUtils;
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
    private final HashMap<Integer, Integer> partitionToChunkStart, partitionToNumChunks;
    private HashSet<Integer> partitionIds;
    private RoutingStrategy routingStrategy;

    public ChunkedFileSet(File directory, RoutingStrategy routingStrategy, int nodeId) {
        this.baseDir = directory;
        if(!Utils.isReadableDir(directory))
            throw new VoldemortException(directory.getAbsolutePath()
                                         + " is not a readable directory.");
        this.indexFileSizes = new ArrayList<Integer>();
        this.dataFileSizes = new ArrayList<Integer>();
        this.indexFiles = new ArrayList<MappedByteBuffer>();
        this.dataFiles = new ArrayList<FileChannel>();
        this.partitionToChunkStart = new HashMap<Integer, Integer>();
        this.partitionToNumChunks = new HashMap<Integer, Integer>();
        this.routingStrategy = routingStrategy;

        // generate partitions list
        this.partitionIds = null;
        for(Node node: routingStrategy.getNodes()) {
            if(node.getId() == nodeId) {
                this.partitionIds = new HashSet<Integer>();
                this.partitionIds.addAll(node.getPartitionIds());
                break;
            }
        }
        if(this.partitionIds == null)
            throw new VoldemortException("Could not open store since the node id could not be found");

        int globalChunkId = 0;
        for(Integer partitionId: this.partitionIds) {
            int chunkId = 0;
            while(true) {
                String fileName = Integer.toString(partitionId) + "_" + Integer.toString(chunkId);
                File index = new File(baseDir, fileName + ".index");
                File data = new File(baseDir, fileName + ".data");
                if(!index.exists() && !data.exists()) {
                    if(chunkId == 0) {
                        // create empty files for this partition
                        try {
                            new File(baseDir, fileName + ".index").createNewFile();
                            new File(baseDir, fileName + ".data").createNewFile();
                            logger.info("No index or data files found, creating empty files for partition "
                                        + partitionId);
                        } catch(IOException e) {
                            throw new VoldemortException("Error creating empty read-only files.", e);
                        }
                    } else {
                        break;
                    }
                } else if(index.exists() ^ data.exists())
                    throw new VoldemortException("One of the following does not exist: "
                                                 + index.toString() + " and " + data.toString()
                                                 + ".");
                if(chunkId == 0)
                    partitionToChunkStart.put(partitionId, globalChunkId);

                /* Deal with file sizes */
                long indexLength = index.length();
                long dataLength = data.length();
                validateFileSizes(indexLength, dataLength);
                indexFileSizes.add((int) indexLength);
                dataFileSizes.add((int) dataLength);

                /* Add the file channel for data */
                dataFiles.add(openChannel(data));
                indexFiles.add(mapFile(index));
                System.out.println("INSIDE GLOBAL = " + globalChunkId + " => " + index.toString());
                chunkId++;
                globalChunkId++;
            }
            partitionToNumChunks.put(partitionId, chunkId);
        }

        if(indexFileSizes.size() == 0)
            throw new VoldemortException("No data chunks found in directory " + baseDir.toString());

        this.numChunks = indexFileSizes.size();
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
        List<Integer> partitionList = routingStrategy.getPartitionList(key);
        partitionList.retainAll(partitionIds);
        if(partitionList.size() != 1)
            throw new VoldemortException("Partition list returned is incorrect");
        Integer chunkId = ReadOnlyUtils.chunk(ByteUtils.md5(key),
                                              partitionToNumChunks.get(partitionList.get(0)));
        return partitionToChunkStart.get(partitionList.get(0)) + chunkId;
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
