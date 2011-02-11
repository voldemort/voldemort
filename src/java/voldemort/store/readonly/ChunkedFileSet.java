package voldemort.store.readonly;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
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

    private final int numChunks, nodeId;
    private final File baseDir;
    private final List<Integer> indexFileSizes;
    private final List<Integer> dataFileSizes;
    private final List<MappedByteBuffer> indexFiles;
    private final List<FileChannel> dataFiles;
    private final HashMap<Integer, Integer> partitionToChunkStart, partitionToNumChunks;
    private HashSet<Integer> partitionIds;
    private RoutingStrategy routingStrategy;
    private ReadOnlyStorageFormat storageFormat;

    public ChunkedFileSet(File directory, RoutingStrategy routingStrategy, int nodeId) {
        this.baseDir = directory;
        if(!Utils.isReadableDir(directory))
            throw new VoldemortException(directory.getAbsolutePath()
                                         + " is not a readable directory.");

        // Check if format file exists
        File metadataFile = new File(baseDir, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        if(Utils.isReadableFile(metadataFile)) {
            try {
                metadata = new ReadOnlyStorageMetadata(metadataFile);
            } catch(IOException e) {
                logger.warn("Cannot read metadata file, assuming default values");
            }
        } else {
            logger.warn("Metadata file not found. Assuming default settings");
        }

        this.storageFormat = ReadOnlyStorageFormat.fromCode((String) metadata.get(ReadOnlyStorageMetadata.FORMAT,
                                                                                  ReadOnlyStorageFormat.READONLY_V0.getCode()));
        this.indexFileSizes = new ArrayList<Integer>();
        this.dataFileSizes = new ArrayList<Integer>();
        this.indexFiles = new ArrayList<MappedByteBuffer>();
        this.dataFiles = new ArrayList<FileChannel>();
        this.partitionToChunkStart = new HashMap<Integer, Integer>();
        this.partitionToNumChunks = new HashMap<Integer, Integer>();
        this.nodeId = nodeId;
        setRoutingStrategy(routingStrategy);

        switch(storageFormat) {
            case READONLY_V0:
                initVersion0();
                break;
            case READONLY_V1:
            case READONLY_V2:
                initVersion12();
                break;
            default:
                throw new VoldemortException("Invalid chunked storage format type " + storageFormat);
        }

        this.numChunks = indexFileSizes.size();
        logger.trace("Opened chunked file set for " + baseDir + " with " + indexFileSizes.size()
                     + " chunks.");
    }

    public ReadOnlyStorageFormat getReadOnlyStorageFormat() {
        return this.storageFormat;
    }

    public void initVersion0() {
        // if the directory is empty create empty files
        if(baseDir.list() != null && baseDir.list().length <= 1) {
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
    }

    public void initVersion12() {
        int globalChunkId = 0;
        if(this.partitionIds != null) {
            for(Integer partitionId: this.partitionIds) {
                int chunkId = 0;
                while(true) {
                    String fileName = Integer.toString(partitionId) + "_"
                                      + Integer.toString(chunkId);
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
                                throw new VoldemortException("Error creating empty read-only files.",
                                                             e);
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
                    chunkId++;
                    globalChunkId++;
                }
                partitionToNumChunks.put(partitionId, chunkId);
            }

            if(indexFileSizes.size() == 0)
                throw new VoldemortException("No data chunks found in directory "
                                             + baseDir.toString());
        }
    }

    private void setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;

        // generate partitions list
        this.partitionIds = null;
        for(Node node: routingStrategy.getNodes()) {
            if(node.getId() == this.nodeId) {
                this.partitionIds = new HashSet<Integer>();
                this.partitionIds.addAll(node.getPartitionIds());
                break;
            }
        }
    }

    public void validateFileSizes(long indexLength, long dataLength) {
        /* sanity check file sizes */
        if(indexLength > Integer.MAX_VALUE || dataLength > Integer.MAX_VALUE)
            throw new VoldemortException("Index or data file exceeds " + Integer.MAX_VALUE
                                         + " bytes.");
        if(indexLength % (getKeyHashSize() + ReadOnlyUtils.POSITION_SIZE) != 0L)
            throw new VoldemortException("Invalid index file, file length must be a multiple of "
                                         + (getKeyHashSize() + ReadOnlyUtils.POSITION_SIZE)
                                         + " but is only " + indexLength + " bytes.");

        if(dataLength < 4 * indexLength / (getKeyHashSize() + ReadOnlyUtils.POSITION_SIZE))
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

    /**
     * Converts the key to the format in which it is stored for searching
     * 
     * @param key Byte array of the key
     * @return The format stored in the index file
     */
    public byte[] keyToStorageFormat(byte[] key) {

        switch(getReadOnlyStorageFormat()) {
            case READONLY_V0:
            case READONLY_V1:
                return ByteUtils.md5(key);
            case READONLY_V2:
                return ByteUtils.copy(ByteUtils.md5(key), 0, 2 * ByteUtils.SIZE_OF_INT);
            default:
                throw new VoldemortException("Unknown read-only storage format");
        }
    }

    /**
     * Depending on the storage format gives the size of the key stored in the
     * index file
     * 
     * @return Size of key in bytes
     */
    private int getKeyHashSize() {
        switch(getReadOnlyStorageFormat()) {
            case READONLY_V0:
            case READONLY_V1:
                return 16;
            case READONLY_V2:
                return 2 * ByteUtils.SIZE_OF_INT;
            default:
                throw new VoldemortException("Unknown read-only storage format");
        }
    }

    /**
     * Given a particular key, first converts its to the storage format and then
     * determines which chunk it belongs to
     * 
     * @param key Byte array of keys
     * @return Chunk id
     */
    public int getChunkForKey(byte[] key) {
        switch(storageFormat) {
            case READONLY_V0: {
                return ReadOnlyUtils.chunk(keyToStorageFormat(key), numChunks);
            }
            case READONLY_V1:
            case READONLY_V2: {
                List<Integer> partitionList = routingStrategy.getPartitionList(key);
                partitionList.retainAll(partitionIds);
                if(partitionList.size() != 1)
                    return -1;

                Integer chunkId = ReadOnlyUtils.chunk(keyToStorageFormat(key),
                                                      partitionToNumChunks.get(partitionList.get(0)));
                return partitionToChunkStart.get(partitionList.get(0)) + chunkId;
            }
            default: {
                return -1;
            }
        }

    }

    public byte[] readValue(byte[] key, int chunk, int valueLocation) {
        FileChannel dataFile = dataFileFor(chunk);
        try {
            switch(storageFormat) {
                case READONLY_V0:
                case READONLY_V1: {
                    // Read value size
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                    dataFile.read(sizeBuffer, valueLocation);
                    int valueSize = sizeBuffer.getInt(0);

                    // Read value
                    ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                    dataFile.read(valueBuffer, valueLocation + 4);
                    return valueBuffer.array();
                }
                case READONLY_V2: {
                    // Read a byte which holds the number of key/values
                    ByteBuffer numKeyValsBuffer = ByteBuffer.allocate(1);
                    dataFile.read(numKeyValsBuffer, valueLocation);
                    int numKeyVal = numKeyValsBuffer.get(0) & ByteUtils.MASK_11111111;

                    if(numKeyVal == 1) {
                        // Avoid the overhead of reading the key and a
                        // comparison

                        // Read key size
                        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                        dataFile.read(sizeBuffer, valueLocation + 1);
                        int keySize = sizeBuffer.getInt(0);
                        sizeBuffer.clear();

                        // Read value size
                        dataFile.read(sizeBuffer, valueLocation + 1 + 4 + keySize);
                        int valueSize = sizeBuffer.getInt(0);

                        // Read value
                        ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                        dataFile.read(valueBuffer, valueLocation + 1 + 4 + 4 + keySize);
                        return valueBuffer.array();

                    } else {
                        int currentPosition = valueLocation + 1;
                        for(int keyId = 0; keyId < numKeyVal; keyId++) {
                            // Read key size
                            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                            dataFile.read(sizeBuffer, currentPosition);
                            int keySize = sizeBuffer.getInt(0);
                            sizeBuffer.clear();

                            // Read value size
                            dataFile.read(sizeBuffer, currentPosition + 4 + keySize);
                            int valueSize = sizeBuffer.getInt(0);

                            // Read key
                            ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                            dataFile.read(keyBuffer, currentPosition + 4);

                            if(ByteUtils.compare(keyBuffer.array(), key) == 0) {
                                // Read value
                                ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                                dataFile.read(valueBuffer, currentPosition + 4 + 4 + keySize);
                                return valueBuffer.array();
                            } else {
                                currentPosition += 4 + 4 + keySize + valueSize;
                            }
                        }
                        throw new VoldemortException("Could not find key " + Arrays.toString(key));
                    }

                }
                default: {
                    throw new VoldemortException("Storage format not supported ");
                }
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
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
        return this.dataFileSizes.get(chunk);
    }

}
