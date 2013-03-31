package voldemort.store.readonly.chunk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.io.MappedFileReader;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * A set of chunked data and index files for a read-only store
 * 
 * 
 */
public class ChunkedFileSet {

    private static Logger logger = Logger.getLogger(ChunkedFileSet.class);

    private final int numChunks;
    private final int nodeId;
    private final File baseDir;
    private final List<Integer> indexFileSizes;
    private final List<Integer> dataFileSizes;
    private final List<MappedByteBuffer> indexFiles;

    private List<MappedFileReader> mappedIndexFileReader;
    private final List<FileChannel> dataFiles;
    private final HashMap<Object, Integer> chunkIdToChunkStart;
    private final HashMap<Object, Integer> chunkIdToNumChunks;
    private ArrayList<Integer> nodePartitionIds;
    private RoutingStrategy routingStrategy;
    private ReadOnlyStorageFormat storageFormat;

    private boolean enforceMlock = false;

    public ChunkedFileSet(File directory,
                          RoutingStrategy routingStrategy,
                          int nodeId,
                          boolean enforceMlock) {

        this.enforceMlock = enforceMlock;
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
        this.mappedIndexFileReader = new ArrayList<MappedFileReader>();

        this.dataFiles = new ArrayList<FileChannel>();
        this.chunkIdToChunkStart = new HashMap<Object, Integer>();
        this.chunkIdToNumChunks = new HashMap<Object, Integer>();
        this.nodeId = nodeId;
        setRoutingStrategy(routingStrategy);

        switch(storageFormat) {
            case READONLY_V0:
                initVersion0();
                break;
            case READONLY_V1:
                initVersion1();
                break;
            case READONLY_V2:
                initVersion2();
                break;
            default:
                throw new VoldemortException("Invalid chunked storage format type " + storageFormat);
        }

        this.numChunks = indexFileSizes.size();
        logger.trace("Opened chunked file set for " + baseDir + " with " + indexFileSizes.size()
                     + " chunks and format  " + storageFormat);
    }

    public ChunkedFileSet(File directory, RoutingStrategy routingStrategy, int nodeId) {
        this(directory, routingStrategy, nodeId, false);

    }

    public DataFileChunkSet toDataFileChunkSet() {

        // Convert the index file into chunk set
        List<DataFileChunk> dataFileChunks = Lists.newArrayList();
        for(FileChannel dataFile: dataFiles) {
            dataFileChunks.add(new LocalDataFileChunk(dataFile));
        }

        return new DataFileChunkSet(dataFileChunks, this.dataFileSizes);
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

            MappedFileReader idxFileReader = null;
            try {
                idxFileReader = new MappedFileReader(index);
                mappedIndexFileReader.add(idxFileReader);
                indexFiles.add(idxFileReader.map(enforceMlock));
            } catch(IOException e) {

                logger.error("Error in mlock", e);
            }

            // indexFiles.add(mapFile(index));
            chunkId++;
        }
        if(chunkId == 0)
            throw new VoldemortException("No data chunks found in directory " + baseDir.toString());
    }

    public void initVersion1() {
        int globalChunkId = 0;
        if(this.nodePartitionIds != null) {
            for(Integer partitionId: this.nodePartitionIds) {
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
                                logger.info("No index or data files found, creating empty chunk files for partition "
                                            + partitionId);
                            } catch(IOException e) {
                                throw new VoldemortException("Error creating empty read-only files for partition "
                                                                     + partitionId,
                                                             e);
                            }
                        } else {
                            break;
                        }
                    } else if(index.exists() ^ data.exists()) {
                        throw new VoldemortException("One of the following does not exist: "
                                                     + index.toString() + " or " + data.toString()
                                                     + ".");
                    }

                    if(chunkId == 0)
                        chunkIdToChunkStart.put(partitionId, globalChunkId);

                    /* Deal with file sizes */
                    long indexLength = index.length();
                    long dataLength = data.length();
                    validateFileSizes(indexLength, dataLength);
                    indexFileSizes.add((int) indexLength);
                    dataFileSizes.add((int) dataLength);

                    /* Add the file channel for data */
                    dataFiles.add(openChannel(data));

                    MappedFileReader idxFileReader = null;
                    try {
                        idxFileReader = new MappedFileReader(index);
                        mappedIndexFileReader.add(idxFileReader);
                        indexFiles.add(idxFileReader.map(enforceMlock));
                    } catch(IOException e) {
                        logger.error("Error in mlock", e);
                    }

                    // indexFiles.add(mapFile(index));
                    chunkId++;
                    globalChunkId++;
                }
                chunkIdToNumChunks.put(partitionId, chunkId);
            }

            if(indexFileSizes.size() == 0)
                throw new VoldemortException("No chunk files found in directory "
                                             + baseDir.toString());
        }
    }

    public void initVersion2() {
        int globalChunkId = 0;
        if(this.nodePartitionIds != null) {

            // Go over every partition and find out all buckets ( pair of master
            // partition id and replica type )
            for(Node node: routingStrategy.getNodes()) {
                for(int partitionId: node.getPartitionIds()) {

                    List<Integer> routingPartitionList = routingStrategy.getReplicatingPartitionList(partitionId);

                    // Find intersection with nodes partition ids
                    Pair<Integer, Integer> bucket = null;
                    for(int replicaType = 0; replicaType < routingPartitionList.size(); replicaType++) {

                        if(nodePartitionIds.contains(routingPartitionList.get(replicaType))) {
                            if(bucket == null) {

                                // Generate bucket information
                                bucket = Pair.create(routingPartitionList.get(0), replicaType);

                                int chunkId = 0;
                                while(true) {
                                    String fileName = Integer.toString(bucket.getFirst()) + "_"
                                                      + Integer.toString(bucket.getSecond()) + "_"
                                                      + Integer.toString(chunkId);
                                    File index = new File(baseDir, fileName + ".index");
                                    File data = new File(baseDir, fileName + ".data");

                                    if(!index.exists() && !data.exists()) {
                                        if(chunkId == 0) {
                                            // create empty files for this
                                            // partition
                                            try {
                                                new File(baseDir, fileName + ".index").createNewFile();
                                                new File(baseDir, fileName + ".data").createNewFile();
                                                logger.info("No index or data files found, creating empty files for partition "
                                                            + routingPartitionList.get(0)
                                                            + " and replica type " + replicaType);
                                            } catch(IOException e) {
                                                throw new VoldemortException("Error creating empty read-only files for partition "
                                                                                     + routingPartitionList.get(0)
                                                                                     + " and replica type "
                                                                                     + replicaType,
                                                                             e);
                                            }
                                        } else {
                                            break;
                                        }
                                    } else if(index.exists() ^ data.exists()) {
                                        throw new VoldemortException("One of the following does not exist: "
                                                                     + index.toString()
                                                                     + " or "
                                                                     + data.toString() + ".");
                                    }

                                    if(chunkId == 0) {
                                        chunkIdToChunkStart.put(bucket, globalChunkId);
                                    }

                                    /* Deal with file sizes */
                                    long indexLength = index.length();
                                    long dataLength = data.length();
                                    validateFileSizes(indexLength, dataLength);
                                    indexFileSizes.add((int) indexLength);
                                    dataFileSizes.add((int) dataLength);

                                    /* Add the file channel for data */
                                    dataFiles.add(openChannel(data));

                                    MappedFileReader idxFileReader = null;
                                    try {
                                        idxFileReader = new MappedFileReader(index);
                                        mappedIndexFileReader.add(idxFileReader);
                                        indexFiles.add(idxFileReader.map(enforceMlock));
                                    } catch(IOException e) {
                                        logger.error("Error in mlock", e);
                                    }

                                    // indexFiles.add(mapFile(index));
                                    chunkId++;
                                    globalChunkId++;
                                }
                                chunkIdToNumChunks.put(bucket, chunkId);
                            } else {
                                throw new VoldemortException("Found a collision for master partition for bucket named "
                                                             + bucket);
                            }
                        }
                    }
                }
            }

            if(indexFileSizes.size() == 0)
                throw new VoldemortException("No chunk files found in directory "
                                             + baseDir.toString());
        }
    }

    /**
     * Get the chunk id to num chunks mapping
     * 
     * @return Map of chunk id to number of chunks
     */
    public HashMap<Object, Integer> getChunkIdToNumChunks() {
        return this.chunkIdToNumChunks;
    }

    private void setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;

        // generate partitions list
        this.nodePartitionIds = null;
        for(Node node: routingStrategy.getNodes()) {
            if(node.getId() == this.nodeId) {
                this.nodePartitionIds = new ArrayList<Integer>();
                this.nodePartitionIds.addAll(node.getPartitionIds());
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

            MappedFileReader idxFileReader = mappedIndexFileReader.get(chunk);
            try {
                idxFileReader.close();
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
                return ReadOnlyUtils.chunk(ByteUtils.md5(key), numChunks);
            }
            case READONLY_V1: {
                List<Integer> routingPartitionList = routingStrategy.getPartitionList(key);
                routingPartitionList.retainAll(nodePartitionIds);

                if(routingPartitionList.size() != 1) {
                    return -1;
                }

                return chunkIdToChunkStart.get(routingPartitionList.get(0))
                       + ReadOnlyUtils.chunk(ByteUtils.md5(key),
                                             chunkIdToNumChunks.get(routingPartitionList.get(0)));
            }
            case READONLY_V2: {
                List<Integer> routingPartitionList = routingStrategy.getPartitionList(key);

                Pair<Integer, Integer> bucket = null;
                for(int replicaType = 0; replicaType < routingPartitionList.size(); replicaType++) {

                    if(nodePartitionIds.contains(routingPartitionList.get(replicaType))) {
                        if(bucket == null) {
                            bucket = Pair.create(routingPartitionList.get(0), replicaType);
                        } else {
                            return -1;
                        }
                    }
                }

                if(bucket == null)
                    return -1;

                return chunkIdToChunkStart.get(bucket)
                       + ReadOnlyUtils.chunk(ByteUtils.md5(key), chunkIdToNumChunks.get(bucket));
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
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_INT);
                    dataFile.read(sizeBuffer, valueLocation);
                    int valueSize = sizeBuffer.getInt(0);

                    // Read value
                    ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                    dataFile.read(valueBuffer, valueLocation + ByteUtils.SIZE_OF_INT);
                    return valueBuffer.array();
                }
                case READONLY_V2: {

                    // Buffer for 'numKeyValues', 'keySize' and 'valueSize'
                    int headerSize = ByteUtils.SIZE_OF_SHORT + (2 * ByteUtils.SIZE_OF_INT);
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(headerSize);
                    dataFile.read(sizeBuffer, valueLocation);
                    valueLocation += headerSize;

                    // Read the number of key-values
                    short numKeyValues = sizeBuffer.getShort(0);

                    // Read the key size
                    int keySize = sizeBuffer.getInt(ByteUtils.SIZE_OF_SHORT);

                    // Read the value size
                    int valueSize = sizeBuffer.getInt(ByteUtils.SIZE_OF_SHORT
                                                      + ByteUtils.SIZE_OF_INT);

                    do {

                        if(keySize == -1 && valueSize == -1) {
                            sizeBuffer.clear();
                            // Reads an extra short, but that is fine since
                            // collisions are rare. Also we save the unnecessary
                            // overhead of allocating a new byte-buffer
                            dataFile.read(sizeBuffer, valueLocation);
                            keySize = sizeBuffer.getInt(0);
                            valueSize = sizeBuffer.getInt(ByteUtils.SIZE_OF_INT);
                            valueLocation += (2 * ByteUtils.SIZE_OF_INT);
                        }

                        // Read key + value
                        ByteBuffer buffer = ByteBuffer.allocate(keySize + valueSize);
                        dataFile.read(buffer, valueLocation);

                        // Compare key
                        if(ByteUtils.compare(key, buffer.array(), 0, keySize) == 0) {
                            return ByteUtils.copy(buffer.array(), keySize, keySize + valueSize);
                        }
                        valueLocation += (keySize + valueSize);
                        keySize = valueSize = -1;

                    } while(--numKeyValues > 0);
                    // Could not find key, return value of no size
                    return new byte[0];
                }

                default: {
                    throw new VoldemortException("Storage format not supported ");
                }
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    /**
     * Iterator for RO keys - Works only for ReadOnlyStorageFormat.READONLY_V2
     */
    public static class ROKeyIterator extends DataFileChunkSetIterator<ByteArray> {

        public ROKeyIterator(ChunkedFileSet chunkedFileSet, ReadWriteLock modificationLock) {
            super(chunkedFileSet.toDataFileChunkSet(), false, modificationLock);
        }

        @Override
        public ByteArray next() {
            if(!hasNext())
                throw new VoldemortException("Reached the end");

            try {
                // Read key size and value size
                ByteBuffer sizeBuffer = ByteBuffer.allocate(2 * ByteUtils.SIZE_OF_INT);
                getCurrentChunk().read(sizeBuffer, getCurrentOffsetInChunk());
                int keySize = sizeBuffer.getInt(0);
                int valueSize = sizeBuffer.getInt(ByteUtils.SIZE_OF_INT);

                // Read the key contents
                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                getCurrentChunk().read(keyBuffer,
                                       getCurrentOffsetInChunk() + (2 * ByteUtils.SIZE_OF_INT));

                // Update the offset
                updateOffset(getCurrentOffsetInChunk() + (2 * ByteUtils.SIZE_OF_INT) + keySize
                             + valueSize);

                // Return the key
                return new ByteArray(keyBuffer.array());
            } catch(IOException e) {
                logger.error(e);
                throw new VoldemortException(e);
            }

        }
    }

    /**
     * Iterator for RO entries - Works only for
     * ReadOnlyStorageFormat.READONLY_V2
     */
    public static class ROEntriesIterator extends
            DataFileChunkSetIterator<Pair<ByteArray, Versioned<byte[]>>> {

        public ROEntriesIterator(ChunkedFileSet chunkedFileSet, ReadWriteLock modificationLock) {
            super(chunkedFileSet.toDataFileChunkSet(), false, modificationLock);
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(!hasNext())
                throw new VoldemortException("Reached the end");

            try {
                // Read key size and value size
                ByteBuffer sizeBuffer = ByteBuffer.allocate(2 * ByteUtils.SIZE_OF_INT);
                getCurrentChunk().read(sizeBuffer, getCurrentOffsetInChunk());
                int keySize = sizeBuffer.getInt(0);
                int valueSize = sizeBuffer.getInt(ByteUtils.SIZE_OF_INT);

                // Read the key and value contents at once
                ByteBuffer keyAndValueBuffer = ByteBuffer.allocate(keySize + valueSize);
                getCurrentChunk().read(keyAndValueBuffer,
                                       getCurrentOffsetInChunk() + (2 * ByteUtils.SIZE_OF_INT));

                // Update the offset
                updateOffset(getCurrentOffsetInChunk() + (2 * ByteUtils.SIZE_OF_INT) + keySize
                             + valueSize);

                return Pair.create(new ByteArray(ByteUtils.copy(keyAndValueBuffer.array(),
                                                                0,
                                                                keySize)),
                                   Versioned.value(ByteUtils.copy(keyAndValueBuffer.array(),
                                                                  keySize,
                                                                  keySize + valueSize)));
            } catch(IOException e) {
                logger.error(e);
                throw new VoldemortException(e);
            }
        }
    }

    /**
     * Iterator over top 8 bytes of md5(key) and all collided entries (
     * including the number of entries )
     * 
     * Works only for ReadOnlyStorageFormat.READONLY_V2
     */
    public static class ROCollidedEntriesIterator extends
            DataFileChunkSetIterator<Pair<ByteBuffer, ByteBuffer>> {

        private MessageDigest md5er = ByteUtils.getDigest("md5");

        public ROCollidedEntriesIterator(DataFileChunkSet dataFileChunkSet) {
            this(dataFileChunkSet, new ReentrantReadWriteLock());
        }

        public ROCollidedEntriesIterator(DataFileChunkSet dataFileChunkSet,
                                         ReadWriteLock modificationLock) {
            super(dataFileChunkSet, true, modificationLock);
        }

        @Override
        public Pair<ByteBuffer, ByteBuffer> next() {
            if(!hasNext())
                throw new VoldemortException("Reached the end");

            try {
                // Initialize buffer for key
                ByteBuffer keyBuffer = ByteBuffer.allocate(2 * ByteUtils.SIZE_OF_INT);

                // Update the tuple count
                ByteBuffer numKeyValsBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_SHORT);
                getCurrentChunk().read(numKeyValsBuffer, getCurrentOffsetInChunk());
                short tupleCount = numKeyValsBuffer.getShort(0);

                int offsetMoved = ByteUtils.SIZE_OF_SHORT;
                ByteBuffer keyValueLength = ByteBuffer.allocate(2 * ByteUtils.SIZE_OF_INT);
                for(int tupleId = 0; tupleId < tupleCount; tupleId++) {
                    // Reads key length, value length
                    getCurrentChunk().read(keyValueLength, getCurrentOffsetInChunk() + offsetMoved);
                    int keyLength = keyValueLength.getInt(0);
                    int valueLength = keyValueLength.getInt(ByteUtils.SIZE_OF_INT);

                    if(keyBuffer.hasRemaining()) {
                        // We are filling the keyBuffer for the first time
                        ByteBuffer tempKeyBuffer = ByteBuffer.allocate(keyLength);
                        getCurrentChunk().read(tempKeyBuffer,
                                               getCurrentOffsetInChunk() + keyLength + valueLength);
                        keyBuffer.put(ByteUtils.copy(md5er.digest(tempKeyBuffer.array()),
                                                     0,
                                                     2 * ByteUtils.SIZE_OF_INT));
                    }
                    offsetMoved += (2 * ByteUtils.SIZE_OF_INT) + keyLength + valueLength;
                    keyValueLength.clear();
                }

                ByteBuffer finalValue = ByteBuffer.allocate(offsetMoved);
                getCurrentChunk().read(finalValue, getCurrentOffsetInChunk());

                // Update the offset
                updateOffset(getCurrentOffsetInChunk() + offsetMoved);

                return Pair.create(keyBuffer, finalValue);
            } catch(IOException e) {
                logger.error(e);
                throw new VoldemortException(e);
            } finally {
                md5er.reset();
            }
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
