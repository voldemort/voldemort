package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.stats.StreamStats;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.Lists;

public class FetchPartitionFileStreamRequestHandler implements StreamRequestHandler {

    private final VAdminProto.FetchPartitionFilesRequest request;

    private final EventThrottler throttler;

    private final File storeDir;

    private final Logger logger = Logger.getLogger(getClass());

    private final long blockSize;

    private final StreamStats stats;

    private final StreamStats.Handle handle;

    private final Iterator<Pair<Integer, Integer>> partitionIterator;

    private FetchStatus fetchStatus;

    private int currentChunkId;

    private Pair<Integer, Integer> currentPair;

    private File indexFile;

    private File dataFile;

    private ChunkedFileWriter chunkedFileWriter;

    private final List<Pair<Integer, Integer>> replicaToPartitionList;

    private final HashMap<Object, Integer> bucketToNumChunks;

    private final boolean nioEnabled;

    protected FetchPartitionFileStreamRequestHandler(VAdminProto.FetchPartitionFilesRequest request,
                                                     MetadataStore metadataStore,
                                                     VoldemortConfig voldemortConfig,
                                                     StoreRepository storeRepository,
                                                     StreamStats stats) {
        this.request = request;
        StoreDefinition storeDef = metadataStore.getStoreDef(request.getStore());
        boolean isReadOnly = storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
        if(!isReadOnly) {
            throw new VoldemortException("Should be fetching partition files only for read-only stores");
        }

        HashMap<Integer, List<Integer>> localReplicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());

        // Filter the replica to partition mapping so as to include only till
        // the number of replicas
        this.replicaToPartitionList = Lists.newArrayList();
        for(Entry<Integer, List<Integer>> entry: localReplicaToPartitionList.entrySet()) {
            for(Iterator<Integer> it = entry.getValue().iterator(); it.hasNext();) {
                this.replicaToPartitionList.add(new Pair<Integer, Integer>(entry.getKey(),
                                                                           it.next()));
            }
        }

        ReadOnlyStorageEngine storageEngine = AdminServiceRequestHandler.getReadOnlyStorageEngine(metadataStore,
                                                                                                  storeRepository,
                                                                                                  request.getStore());
        this.bucketToNumChunks = storageEngine.getChunkedFileSet().getChunkIdToNumChunks();
        this.blockSize = voldemortConfig.getAllProps()
                                        .getLong("partition.buffer.size.bytes",
                                                 voldemortConfig.getAdminSocketBufferSize());
        this.storeDir = new File(storageEngine.getCurrentDirPath());
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        this.stats = stats;
        this.handle = stats.makeHandle(StreamStats.Operation.FETCH_FILE,
                                       RebalanceUtils.flattenPartitionTuples(new HashSet<Pair<Integer, Integer>>(replicaToPartitionList)));
        this.partitionIterator = Collections.unmodifiableList(replicaToPartitionList).iterator();
        this.fetchStatus = FetchStatus.NEXT_PARTITION;
        this.currentChunkId = 0;
        this.indexFile = null;
        this.dataFile = null;
        this.chunkedFileWriter = null;
        this.nioEnabled = voldemortConfig.getUseNioConnector();
    }

    public StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    public final void close(DataOutputStream outputStream) throws IOException {
        ProtoUtils.writeEndOfStream(outputStream);
    }

    public final void handleError(DataOutputStream outputStream, VoldemortException e)
            throws IOException {
        logger.error("handleFetchPartitionFilesEntries failed for request(" + request.toString()
                     + ")", e);
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        StreamRequestHandlerState handlerState = StreamRequestHandlerState.WRITING;

        switch(fetchStatus) {
            case NEXT_PARTITION:
                handlerState = handleNextPartition();
                break;
            case SEND_DATA_FILE:
                handleSendDataFile(outputStream);
                break;
            case SEND_INDEX_FILE:
                handleSendIndexFile();
                break;
            default:
                throw new VoldemortException("Invalid fetch status: " + fetchStatus);
        }

        return handlerState;
    }

    private void handleSendIndexFile() throws IOException {
        if(0 == chunkedFileWriter.streamFile()) {
            // we are done with the index file, move to next chunk
            logger.info("Completed streaming " + indexFile.getAbsolutePath());
            this.chunkedFileWriter.close();
            currentChunkId++;
            dataFile = indexFile = null;
            handle.incrementEntriesScanned();
            fetchStatus = FetchStatus.SEND_DATA_FILE;
        } else {
            // index file not done yet, keep sending
            fetchStatus = FetchStatus.SEND_INDEX_FILE;
        }
    }

    private void handleSendDataFile(DataOutputStream outputStream) throws IOException {
        if(null == dataFile && null == indexFile) {
            // first time enter here, create files based on partition and
            // chunk ids
            String fileName = currentPair.getSecond().toString() + "_"
                              + currentPair.getFirst().toString() + "_"
                              + Integer.toString(currentChunkId);
            this.dataFile = new File(this.storeDir, fileName + ".data");
            this.indexFile = new File(this.storeDir, fileName + ".index");
            validateFiles(currentPair);
            if(isPartitionFinished()) {
                // finished with this partition, move to next one
                fetchStatus = FetchStatus.NEXT_PARTITION;
                return;
            }

            // create a new writer for data file
            this.chunkedFileWriter = new ChunkedFileWriter(dataFile, outputStream);
            logger.info("Streaming " + dataFile.getAbsolutePath());
            this.chunkedFileWriter.writeHeader();
        }

        if(0 == chunkedFileWriter.streamFile()) {
            // we are done with the data file, move to index file
            logger.info("Completed streaming " + dataFile.getAbsolutePath());
            this.chunkedFileWriter.close();
            this.chunkedFileWriter = new ChunkedFileWriter(indexFile, outputStream);
            logger.info("Streaming " + indexFile.getAbsolutePath());
            this.chunkedFileWriter.writeHeader();
            fetchStatus = FetchStatus.SEND_INDEX_FILE;
        } else {
            // data file not done yet, keep sending
            fetchStatus = FetchStatus.SEND_DATA_FILE;
        }
    }

    private StreamRequestHandlerState handleNextPartition() {

        StreamRequestHandlerState handlerState = StreamRequestHandlerState.WRITING;

        if(partitionIterator.hasNext()) {
            // start a new partition
            currentPair = partitionIterator.next();
            currentChunkId = 0;
            dataFile = indexFile = null;
            fetchStatus = FetchStatus.SEND_DATA_FILE;
        } else {
            // we are done since we have gone through the entire
            // partition list
            logger.info("Finished streaming files for partitions: " + replicaToPartitionList);
            stats.closeHandle(handle);
            handlerState = StreamRequestHandlerState.COMPLETE;
        }

        return handlerState;
    }

    private void validateFiles(Pair<Integer, Integer> replicaToPartitionTuple) {
        if(!bucketToNumChunks.containsKey(Pair.create(replicaToPartitionTuple.getSecond(),
                                                      replicaToPartitionTuple.getFirst()))) {
            throw new VoldemortException("Bucket [ partition = "
                                         + replicaToPartitionTuple.getSecond() + ", replica = "
                                         + replicaToPartitionTuple.getFirst()
                                         + " ] does not exist for store " + request.getStore());
        }
    }

    private boolean isPartitionFinished() {
        boolean finished = false;
        if(!indexFile.exists() && !dataFile.exists() && currentChunkId > 0) {
            finished = true;
        }
        return finished;
    }

    enum FetchStatus {
        NEXT_PARTITION,
        SEND_DATA_FILE,
        SEND_INDEX_FILE
    }

    class ChunkedFileWriter {

        private final File fileToWrite;
        private final DataOutputStream outStream;
        private final FileChannel dataChannel;
        private final WritableByteChannel outChannel;
        private long currentPos;

        ChunkedFileWriter(File fileToWrite, DataOutputStream stream) throws FileNotFoundException {
            this.fileToWrite = fileToWrite;
            this.outStream = stream;
            this.dataChannel = new FileInputStream(fileToWrite).getChannel();
            this.outChannel = Channels.newChannel(outStream);
            this.currentPos = 0;
        }

        public void close() throws IOException {
            dataChannel.close();
            if(nioEnabled) {
                outChannel.close();
            }
        }

        void writeHeader() throws IOException {
            VAdminProto.FileEntry response = VAdminProto.FileEntry.newBuilder()
                                                                  .setFileName(fileToWrite.getName())
                                                                  .setFileSizeBytes(dataChannel.size())
                                                                  .build();

            ProtoUtils.writeMessage(outStream, response);
            throttler.maybeThrottle(response.getSerializedSize());
        }

        // this function returns the number of bytes left, 0 means done
        long streamFile() throws IOException {
            long bytesRemaining = dataChannel.size() - currentPos;
            if(0 < bytesRemaining) {
                long bytesToWrite = Math.min(bytesRemaining, blockSize);
                long bytesWritten = dataChannel.transferTo(currentPos, bytesToWrite, outChannel);
                currentPos += bytesWritten;
                logger.info(bytesWritten + " of bytes sent");
                throttler.maybeThrottle((int) bytesWritten);
            }
            bytesRemaining = dataChannel.size() - currentPos;
            return bytesRemaining;
        }
    }
}
