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

        ReadOnlyStorageEngine storageEngine = AdminServiceRequestHandler.getReadOnlyStorageEngine(metadataStore,
                                                                                                  storeRepository,
                                                                                                  request.getStore());

        HashMap<Integer, List<Integer>> localReplicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());

        // Filter the replica to partition mapping so as to include only till
        // the number of replicas
        this.replicaToPartitionList = Lists.newArrayList();
        for(int replicaType = 0; replicaType < storeDef.getReplicationFactor(); replicaType++) {
            if(localReplicaToPartitionList.containsKey(replicaType)) {
                List<Integer> partitionList = localReplicaToPartitionList.get(replicaType);
                for(Iterator<Integer> it = partitionList.iterator(); it.hasNext();) {
                    this.replicaToPartitionList.add(new Pair<Integer, Integer>(replicaType,
                                                                               it.next()));
                }
            }
        }

        this.blockSize = voldemortConfig.getAllProps()
                                        .getLong("partition.buffer.size.bytes",
                                                 voldemortConfig.getAdminSocketBufferSize());
        this.storeDir = new File(storageEngine.getCurrentDirPath());
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        this.stats = stats;
        this.handle = stats.makeHandle(StreamStats.Operation.FETCH_FILE,
                                       RebalanceUtils.flattenPartitionTuples(new HashSet<Pair<Integer, Integer>>(replicaToPartitionList)));
        this.partitionIterator = Collections.unmodifiableList(replicaToPartitionList).iterator();
        this.fetchStatus = FetchStatus.next_partition;
        this.currentChunkId = 0;
        this.indexFile = null;
        this.dataFile = null;
        this.chunkedFileWriter = null;
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
            case next_partition:
                handlerState = handleNextPartition();
                break;
            case send_data_file:
                handleSendDataFile(outputStream);
                break;
            case send_index_file:
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
            fetchStatus = FetchStatus.send_data_file;
        } else {
            // index file not done yet, keep sending
            fetchStatus = FetchStatus.send_index_file;
        }
    }

    private void handleSendDataFile(DataOutputStream outputStream) throws IOException {
        if(null == dataFile && null == indexFile) {
            // first time enter here, create files based on partition and
            // chunk ids
            String fileName = currentPair.getSecond().toString() + "_"
                              + currentPair.getFirst().toString() + "_"
                              + Integer.toString(currentChunkId);
            dataFile = new File(this.storeDir, fileName + ".data");
            indexFile = new File(this.storeDir, fileName + ".index");
            validateFiles();
            if(isPartitionFinished()) {
                // finished with this partition, move to next one
                fetchStatus = FetchStatus.next_partition;
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
            fetchStatus = FetchStatus.send_index_file;
        } else {
            // data file not done yet, keep sending
            fetchStatus = FetchStatus.send_data_file;
        }
    }

    private StreamRequestHandlerState handleNextPartition() {

        StreamRequestHandlerState handlerState = StreamRequestHandlerState.WRITING;

        if(partitionIterator.hasNext()) {
            // start a new partition
            currentPair = partitionIterator.next();
            currentChunkId = 0;
            dataFile = indexFile = null;
            fetchStatus = FetchStatus.send_data_file;
        } else {
            // we are done since we have gone through the entire
            // partition list
            logger.info("Finished streaming files for partitions: " + replicaToPartitionList);
            stats.closeHandle(handle);
            handlerState = StreamRequestHandlerState.COMPLETE;
        }

        return handlerState;
    }

    private void validateFiles() {
        if(!indexFile.exists() && !dataFile.exists() && 0 == currentChunkId) {
            throw new VoldemortException("Could not find any data for "
                                         + currentPair.getSecond().toString() + "_"
                                         + currentPair.getFirst().toString() + "_" + currentChunkId);
        }

        if(indexFile.exists() ^ dataFile.exists()) {
            throw new VoldemortException("One of the following does not exist: "
                                         + indexFile.toString() + " and " + dataFile.toString()
                                         + ".");
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
        next_partition,
        send_data_file,
        send_index_file
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
            outChannel.close();
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