package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
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

public class FetchPartitionFileStreamRequestHandler implements StreamRequestHandler {

    private final VAdminProto.FetchPartitionFilesRequest request;

    private final EventThrottler throttler;

    private final File storeDir;

    private final Logger logger = Logger.getLogger(getClass());

    private final long blockSize;

    private final StreamStats stats;

    private final StreamStats.Handle handle;

    private final ReadOnlyStorageEngine storageEngine;

    private final HashMap<Integer, List<Integer>> replicaToPartitionList;

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

        this.replicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());

        this.storageEngine = AdminServiceRequestHandler.getReadOnlyStorageEngine(metadataStore,
                                                                                 storeRepository,
                                                                                 request.getStore());

        this.blockSize = voldemortConfig.getAllProps()
                                        .getLong("partition.buffer.size.bytes",
                                                 voldemortConfig.getAdminSocketBufferSize());
        this.storeDir = new File(storageEngine.getCurrentDirPath());
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        this.stats = stats;
        this.handle = stats.makeHandle(StreamStats.Operation.FETCH_FILE, replicaToPartitionList);
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
        HashMap<Object, Integer> bucketToNumChunks = storageEngine.getChunkedFileSet()
                                                                  .getChunkIdToNumChunks();

        for(Entry<Integer, List<Integer>> entry: replicaToPartitionList.entrySet()) {

            int replicaType = entry.getKey();

            // Go over every partition for this replica type
            for(int partitionId: entry.getValue()) {

                // Check if this bucket exists
                if(!bucketToNumChunks.containsKey(Pair.create(partitionId, replicaType))) {
                    throw new VoldemortException("Bucket [ partition = " + partitionId
                                                 + ", replica = " + replicaType
                                                 + " ] does not exist for store "
                                                 + request.getStore());
                }

                // Get number of chunks for this bucket
                int numChunks = bucketToNumChunks.get(Pair.create(partitionId, replicaType));

                for(int chunkId = 0; chunkId < numChunks; chunkId++) {
                    String fileName = Integer.toString(partitionId) + "_"
                                      + Integer.toString(replicaType) + "_"
                                      + Integer.toString(chunkId);
                    File index = new File(this.storeDir, fileName + ".index");
                    File data = new File(this.storeDir, fileName + ".data");

                    // Both files in chunk exist, start streaming...
                    logger.info("Streaming " + data.getAbsolutePath());
                    streamFile(data, outputStream);
                    logger.info("Completed streaming " + data.getAbsolutePath());
                    logger.info("Streaming " + index.getAbsolutePath());
                    streamFile(index, outputStream);
                    logger.info("Completed streaming " + index.getAbsolutePath());
                    handle.incrementEntriesScanned();
                }

            }
        }
        stats.closeHandle(handle);
        return StreamRequestHandlerState.COMPLETE;
    }

    void streamFile(File fileToStream, DataOutputStream stream) throws IOException {
        FileChannel dataChannel = new FileInputStream(fileToStream).getChannel();
        try {
            VAdminProto.FileEntry response = VAdminProto.FileEntry.newBuilder()
                                                                  .setFileName(fileToStream.getName())
                                                                  .setFileSizeBytes(dataChannel.size())
                                                                  .build();
            // Write header
            ProtoUtils.writeMessage(stream, response);
            throttler.maybeThrottle(response.getSerializedSize());

            // Write rest of file
            WritableByteChannel channelOut = Channels.newChannel(stream);

            // Send chunks to help with throttling
            boolean completedFile = false;
            long chunkSize = 0;
            for(long chunkStart = 0; chunkStart < dataChannel.size() && !completedFile; chunkStart += blockSize) {
                if(dataChannel.size() - chunkStart < blockSize) {
                    chunkSize = dataChannel.size() - chunkStart;
                    completedFile = true;
                } else {
                    chunkSize = blockSize;
                }
                dataChannel.transferTo(chunkStart, chunkSize, channelOut);
                throttler.maybeThrottle((int) chunkSize);
            }
        } finally {
            if(dataChannel != null) {
                dataChannel.close();
            }
        }
    }
}
