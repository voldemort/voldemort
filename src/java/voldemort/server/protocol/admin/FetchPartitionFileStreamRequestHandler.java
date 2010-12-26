package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.utils.EventThrottler;

public class FetchPartitionFileStreamRequestHandler implements StreamRequestHandler {

    private final VAdminProto.FetchPartitionFilesRequest request;

    private final EventThrottler throttler;

    private final File storeDir;

    private final Logger logger = Logger.getLogger(getClass());

    private final long blockSize;

    protected FetchPartitionFileStreamRequestHandler(VAdminProto.FetchPartitionFilesRequest request,
                                                     MetadataStore metadataStore,
                                                     VoldemortConfig voldemortConfig,
                                                     StoreRepository storeRepository) {
        this.request = request;
        boolean isReadOnly = metadataStore.getStoreDef(request.getStore())
                                          .getType()
                                          .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
        if(!isReadOnly) {
            throw new VoldemortException("Should be fetching partition files only for read-only stores");
        }

        ReadOnlyStorageEngine storageEngine = AdminServiceRequestHandler.getReadOnlyStorageEngine(metadataStore,
                                                                                 storeRepository,
                                                                                 request.getStore());

        this.blockSize = voldemortConfig.getAllProps()
                                        .getLong("partition.buffer.size.bytes",
                                                 voldemortConfig.getAdminSocketBufferSize());
        this.storeDir = new File(storageEngine.getCurrentDirPath());
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
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
        List<Integer> partitionIds = request.getPartitionsList();

        for(Integer partitionId: partitionIds) {
            int chunkId = 0;
            while(true) {
                String fileName = Integer.toString(partitionId) + "_" + Integer.toString(chunkId);
                File index = new File(this.storeDir, fileName + ".index");
                File data = new File(this.storeDir, fileName + ".data");
                if(!index.exists() && !data.exists()) {
                    if(chunkId == 0) {
                        throw new VoldemortException("Could not find any data for partition "
                                                     + partitionId);
                    } else {
                        break;
                    }
                } else if(index.exists() ^ data.exists())
                    throw new VoldemortException("One of the following does not exist: "
                                                 + index.toString() + " and " + data.toString()
                                                 + ".");

                // Both files in chunk exist, start streaming...
                logger.info("Streaming " + data.getAbsolutePath());
                streamFile(data, outputStream);
                logger.info("Completed streaming " + data.getAbsolutePath());
                logger.info("Streaming " + index.getAbsolutePath());
                streamFile(index, outputStream);
                logger.info("Completed streaming " + index.getAbsolutePath());

                chunkId++;
            }
        }

        return StreamRequestHandlerState.COMPLETE;
    }

    void streamFile(File fileToStream, DataOutputStream stream) throws IOException {
        FileChannel dataChannel = new FileInputStream(fileToStream).getChannel();
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
    }
}
