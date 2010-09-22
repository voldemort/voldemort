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
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.utils.EventThrottler;

public class FetchPartitionFileStreamRequestHandler implements StreamRequestHandler {

    private final VAdminProto.FetchPartitionFilesRequest request;

    private final ErrorCodeMapper errorCodeMapper;

    private final EventThrottler throttler;

    private final ReadOnlyStorageEngine storageEngine;

    private final long startTime;

    private final File storeDir;

    private final Logger logger = Logger.getLogger(getClass());

    protected FetchPartitionFileStreamRequestHandler(VAdminProto.FetchPartitionFilesRequest request,
                                                     MetadataStore metadataStore,
                                                     ErrorCodeMapper errorCodeMapper,
                                                     VoldemortConfig voldemortConfig,
                                                     StoreRepository storeRepository) {
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        boolean isReadOnly = metadataStore.getStoreDef(request.getStore())
                                          .getType()
                                          .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
        if(!isReadOnly) {
            throw new VoldemortException("Should be fetching partition files only for read-only stores");
        }

        this.storageEngine = (ReadOnlyStorageEngine) AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                                                 request.getStore());
        this.storeDir = new File(storageEngine.getStoreDirPath());
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        this.startTime = System.currentTimeMillis();
    }

    public StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    public final void close(DataOutputStream outputStream) throws IOException {
        ProtoUtils.writeEndOfStream(outputStream);
    }

    public final void handleError(DataOutputStream outputStream, VoldemortException e)
            throws IOException {
        VAdminProto.FetchPartitionEntriesResponse response = VAdminProto.FetchPartitionEntriesResponse.newBuilder()
                                                                                                      .setError(ProtoUtils.encodeError(errorCodeMapper,
                                                                                                                                       e))
                                                                                                      .build();

        ProtoUtils.writeMessage(outputStream, response);
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
                // Stream data file...
                FileChannel channel = new FileInputStream(index).getChannel();
                WritableByteChannel channelOut = Channels.newChannel(outputStream);
                channel.transferTo(0, 10L, channelOut);

                // Stream index file...

                chunkId++;
            }
        }

        return null;
    }

}
