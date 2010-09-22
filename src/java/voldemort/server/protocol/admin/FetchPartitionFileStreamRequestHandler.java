package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

public class FetchPartitionFileStreamRequestHandler implements StreamRequestHandler {

    private final VAdminProto.FetchPartitionFilesRequest request;

    private final ErrorCodeMapper errorCodeMapper;

    private final EventThrottler throttler;

    private final ReadOnlyStorageEngine storageEngine;

    private final long startTime;

    private final Logger logger = Logger.getLogger(getClass());

    private final boolean isReadOnly;

    protected FetchPartitionFileStreamRequestHandler(VAdminProto.FetchPartitionFilesRequest request,
                                                     MetadataStore metadataStore,
                                                     ErrorCodeMapper errorCodeMapper,
                                                     VoldemortConfig voldemortConfig,
                                                     StoreRepository storeRepository) {
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        this.isReadOnly = metadataStore.getStoreDef(request.getStore())
                                       .getType()
                                       .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
        this.storageEngine = (ReadOnlyStorageEngine) AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                                                 request.getStore());
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        this.startTime = System.currentTimeMillis();
    }

    public StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    public final void close(DataOutputStream outputStream) throws IOException {
        if(null != keyIterator)
            keyIterator.close();

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
        if(!isReadOnly) {

        }
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        ByteArray key = keyIterator.next();

        // Since Master-Only filter does not need value, we can save some disk
        // seeks by getting back only Master replica values
        if(validPartition(key.get()) && filter.accept(key, null)) {
            for(Versioned<byte[]> value: storageEngine.get(key)) {
                throttler.maybeThrottle(key.length());
                fetched++;
                VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();

                VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                      .setKey(ProtoUtils.encodeBytes(key))
                                                                                      .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                      .build();
                response.setPartitionEntry(partitionEntry);

                Message message = response.build();
                ProtoUtils.writeMessage(outputStream, message);

                throttler.maybeThrottle(AdminServiceRequestHandler.valueSize(value));
            }
        }

        // log progress
        counter++;

        if(0 == counter % 100000) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            if(logger.isDebugEnabled())
                logger.debug("fetchMasterEntries() scanned " + counter + " entries, fetched "
                             + fetched + " entries for store:" + storageEngine.getName()
                             + " partition:" + partitionList + " in " + totalTime + " s");
        }

        if(keyIterator.hasNext())
            return StreamRequestHandlerState.WRITING;
        else
            return StreamRequestHandlerState.COMPLETE;
    }

}
