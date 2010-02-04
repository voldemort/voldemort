package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.NetworkClassLoader;

import com.google.protobuf.Message;

public class FetchKeysStreamRequestHandler extends FetchStreamRequestHandler {

    public FetchKeysStreamRequestHandler(FetchPartitionEntriesRequest request,
                                         MetadataStore metadataStore,
                                         ErrorCodeMapper errorCodeMapper,
                                         VoldemortConfig voldemortConfig,
                                         StoreRepository storeRepository,
                                         NetworkClassLoader networkClassLoader) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader);
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        ByteArray key = keyIterator.next();

        if(validPartition(key.get()) && filter.accept(key, null)) {
            VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
            response.setKey(ProtoUtils.encodeBytes(key));

            fetched++;
            Message message = response.build();
            ProtoUtils.writeMessage(outputStream, message);

            throttler.maybeThrottle(key.length());
        }

        // log progress
        counter++;

        if(0 == counter % 100000) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            if(logger.isDebugEnabled())
                logger.debug("fetchKeys() scanned " + counter + " keys, fetched " + fetched
                             + " keys for store:" + storageEngine.getName() + " partition:"
                             + partitionList + " in " + totalTime + " s");
        }

        if(keyIterator.hasNext())
            return StreamRequestHandlerState.WRITING;
        else
            return StreamRequestHandlerState.COMPLETE;
    }

}
