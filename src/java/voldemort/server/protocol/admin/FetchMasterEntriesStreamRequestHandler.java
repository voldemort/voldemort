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
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

/**
 * FetchMasterEntries fetches and returns key/value entry only if the partition
 * is the master.
 * <p>
 * For performance reason use storageEngine.keys() iterator to filter out
 * unwanted keys (first by checking correct partition and then passing it
 * through master filter) and then call storageEngine.get() for valid keys.
 * <p>
 */

public class FetchMasterEntriesStreamRequestHandler extends FetchStreamRequestHandler {

    public FetchMasterEntriesStreamRequestHandler(FetchPartitionEntriesRequest request,
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

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        ByteArray key = keyIterator.next();

        // Since Master-Only filter does not need value, we can save some disk
        // seeks by getting back only Master replica values
        if(validPartition(key.get()) && filter.accept(key, null) && counter % skipRecords == 0) {
            for(Versioned<byte[]> value: storageEngine.get(key, null)) {
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
