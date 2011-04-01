package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamStats;
import voldemort.store.stats.StreamStats.Operation;
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
                                                  NetworkClassLoader networkClassLoader,
                                                  StreamStats stats) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader,
              stats,
              Operation.FETCH_ENTRIES);
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        long startNs = System.nanoTime();
        ByteArray key = keyIterator.next();

        // Since Master-Only filter does not need value, we can save some disk
        // seeks by getting back only Master replica values
        if(validPartition(key.get()) && filter.accept(key, null) && counter % skipRecords == 0) {
            List<Versioned<byte[]>> values = storageEngine.get(key, null);
            stats.recordDiskTime(handle, System.nanoTime() - startNs);

            for(Versioned<byte[]> value: values) {
                throttler.maybeThrottle(key.length());
                handle.incrementEntriesScanned();
                fetched++;
                VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();

                VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                      .setKey(ProtoUtils.encodeBytes(key))
                                                                                      .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                      .build();
                response.setPartitionEntry(partitionEntry);

                Message message = response.build();
                startNs = System.nanoTime();
                ProtoUtils.writeMessage(outputStream, message);
                stats.recordNetworkTime(handle, System.nanoTime() - startNs);

                throttler.maybeThrottle(AdminServiceRequestHandler.valueSize(value));
            }
        } else {
            stats.recordDiskTime(handle, System.nanoTime() - startNs);
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
        else {
            stats.closeHandle(handle);
            return StreamRequestHandlerState.COMPLETE;
        }
    }

}
