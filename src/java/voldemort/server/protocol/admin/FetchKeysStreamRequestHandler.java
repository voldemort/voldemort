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
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.RebalanceUtils;

import com.google.protobuf.Message;

public class FetchKeysStreamRequestHandler extends FetchStreamRequestHandler {

    protected final ClosableIterator<ByteArray> keyIterator;

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
              networkClassLoader,
              Operation.FETCH_KEYS);
        this.keyIterator = storageEngine.keys();
        logger.info("Starting fetch keys for store '" + storageEngine.getName()
                    + "' with replica to partition mapping " + replicaToPartitionList);
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        long startNs = System.nanoTime();
        ByteArray key = keyIterator.next();
        if(streamStats != null) {
            streamStats.reportStorageTime(operation, System.nanoTime() - startNs);
            streamStats.reportStreamingScan(operation);
        }

        throttler.maybeThrottle(key.length());
        if(RebalanceUtils.checkKeyBelongsToPartition(nodeId,
                                                     key.get(),
                                                     replicaToPartitionList,
                                                     initialCluster,
                                                     storeDef)
           && filter.accept(key, null)
           && counter % skipRecords == 0) {
            VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
            response.setKey(ProtoUtils.encodeBytes(key));

            fetched++;
            if(streamStats != null)
                streamStats.reportStreamingFetch(operation);
            Message message = response.build();

            startNs = System.nanoTime();
            ProtoUtils.writeMessage(outputStream, message);
            if(streamStats != null)
                streamStats.reportNetworkTime(operation, System.nanoTime() - startNs);
        }

        // log progress
        counter++;

        if(0 == counter % STAT_RECORDS_INTERVAL) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            logger.info("Fetch keys scanned " + counter + " keys, fetched " + fetched
                        + " keys for store '" + storageEngine.getName()
                        + "' replicaToPartitionList:" + replicaToPartitionList + " in " + totalTime
                        + " s");
        }

        if(keyIterator.hasNext())
            return StreamRequestHandlerState.WRITING;
        else {
            return StreamRequestHandlerState.COMPLETE;
        }
    }

    @Override
    public final void close(DataOutputStream outputStream) throws IOException {
        if(null != keyIterator)
            keyIterator.close();
        super.close(outputStream);
    }
}
