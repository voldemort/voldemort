package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.UpdatePartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.stats.StreamStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * UpdatePartitionEntriesStreamRequestHandler implements the streaming logic for
 * updating partition entries.
 */

public class UpdatePartitionEntriesStreamRequestHandler implements StreamRequestHandler {

    private VAdminProto.UpdatePartitionEntriesRequest request;

    private final VAdminProto.UpdatePartitionEntriesResponse.Builder responseBuilder = VAdminProto.UpdatePartitionEntriesResponse.newBuilder();

    private final ErrorCodeMapper errorCodeMapper;

    private final EventThrottler throttler;

    private final VoldemortFilter filter;

    private final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    private int counter;

    private final long startTime;

    private final StreamStats stats;

    private final StreamStats.Handle handle;

    private final Logger logger = Logger.getLogger(getClass());

    public UpdatePartitionEntriesStreamRequestHandler(UpdatePartitionEntriesRequest request,
                                                      ErrorCodeMapper errorCodeMapper,
                                                      VoldemortConfig voldemortConfig,
                                                      StoreRepository storeRepository,
                                                      NetworkClassLoader networkClassLoader,
                                                      StreamStats stats) {
        super();
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                    request.getStore());
        throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        filter = (request.hasFilter()) ? AdminServiceRequestHandler.getFilterFromRequest(request.getFilter(),
                                                                                         voldemortConfig,
                                                                                         networkClassLoader)
                                      : new DefaultVoldemortFilter();
        startTime = System.currentTimeMillis();
        this.stats = stats;
        this.handle = stats.makeHandle(StreamStats.Operation.UPDATE, ImmutableList.<Integer>of());
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(request == null) {
            int size = 0;

            try {
                size = inputStream.readInt();
            } catch(EOFException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Incomplete read for message size");

                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            if(size == -1) {
                if(logger.isTraceEnabled())
                    logger.trace("Message size -1, completed partition update");
                handle.setFinished(true);
                stats.closeHandle(handle);
                return StreamRequestHandlerState.COMPLETE;
            }

            if(logger.isTraceEnabled())
                logger.trace("UpdatePartitionEntriesRequest message size: " + size);

            byte[] input = new byte[size];

            try {
                ByteUtils.read(inputStream, input);
            } catch(EOFException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Incomplete read for message");

                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            VAdminProto.UpdatePartitionEntriesRequest.Builder builder = VAdminProto.UpdatePartitionEntriesRequest.newBuilder();
            builder.mergeFrom(input);
            request = builder.build();
        }

        VAdminProto.PartitionEntry partitionEntry = request.getPartitionEntry();
        ByteArray key = ProtoUtils.decodeBytes(partitionEntry.getKey());
        Versioned<byte[]> value = ProtoUtils.decodeVersioned(partitionEntry.getVersioned());

        if(filter.accept(key, value)) {
            try {
                storageEngine.put(key, value, null);

                if(logger.isTraceEnabled())
                    logger.trace("updateEntries (Streaming put) successful");
            } catch(ObsoleteVersionException e) {
                // log and ignore
                if(logger.isDebugEnabled())
                    logger.debug("updateEntries (Streaming put) threw ObsoleteVersionException, Ignoring.");
            }

            throttler.maybeThrottle(key.length() + AdminServiceRequestHandler.valueSize(value));
        }

        // log progress
        counter++;
        handle.incrementEntriesScanned();

        if(0 == counter % 100000) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            if(logger.isDebugEnabled())
                logger.debug("updateEntries() updated " + counter + " entries for store:"
                             + storageEngine.getName() + " in " + totalTime + " s");
        }

        request = null;
        return StreamRequestHandlerState.READING;
    }

    public StreamRequestDirection getDirection() {
        return StreamRequestDirection.READING;
    }

    public void close(DataOutputStream outputStream) throws IOException {
        ProtoUtils.writeMessage(outputStream, responseBuilder.build());
    }

    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException {
        responseBuilder.setError(ProtoUtils.encodeError(errorCodeMapper, e));

        if(logger.isEnabledFor(Level.ERROR))
            logger.error("handleUpdatePartitionEntries failed for request(" + request + ")", e);
    }

}
