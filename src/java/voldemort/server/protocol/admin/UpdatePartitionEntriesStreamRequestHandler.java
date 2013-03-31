package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

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
import voldemort.store.stats.StreamingStats;
import voldemort.store.stats.StreamingStats.Operation;
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

    private final StreamingStats streamStats;

    private final Logger logger = Logger.getLogger(getClass());

    private AtomicBoolean isBatchWriteOff;

    public UpdatePartitionEntriesStreamRequestHandler(UpdatePartitionEntriesRequest request,
                                                      ErrorCodeMapper errorCodeMapper,
                                                      VoldemortConfig voldemortConfig,
                                                      StoreRepository storeRepository,
                                                      NetworkClassLoader networkClassLoader) {
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
        if(voldemortConfig.isJmxEnabled()) {
            this.streamStats = storeRepository.getStreamingStats(storageEngine.getName());
        } else {
            this.streamStats = null;
        }
        storageEngine.beginBatchModifications();
        isBatchWriteOff = new AtomicBoolean(false);
    }

    @Override
    protected void finalize() {
        // when the object is GCed, don't forget to end the batch-write mode.
        // This is ugly. But the cleanest way to do this, given our network code
        // does not guarantee that close() will always be called
        if(!isBatchWriteOff.get())
            storageEngine.endBatchModifications();
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        long startNs = System.nanoTime();
        if(request == null) {
            int size = 0;

            try {
                size = inputStream.readInt();
            } catch(EOFException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Incomplete read for message size");
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES, System.nanoTime()
                                                                            - startNs);
                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            if(size == -1) {
                long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                logger.info("Update entries successfully updated " + counter
                            + " entries for store '" + storageEngine.getName() + "' in "
                            + totalTime + " s");

                if(logger.isTraceEnabled())
                    logger.trace("Message size -1, completed partition update");
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES, System.nanoTime()
                                                                            - startNs);
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
            } finally {
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES, System.nanoTime()
                                                                            - startNs);
            }

            VAdminProto.UpdatePartitionEntriesRequest.Builder builder = VAdminProto.UpdatePartitionEntriesRequest.newBuilder();
            builder.mergeFrom(input);
            request = builder.build();
        }

        VAdminProto.PartitionEntry partitionEntry = request.getPartitionEntry();
        ByteArray key = ProtoUtils.decodeBytes(partitionEntry.getKey());
        Versioned<byte[]> value = ProtoUtils.decodeVersioned(partitionEntry.getVersioned());

        if(filter.accept(key, value)) {
            startNs = System.nanoTime();
            try {
                storageEngine.put(key, value, null);

                if(logger.isTraceEnabled())
                    logger.trace("updateEntries (Streaming put) successful");
            } catch(ObsoleteVersionException e) {
                // log and ignore
                if(logger.isDebugEnabled())
                    logger.debug("updateEntries (Streaming put) threw ObsoleteVersionException, Ignoring.");
            } finally {
                if(streamStats != null)
                    streamStats.reportStorageTime(Operation.UPDATE_ENTRIES, System.nanoTime()
                                                                            - startNs);
            }

            throttler.maybeThrottle(key.length() + AdminServiceRequestHandler.valueSize(value));
        }

        // log progress
        counter++;
        if(streamStats != null)
            streamStats.reportStreamingPut(Operation.UPDATE_ENTRIES);

        if(0 == counter % STAT_RECORDS_INTERVAL) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            logger.info("Update entries updated " + counter + " entries for store '"
                        + storageEngine.getName() + "' in " + totalTime + " s");
        }

        request = null;
        return StreamRequestHandlerState.READING;
    }

    public StreamRequestDirection getDirection() {
        return StreamRequestDirection.READING;
    }

    public void close(DataOutputStream outputStream) throws IOException {
        ProtoUtils.writeMessage(outputStream, responseBuilder.build());
        storageEngine.endBatchModifications();
        isBatchWriteOff.compareAndSet(false, true);
    }

    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException {
        responseBuilder.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        if(logger.isEnabledFor(Level.ERROR))
            logger.error("handleUpdatePartitionEntries failed for request(" + request + ")", e);
    }
}
