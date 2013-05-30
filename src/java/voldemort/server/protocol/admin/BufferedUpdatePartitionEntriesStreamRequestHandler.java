package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.UpdatePartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreUtils;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * The buffering is so that if we the stream contains multiple versions for the
 * same key, then we would want the storage to be updated with all the versions
 * atomically, to make sure client does not read a partial set of versions at
 * any point
 * 
 */
class BufferedUpdatePartitionEntriesStreamRequestHandler extends
        UpdatePartitionEntriesStreamRequestHandler {

    private static final int VALS_BUFFER_EXPECTED_SIZE = 5;
    /**
     * Current key being buffered.
     */
    private ByteArray currBufferedKey;

    private List<Versioned<byte[]>> currBufferedVals;

    public BufferedUpdatePartitionEntriesStreamRequestHandler(UpdatePartitionEntriesRequest request,
                                                              ErrorCodeMapper errorCodeMapper,
                                                              VoldemortConfig voldemortConfig,
                                                              StorageEngine<ByteArray, byte[], byte[]> storageEngine,
                                                              StoreRepository storeRepository,
                                                              NetworkClassLoader networkClassLoader) {
        super(request,
              errorCodeMapper,
              voldemortConfig,
              storageEngine,
              storeRepository,
              networkClassLoader);
        currBufferedKey = null;
        currBufferedVals = new ArrayList<Versioned<byte[]>>(VALS_BUFFER_EXPECTED_SIZE);
    }

    @Override
    protected void finalize() {
        super.finalize();
        /*
         * Also check if we have any pending values being buffered. if so, flush
         * to storage.
         */
        writeBufferedValsToStorageIfAny();
    }

    /**
     * Persists the current set of versions buffered for the current key into
     * storage, using the multiVersionPut api
     * 
     * NOTE: Now, it could be that the stream broke off and has more pending
     * versions. For now, we simply commit what we have to disk. A better design
     * would rely on in-stream markers to do the flushing to storage.
     */
    private void writeBufferedValsToStorage() {
        long startNs = System.nanoTime();

        List<Versioned<byte[]>> obsoleteVals = storageEngine.multiVersionPut(currBufferedKey,
                                                                             currBufferedVals);
        currBufferedVals = new ArrayList<Versioned<byte[]>>(VALS_BUFFER_EXPECTED_SIZE);
        if(streamStats != null) {
            streamStats.reportStorageTime(Operation.UPDATE_ENTRIES,
                                          Utils.elapsedTimeNs(startNs, System.nanoTime()));
            streamStats.reportStreamingPut(Operation.UPDATE_ENTRIES);
        }

        if(logger.isTraceEnabled())
            logger.trace("updateEntries (Streaming multi-version-put) successful");

        // log Obsolete versions in debug mode
        if(logger.isDebugEnabled() && obsoleteVals.size() > 0) {
            logger.debug("updateEntries (Streaming multi-version-put) rejected these versions as obsolete : "
                         + StoreUtils.getVersions(obsoleteVals) + " for key " + currBufferedKey);
        }

        // log progress
        counter++;
        if(0 == counter % STAT_RECORDS_INTERVAL) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            logger.info("Update entries updated " + counter + " entries for store '"
                        + storageEngine.getName() + "' in " + totalTime + " s");
        }

        // throttling
        int totalValueSize = 0;
        for(Versioned<byte[]> value: currBufferedVals) {
            totalValueSize += AdminServiceRequestHandler.valueSize(value);
        }
        throttler.maybeThrottle(currBufferedKey.length() + totalValueSize);
    }

    private void writeBufferedValsToStorageIfAny() {
        if(currBufferedVals.size() > 0) {
            writeBufferedValsToStorage();
        }
    }

    @Override
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
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            if(size == -1) {
                long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                logger.info("Update entries successfully updated " + counter
                            + " entries for store '" + storageEngine.getName() + "' in "
                            + totalTime + " s");
                // Write the last buffered key to storage
                writeBufferedValsToStorage();
                if(logger.isTraceEnabled())
                    logger.trace("Message size -1, completed partition update");
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
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
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
            }

            VAdminProto.UpdatePartitionEntriesRequest.Builder builder = VAdminProto.UpdatePartitionEntriesRequest.newBuilder();
            builder.mergeFrom(input);
            request = builder.build();
        }

        VAdminProto.PartitionEntry partitionEntry = request.getPartitionEntry();
        ByteArray key = ProtoUtils.decodeBytes(partitionEntry.getKey());
        Versioned<byte[]> value = ProtoUtils.decodeVersioned(partitionEntry.getVersioned());

        if(filter.accept(key, value)) {
            // Check if the current key is same as the one before.
            if(currBufferedKey != null && !key.equals(currBufferedKey)) {
                // if not, write buffered values for the previous key to storage
                writeBufferedValsToStorage();
            }
            currBufferedKey = key;
            currBufferedVals.add(value);
        }

        request = null;
        return StreamRequestHandlerState.READING;
    }

    @Override
    public void close(DataOutputStream outputStream) throws IOException {
        writeBufferedValsToStorageIfAny();
        super.close(outputStream);
    }

    @Override
    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException {
        writeBufferedValsToStorageIfAny();
        super.handleError(outputStream, e);
    }
}
