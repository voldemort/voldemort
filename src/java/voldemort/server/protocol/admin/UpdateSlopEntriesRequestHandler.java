package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.UpdateSlopEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.stats.StreamingStats;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class UpdateSlopEntriesRequestHandler implements StreamRequestHandler {

    private VAdminProto.UpdateSlopEntriesRequest request;

    private final VAdminProto.UpdateSlopEntriesResponse.Builder responseBuilder = VAdminProto.UpdateSlopEntriesResponse.newBuilder();

    private final ErrorCodeMapper errorCodeMapper;

    private final StoreRepository storeRepository;

    private final long startTime;

    private long networkTimeNs;

    private boolean isJmxEnabled;

    private long counter = 0L;

    private final Logger logger = Logger.getLogger(getClass());

    public UpdateSlopEntriesRequestHandler(UpdateSlopEntriesRequest request,
                                           ErrorCodeMapper errorCodeMapper,
                                           StoreRepository storeRepository,
                                           VoldemortConfig voldemortConfig) {
        super();
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        this.storeRepository = storeRepository;
        startTime = System.currentTimeMillis();
        networkTimeNs = 0;
        this.isJmxEnabled = voldemortConfig.isJmxEnabled();
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
            logger.error("handleUpdateSlopEntries failed for request(" + request + ")", e);
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
                networkTimeNs += System.nanoTime() - startNs;
                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            if(size == -1) {
                if(logger.isTraceEnabled())
                    logger.trace("Message size -1, completed slop update");
                return StreamRequestHandlerState.COMPLETE;
            }

            if(logger.isTraceEnabled())
                logger.trace("UpdateSlopEntriesRequest message size: " + size);

            byte[] input = new byte[size];

            try {
                ByteUtils.read(inputStream, input);
                networkTimeNs += System.nanoTime() - startNs;
            } catch(EOFException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Incomplete read for message");

                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            VAdminProto.UpdateSlopEntriesRequest.Builder builder = VAdminProto.UpdateSlopEntriesRequest.newBuilder();
            builder.mergeFrom(input);
            request = builder.build();
        }

        StorageEngine<ByteArray, byte[], byte[]> storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                                                             request.getStore());
        StreamingStats streamStats = null;
        if(isJmxEnabled) {
            streamStats = storeRepository.getStreamingStats(storageEngine.getName());
            streamStats.reportNetworkTime(Operation.SLOP_UPDATE, networkTimeNs);
        }
        networkTimeNs = 0;

        ByteArray key = ProtoUtils.decodeBytes(request.getKey());
        VectorClock vectorClock = ProtoUtils.decodeClock(request.getVersion());

        switch(request.getRequestType()) {
            case PUT:
                try {

                    // Retrieve the transform if its exists
                    byte[] transforms = null;
                    if(request.hasTransform()) {
                        transforms = ProtoUtils.decodeBytes(request.getTransform()).get();
                    }

                    // Retrieve the value
                    byte[] value = ProtoUtils.decodeBytes(request.getValue()).get();
                    startNs = System.nanoTime();
                    storageEngine.put(key, Versioned.value(value, vectorClock), transforms);
                    if(isJmxEnabled)
                        streamStats.reportStorageTime(Operation.SLOP_UPDATE, System.nanoTime()
                                                                             - startNs);
                    if(logger.isTraceEnabled())
                        logger.trace("updateSlopEntries (Streaming put) successful");
                } catch(ObsoleteVersionException e) {
                    // log and ignore
                    if(logger.isDebugEnabled())
                        logger.debug("updateSlopEntries (Streaming put) threw ObsoleteVersionException, Ignoring.");
                }
                break;
            case DELETE:
                try {
                    startNs = System.nanoTime();
                    storageEngine.delete(key, vectorClock);
                    if(isJmxEnabled)
                        streamStats.reportStorageTime(Operation.SLOP_UPDATE, System.nanoTime()
                                                                             - startNs);

                    if(logger.isTraceEnabled())
                        logger.trace("updateSlopEntries (Streaming delete) successful");
                } catch(ObsoleteVersionException e) {
                    // log and ignore
                    if(logger.isDebugEnabled())
                        logger.debug("updateSlopEntries (Streaming delete) threw ObsoleteVersionException, Ignoring.");
                }
                break;
            default:
                throw new VoldemortException("Unsupported operation ");
        }

        // log progress
        counter++;
        if(isJmxEnabled)
            streamStats.reportStreamingPut(Operation.SLOP_UPDATE);

        if(0 == counter % 100000) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            if(logger.isDebugEnabled())
                logger.debug("updateSlopEntries() updated " + counter + " entries in " + totalTime
                             + " s");
        }

        request = null;
        return StreamRequestHandlerState.READING;
    }
}
