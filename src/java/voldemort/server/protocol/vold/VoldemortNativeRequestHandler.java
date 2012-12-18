package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.common.VoldemortOpCode;
import voldemort.common.nio.ByteBufferBackedInputStream;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Server-side request handler for voldemort native client protocol
 * 
 * 
 */
public class VoldemortNativeRequestHandler extends AbstractRequestHandler implements RequestHandler {

    private final Logger logger = Logger.getLogger(VoldemortNativeRequestHandler.class);

    private final int protocolVersion;

    public VoldemortNativeRequestHandler(ErrorCodeMapper errorMapper,
                                         StoreRepository repository,
                                         int protocolVersion) {
        super(errorMapper, repository);
        if(protocolVersion < 0 || protocolVersion > 3)
            throw new IllegalArgumentException("Unknown protocol version: " + protocolVersion);
        this.protocolVersion = protocolVersion;
    }

    public StreamRequestHandler handleRequest(DataInputStream inputStream,
                                              DataOutputStream outputStream) throws IOException {
        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        RequestRoutingType routingType = getRoutingType(inputStream);

        Store<ByteArray, byte[], byte[]> store = getStore(storeName, routingType);
        if(store == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        } else {
            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    handleGet(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    handleGetAll(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    handlePut(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    handleDelete(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    handleGetVersion(inputStream, outputStream, store);
                    break;
                default:
                    throw new IOException("Unknown op code: " + opCode);
            }
        }
        outputStream.flush();
        return null;
    }

    private RequestRoutingType getRoutingType(DataInputStream inputStream) throws IOException {
        RequestRoutingType routingType = RequestRoutingType.NORMAL;

        if(protocolVersion > 0) {
            boolean isRouted = inputStream.readBoolean();
            routingType = RequestRoutingType.getRequestRoutingType(isRouted, false);
        }

        if(protocolVersion > 1) {
            int routingTypeCode = inputStream.readByte();
            routingType = RequestRoutingType.getRequestRoutingType(routingTypeCode);
        }

        return routingType;
    }

    private void handleGetVersion(DataInputStream inputStream,
                                  DataOutputStream outputStream,
                                  Store<ByteArray, byte[], byte[]> store) throws IOException {
        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        ByteArray key = readKey(inputStream);
        List<Version> results = null;
        try {
            results = store.getVersions(key);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            logger.error(e.getMessage());
            writeException(outputStream, e);
            return;
        }
        outputStream.writeInt(results.size());

        String clockStr = "";

        for(Version v: results) {
            byte[] clock = ((VectorClock) v).toBytes();

            if(logger.isDebugEnabled())
                clockStr += clock + " ";

            outputStream.writeInt(clock.length);
            outputStream.write(clock);
        }

        if(logger.isDebugEnabled()) {
            logger.debug("GETVERSIONS started at: " + startTimeMs + " handlerRef: "
                         + System.identityHashCode(inputStream) + " key: "
                         + ByteUtils.toHexString(key.get()) + " "
                         + (System.nanoTime() - startTimeNs) + " ns, keySize: " + key.length()
                         + "clocks: " + clockStr);
        }
    }

    /**
     * This is pretty ugly. We end up mimicking the request logic here, so this
     * needs to stay in sync with handleRequest.
     */

    public boolean isCompleteRequest(final ByteBuffer buffer) {
        DataInputStream inputStream = new DataInputStream(new ByteBufferBackedInputStream(buffer));

        try {
            byte opCode = inputStream.readByte();

            // Read the store name in, but just to skip the bytes.
            inputStream.readUTF();

            // Read the 'is routed' flag in, but just to skip the byte.
            if(protocolVersion > 0) {
                boolean routed = inputStream.readBoolean();
                if(logger.isDebugEnabled()) {
                    logger.debug("isRouted=" + routed);
                }
            }

            // Read routing type
            if(protocolVersion > 1) {
                int routingTypeCode = inputStream.readByte();
                if(logger.isDebugEnabled()) {
                    logger.debug("routingTypeCode=" + routingTypeCode);
                }
            }

            switch(opCode) {
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    // Read the key just to skip the bytes.
                    readKey(inputStream);
                    break;
                case VoldemortOpCode.GET_OP_CODE:
                    // Read the key just to skip the bytes.
                    readKey(inputStream);
                    if(protocolVersion > 2) {
                        boolean hasTransform = inputStream.readBoolean();
                        if(hasTransform) {
                            readTransforms(inputStream);
                        }
                    }
                    break;
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    int numKeys = inputStream.readInt();

                    // Read the keys to skip the bytes.
                    for(int i = 0; i < numKeys; i++)
                        readKey(inputStream);

                    if(protocolVersion > 2) {
                        boolean hasTransform = inputStream.readBoolean();
                        if(hasTransform) {
                            int numTrans = inputStream.readInt();
                            for(int i = 0; i < numTrans; i++) {
                                readTransforms(inputStream);
                            }
                        }
                    }
                    break;
                case VoldemortOpCode.PUT_OP_CODE: {
                    readKey(inputStream);

                    int dataSize = inputStream.readInt();
                    int newPosition = buffer.position() + dataSize;

                    if(newPosition > buffer.limit() || newPosition < 0)
                        throw new Exception("Data inconsistency on put - dataSize: " + dataSize
                                            + ", position: " + buffer.position() + ", limit: "
                                            + buffer.limit());

                    // Here we skip over the data (without reading it in) and
                    // move our position to just past it.
                    buffer.position(newPosition);
                    if(protocolVersion > 2) {
                        boolean hasTransform = inputStream.readBoolean();
                        if(hasTransform) {
                            readTransforms(inputStream);
                        }
                    }
                    break;
                }
                case VoldemortOpCode.DELETE_OP_CODE: {
                    readKey(inputStream);

                    int versionSize = inputStream.readShort();
                    int newPosition = buffer.position() + versionSize;

                    if(newPosition > buffer.limit() || newPosition < 0)
                        throw new Exception("Data inconsistency on delete - versionSize: "
                                            + versionSize + ", position: " + buffer.position()
                                            + ", limit: " + buffer.limit());

                    // Here we skip over the data (without reading it in) and
                    // move our position to just past it.
                    buffer.position(newPosition);
                    break;
                }
                default:
                    // Do nothing, let the request handler address this...
            }

            // If there aren't any remaining, we've "consumed" all the bytes and
            // thus have a complete request...
            return !buffer.hasRemaining();
        } catch(Exception e) {
            // This could also occur if the various methods we call into
            // re-throw a corrupted value error as some other type of exception.
            // For example, updating the position on a buffer past its limit
            // throws an InvalidArgumentException.
            if(logger.isDebugEnabled())
                logger.debug("Probable partial read occurred causing exception", e);

            return false;
        }
    }

    private ByteArray readKey(DataInputStream inputStream) throws IOException {
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        inputStream.readFully(key);
        return new ByteArray(key);
    }

    private byte[] readTransforms(DataInputStream inputStream) throws IOException {
        int size = inputStream.readInt();
        if(size == 0)
            return null;
        byte[] transforms = new byte[size];
        inputStream.readFully(transforms);
        return transforms;
    }

    private void writeResults(DataOutputStream outputStream, List<Versioned<byte[]>> values)
            throws IOException {
        outputStream.writeInt(values.size());
        for(Versioned<byte[]> v: values) {
            byte[] clock = ((VectorClock) v.getVersion()).toBytes();
            byte[] value = v.getValue();
            outputStream.writeInt(clock.length + value.length);
            outputStream.write(clock);
            outputStream.write(value);
        }
    }

    private void handleGet(DataInputStream inputStream,
                           DataOutputStream outputStream,
                           Store<ByteArray, byte[], byte[]> store) throws IOException {
        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        ByteArray key = readKey(inputStream);

        byte[] transforms = null;
        if(protocolVersion > 2) {
            if(inputStream.readBoolean())
                transforms = readTransforms(inputStream);
        }
        List<Versioned<byte[]>> results = null;
        try {
            results = store.get(key, transforms);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            logger.error(e.getMessage());
            writeException(outputStream, e);
            return;
        }
        writeResults(outputStream, results);
        if(logger.isDebugEnabled()) {
            debugLogReturnValue(inputStream, key, results, startTimeMs, startTimeNs, "GET");
        }
    }

    private void handleGetAll(DataInputStream inputStream,
                              DataOutputStream outputStream,
                              Store<ByteArray, byte[], byte[]> store) throws IOException {
        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        // read keys
        int numKeys = inputStream.readInt();
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(readKey(inputStream));

        Map<ByteArray, byte[]> transforms = null;
        if(protocolVersion > 2) {
            if(inputStream.readBoolean()) {
                int size = inputStream.readInt();
                transforms = new HashMap<ByteArray, byte[]>(size);
                for(int i = 0; i < size; i++) {
                    transforms.put(readKey(inputStream), readTransforms(inputStream));
                }
            }
        }

        // execute the operation
        Map<ByteArray, List<Versioned<byte[]>>> results = null;
        try {
            results = store.getAll(keys, transforms);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            logger.error(e.getMessage());
            writeException(outputStream, e);
            return;
        }

        // write back the results
        outputStream.writeInt(results.size());

        if(logger.isDebugEnabled())
            logger.debug("GETALL start");

        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            // write the key
            outputStream.writeInt(entry.getKey().length());
            outputStream.write(entry.getKey().get());
            // write the values
            writeResults(outputStream, entry.getValue());

            if(logger.isDebugEnabled()) {
                debugLogReturnValue(inputStream,
                                    entry.getKey(),
                                    entry.getValue(),
                                    startTimeMs,
                                    startTimeNs,
                                    "GETALL");
            }
        }

        if(logger.isDebugEnabled())
            logger.debug("GETALL end");
    }

    private void debugLogReturnValue(DataInputStream input,
                                     ByteArray key,
                                     List<Versioned<byte[]>> values,
                                     long startTimeMs,
                                     long startTimeNs,
                                     String getType) {
        long totalValueSize = 0;
        String valueSizeStr = "[";
        String valueHashStr = "[";
        String versionsStr = "[";
        for(Versioned<byte[]> b: values) {
            int len = b.getValue().length;
            totalValueSize += len;
            valueSizeStr += len + ",";
            valueHashStr += b.hashCode() + ",";
            versionsStr += b.getVersion();
        }
        valueSizeStr += "]";
        valueHashStr += "]";
        versionsStr += "]";

        logger.debug(getType + " handlerRef: " + System.identityHashCode(input) + " start time: "
                     + startTimeMs + " key: " + ByteUtils.toHexString(key.get())
                     + " elapsed time: " + (System.nanoTime() - startTimeNs) + " ns, keySize: "
                     + key.length() + " numResults: " + values.size() + " totalResultSize: "
                     + totalValueSize + " resultSizes: " + valueSizeStr + " resultHashes: "
                     + valueHashStr + " versions: " + versionsStr + " current time: "
                     + System.currentTimeMillis());
    }

    private void handlePut(DataInputStream inputStream,
                           DataOutputStream outputStream,
                           Store<ByteArray, byte[], byte[]> store) throws IOException {
        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        ByteArray key = readKey(inputStream);
        int valueSize = inputStream.readInt();
        byte[] bytes = new byte[valueSize];
        ByteUtils.read(inputStream, bytes);
        VectorClock clock = new VectorClock(bytes);
        byte[] value = ByteUtils.copy(bytes, clock.sizeInBytes(), bytes.length);

        byte[] transforms = null;
        if(protocolVersion > 2) {
            if(inputStream.readBoolean()) {
                transforms = readTransforms(inputStream);
            }
        }
        try {
            store.put(key, new Versioned<byte[]>(value, clock), transforms);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }

        if(logger.isDebugEnabled()) {
            logger.debug("PUT started at: " + startTimeMs + " handlerRef: "
                         + System.identityHashCode(inputStream) + " key: "
                         + ByteUtils.toHexString(key.get()) + " "
                         + (System.nanoTime() - startTimeNs) + " ns, keySize: " + key.length()
                         + " valueHash: " + value.hashCode() + " valueSize: " + value.length
                         + " clockSize: " + clock.sizeInBytes() + " time: "
                         + System.currentTimeMillis());
        }
    }

    private void handleDelete(DataInputStream inputStream,
                              DataOutputStream outputStream,
                              Store<ByteArray, byte[], byte[]> store) throws IOException {
        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        ByteArray key = readKey(inputStream);
        int versionSize = inputStream.readShort();
        byte[] versionBytes = new byte[versionSize];
        ByteUtils.read(inputStream, versionBytes);
        VectorClock version = new VectorClock(versionBytes);
        try {
            boolean succeeded = store.delete(key, version);
            outputStream.writeShort(0);
            outputStream.writeBoolean(succeeded);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }

        if(logger.isDebugEnabled()) {
            logger.debug("DELETE started at: " + startTimeMs + " key: "
                         + ByteUtils.toHexString(key.get()) + " handlerRef: "
                         + System.identityHashCode(inputStream) + " time: "
                         + (System.nanoTime() - startTimeNs) + " ns, keySize: " + key.length()
                         + " clockSize: " + version.sizeInBytes());
        }
    }

    private void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = getErrorMapper().getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }

}
