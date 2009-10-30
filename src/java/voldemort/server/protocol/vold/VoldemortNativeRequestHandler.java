package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Server-side request handler for voldemort native client protocol
 * 
 * @author jay
 * 
 */
public class VoldemortNativeRequestHandler extends AbstractRequestHandler implements RequestHandler {

    private final Logger logger = Logger.getLogger(VoldemortNativeRequestHandler.class);

    private final int protocolVersion;

    public VoldemortNativeRequestHandler(ErrorCodeMapper errorMapper,
                                         StoreRepository repository,
                                         int protocolVersion) {
        super(errorMapper, repository);
        if(protocolVersion < 0 || protocolVersion > 1)
            throw new IllegalArgumentException("Unknown protocol version: " + protocolVersion);
        this.protocolVersion = protocolVersion;
    }

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        boolean isRouted = false;
        if(protocolVersion > 0)
            isRouted = inputStream.readBoolean();
        Store<ByteArray, byte[]> store = getStore(storeName, isRouted);
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
    }

    private void handleGetVersion(DataInputStream inputStream,
                                  DataOutputStream outputStream,
                                  Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = readKey(inputStream);
        List<Version> results = null;
        try {
            results = store.getVersions(key);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
        outputStream.writeInt(results.size());
        for(Version v: results) {
            byte[] clock = ((VectorClock) v).toBytes();
            outputStream.writeInt(clock.length);
            outputStream.write(clock);
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
            if(protocolVersion > 0)
                inputStream.readBoolean();

            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    // Read the key just to skip the bytes.
                    readKey(inputStream);
                    break;
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    int numKeys = inputStream.readInt();

                    // Read the keys to skip the bytes.
                    for(int i = 0; i < numKeys; i++)
                        readKey(inputStream);

                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    readKey(inputStream);

                    int dataSize = inputStream.readInt();

                    // Here we skip over the data (without reading it in) and
                    // move our position to just past it.
                    buffer.position(buffer.position() + dataSize);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    readKey(inputStream);

                    int versionSize = inputStream.readShort();

                    // Here we skip over the version (without reading it in) and
                    // move our position to just past it.
                    buffer.position(buffer.position() + versionSize);
                    break;
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
                           Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = readKey(inputStream);
        List<Versioned<byte[]>> results = null;
        try {
            results = store.get(key);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
        writeResults(outputStream, results);
    }

    private void handleGetAll(DataInputStream inputStream,
                              DataOutputStream outputStream,
                              Store<ByteArray, byte[]> store) throws IOException {
        // read keys
        int numKeys = inputStream.readInt();
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(readKey(inputStream));

        // execute the operation
        Map<ByteArray, List<Versioned<byte[]>>> results = null;
        try {
            results = store.getAll(keys);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
            return;
        }

        // write back the results
        outputStream.writeInt(results.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            // write the key
            outputStream.writeInt(entry.getKey().length());
            outputStream.write(entry.getKey().get());
            // write the values
            writeResults(outputStream, entry.getValue());
        }
    }

    private void handlePut(DataInputStream inputStream,
                           DataOutputStream outputStream,
                           Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = readKey(inputStream);
        int valueSize = inputStream.readInt();
        byte[] bytes = new byte[valueSize];
        ByteUtils.read(inputStream, bytes);
        VectorClock clock = new VectorClock(bytes);
        byte[] value = ByteUtils.copy(bytes, clock.sizeInBytes(), bytes.length);
        try {
            store.put(key, new Versioned<byte[]>(value, clock));
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    private void handleDelete(DataInputStream inputStream,
                              DataOutputStream outputStream,
                              Store<ByteArray, byte[]> store) throws IOException {
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
    }

    private void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = getErrorMapper().getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }

}
