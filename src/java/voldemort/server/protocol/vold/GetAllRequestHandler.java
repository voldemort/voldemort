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
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetAllRequestHandler extends ClientRequestHandler {

    private static final Logger logger = Logger.getLogger(VoldemortNativeRequestHandler.class);

    List<ByteArray> keys;
    Map<ByteArray, byte[]> transforms;
    Map<ByteArray, List<Versioned<byte[]>>> results;

    public GetAllRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        super(store, protocolVersion);
    }

    public static boolean isCompleteRequest(DataInputStream inputStream,
                                            ByteBuffer buffer,
                                            int protocolVersion) throws IOException,
            VoldemortException {
        int numKeys = inputStream.readInt();

        // Read the keys to skip the bytes.
        for(int i = 0; i < numKeys; i++) {
            if(!ClientRequestHandler.skipByteArray(inputStream, buffer))
                return false;
        }

        if(protocolVersion > 2) {
            boolean hasTransform = inputStream.readBoolean();
            if(hasTransform) {
                int numTrans = inputStream.readInt();
                for(int i = 0; i < numTrans; i++) {
                    if(!ClientRequestHandler.skipByteArray(inputStream, buffer))
                        return false;
                    ClientRequestHandler.readTransforms(inputStream);
                }
            }
        }
        return true;
    }

    @Override
    public boolean parseRequest(DataInputStream inputStream) throws IOException {
        int numKeys = inputStream.readInt();
        keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(ClientRequestHandler.readKey(inputStream));

        if(protocolVersion > 2) {
            if(inputStream.readBoolean()) {
                int size = inputStream.readInt();
                transforms = new HashMap<ByteArray, byte[]>(size);
                for(int i = 0; i < size; i++) {
                    transforms.put(ClientRequestHandler.readKey(inputStream),
                                   ClientRequestHandler.readTransforms(inputStream));
                }
            }
        }

        return true;
    }

    @Override
    public void processRequest() throws VoldemortException {
        results = store.getAll(keys, transforms);
    }

    @Override
    public void writeResponse(DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(0);

        outputStream.writeInt(results.size());

        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            // write the key
            outputStream.writeInt(entry.getKey().length());
            outputStream.write(entry.getKey().get());
            // write the values
            ClientRequestHandler.writeResults(outputStream, entry.getValue());
        }
    }

    @Override
    public int getResponseSize() {
        int size = 2 + 4;
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            // write the key
            size += 4 + entry.getKey().length();
            // write the values
            size += ClientRequestHandler.getResultsSize(entry.getValue());
        }
        return size;
    }

    public String getDebugMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Operation GETALL ");
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            sb.append(ClientRequestHandler.getDebugMessageForKey(entry.getKey()));
            sb.append(ClientRequestHandler.getDebugMessageForValue(entry.getValue()));
        }
        return sb.toString();
    }

}
