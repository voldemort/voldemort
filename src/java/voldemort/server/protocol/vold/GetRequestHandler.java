package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetRequestHandler extends ClientRequestHandler {

    ByteArray key;
    byte[] transforms;
    List<Versioned<byte[]>> results;

    public static boolean isCompleteRequest(DataInputStream inputStream,
                                            ByteBuffer buffer,
                                            int protocolVersion) throws IOException,
            VoldemortException {
        if(ClientRequestHandler.skipByteArray(inputStream, buffer)) {
            ClientRequestHandler.readSingleTransform(inputStream, protocolVersion);
            return true;
        }
        return false;
    }

    public GetRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        super(store, protocolVersion);
    }

    @Override
    public boolean parseRequest(DataInputStream inputStream) throws IOException {
        key = ClientRequestHandler.readKey(inputStream);
        transforms = ClientRequestHandler.readSingleTransform(inputStream, protocolVersion);
        return true;
    }

    @Override
    public void processRequest() {
        results = store.get(key, transforms);
    }

    @Override
    public void writeResponse(DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(0);
        ClientRequestHandler.writeResults(outputStream, results);
    }

    @Override
    public int getResponseSize() {
        return 2 + ClientRequestHandler.getResultsSize(results);
    }

    @Override
    public String getDebugMessage() {
        return "Operation GET " + ClientRequestHandler.getDebugMessageForKey(key)
               + ClientRequestHandler.getDebugMessageForValue(results);
    }

}
