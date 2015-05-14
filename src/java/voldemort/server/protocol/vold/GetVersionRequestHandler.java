package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;


public class GetVersionRequestHandler extends ClientRequestHandler {

    ByteArray key;
    List<Version> results;

    public GetVersionRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        super(store, protocolVersion);
    }

    public static boolean isCompleteRequest(DataInputStream inputStream, ByteBuffer buffer)
            throws IOException, VoldemortException {
        // skip the key
        return ClientRequestHandler.skipByteArray(inputStream, buffer);
    }

    @Override
    public boolean parseRequest(DataInputStream inputStream) throws IOException {
        key = ClientRequestHandler.readKey(inputStream);
        return true;
    }

    @Override
    public void processRequest() throws VoldemortException {
        results = store.getVersions(key);
    }

    @Override
    public void writeResponse(DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(0);
        outputStream.writeInt(results.size());
        for(Version v: results) {
            byte[] clock = ((VectorClock) v).toBytes();

            outputStream.writeInt(clock.length);
            outputStream.write(clock);
        }
    }

    @Override
    public int getResponseSize() {
        int size = 2 + 4;
        for(Version v: results) {
            size += 4 + ((VectorClock) v).sizeInBytes();
        }
        return size;
    }

    @Override
    public String getDebugMessage() {
        return "Operation GetVersion" + ClientRequestHandler.getDebugMessageForKey(key);
    }

}
