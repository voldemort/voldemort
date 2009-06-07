package voldemort.server.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A request handler that answers client requests in some given format
 * 
 * @author jay
 * 
 */
public interface RequestHandler {

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException;

    public boolean isCompleteRequest(ByteBuffer buffer);

}
