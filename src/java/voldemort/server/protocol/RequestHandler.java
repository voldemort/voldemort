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

    public StreamRequestHandler handleRequest(DataInputStream inputStream,
                                              DataOutputStream outputStream) throws IOException;

    /**
     * This method is used by non-blocking code to determine if the give buffer
     * represents a complete request. Because the non-blocking code can by
     * definition not just block waiting for more data, it's possible to get
     * partial reads, and this identifies that case.
     * 
     * @param buffer Buffer to check; the buffer is reset to position 0 before
     *        calling this method and the caller must reset it after the call
     *        returns
     * @return True if the buffer holds a complete request, false otherwise
     */

    public boolean isCompleteRequest(ByteBuffer buffer);

}
