package voldemort.server.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.VoldemortException;

/**
 * Implements an iterator-esque streaming request handler wherein we keep
 * executing handleRequest until it returns
 * {@link StreamRequestHandlerState#COMPLETE}.
 * 
 */

public interface StreamRequestHandler {

    public final static int STAT_RECORDS_INTERVAL = 100000;

    /**
     * Handles a "segment" of a streaming request.
     * 
     * @param inputStream
     * @param outputStream
     * 
     * @return {@link StreamRequestHandlerState}
     * 
     * @throws IOException
     */

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException;

    public void close(DataOutputStream outputStream) throws IOException;

    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException;

    public StreamRequestDirection getDirection();

    public enum StreamRequestHandlerState {

        COMPLETE,
        READING,
        WRITING,
        INCOMPLETE_READ;

    }

    public enum StreamRequestDirection {

        READING,
        WRITING;

    }

}
