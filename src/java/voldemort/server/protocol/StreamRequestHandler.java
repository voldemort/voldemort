package voldemort.server.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.VoldemortException;

public interface StreamRequestHandler {

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException;

    public StreamRequestDirection getDirection();

    public void close(DataOutputStream outputStream) throws IOException;

    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException;

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
