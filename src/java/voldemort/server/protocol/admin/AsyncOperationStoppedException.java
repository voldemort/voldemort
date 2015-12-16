package voldemort.server.protocol.admin;

import voldemort.VoldemortException;

/**
 * This exception is used to denote that an {@link AsyncOperation} has been stopped.
 */
public class AsyncOperationStoppedException extends VoldemortException {
    public AsyncOperationStoppedException(String message) {
        super(message);
    }
}
