package voldemort.server.protocol.admin;

import voldemort.VoldemortException;

/**
 * This exception means that an AsyncOperation with a specific ID was not found
 * on the server.
 */
public class AsyncOperationNotFoundException extends VoldemortException {
    /**
     * DO NOT CHANGE THIS STRING.
     *
     * This string is used to identify this kind of exception coming from
     * older servers who did not have this as a dedicated exception type.
     */
    public final static String MESSAGE = "No operation with id %s found";
    public AsyncOperationNotFoundException(int requestId) {
        super(String.format(MESSAGE, requestId));
    }
}
