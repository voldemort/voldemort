package voldemort.client.protocol.admin;

import voldemort.VoldemortException;

/**
 * This exception is thrown by the {@link AdminClient} when waiting for an
 * {@link voldemort.server.protocol.admin.AsyncOperation} to finish and the
 * maximum threshold of time is exceeded.
 */
public class AsyncOperationTimeoutException extends VoldemortException {
    public AsyncOperationTimeoutException() {
        super();
    }

    public AsyncOperationTimeoutException(String s, Throwable t) {
        super(s, t);
    }

    public AsyncOperationTimeoutException(String s) {
        super(s);
    }

    public AsyncOperationTimeoutException(Throwable t) {
        super(t);
    }
}
