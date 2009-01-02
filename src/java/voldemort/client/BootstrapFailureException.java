package voldemort.client;

import voldemort.VoldemortException;

public class BootstrapFailureException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public BootstrapFailureException() {}

    public BootstrapFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public BootstrapFailureException(String message) {
        super(message);
    }

    public BootstrapFailureException(Throwable cause) {
        super(cause);
    }

}