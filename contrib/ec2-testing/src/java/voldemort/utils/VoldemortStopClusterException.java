package voldemort.utils;

public class VoldemortStopClusterException extends Exception {

    public VoldemortStopClusterException() {}

    public VoldemortStopClusterException(String message) {
        super(message);
    }

    public VoldemortStopClusterException(Throwable cause) {
        super(cause);
    }

    public VoldemortStopClusterException(String message, Throwable cause) {
        super(message, cause);
    }

}
