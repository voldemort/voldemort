package voldemort.utils;

public class VoldemortStartClusterException extends Exception {

    public VoldemortStartClusterException() {}

    public VoldemortStartClusterException(String message) {
        super(message);
    }

    public VoldemortStartClusterException(Throwable cause) {
        super(cause);
    }

    public VoldemortStartClusterException(String message, Throwable cause) {
        super(message, cause);
    }

}
