package voldemort.utils;

public class VoldemortDeploymentException extends Exception {

    public VoldemortDeploymentException() {}

    public VoldemortDeploymentException(String message) {
        super(message);
    }

    public VoldemortDeploymentException(Throwable cause) {
        super(cause);
    }

    public VoldemortDeploymentException(String message, Throwable cause) {
        super(message, cause);
    }

}
