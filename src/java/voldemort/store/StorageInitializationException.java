package voldemort.store;

import voldemort.VoldemortException;

/**
 * This exception indicates that the server was unable to initialize on or more
 * storage services or stores within a service.
 * 
 * @author jay
 * 
 */
public class StorageInitializationException extends VoldemortException {

    private final static long serialVersionUID = 1;

    public StorageInitializationException() {}

    public StorageInitializationException(String message) {
        super(message);
    }

    public StorageInitializationException(Throwable t) {
        super(t);
    }

    public StorageInitializationException(String message, Throwable t) {
        super(message, t);
    }

}
