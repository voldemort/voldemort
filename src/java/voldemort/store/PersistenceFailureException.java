package voldemort.store;

import voldemort.VoldemortException;

/**
 * Thrown by the StorageEngine if storage fails
 * 
 * @author jay
 * 
 */
public class PersistenceFailureException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public PersistenceFailureException() {
        super();
    }

    public PersistenceFailureException(String s, Throwable t) {
        super(s, t);
    }

    public PersistenceFailureException(String s) {
        super(s);
    }

    public PersistenceFailureException(Throwable t) {
        super(t);
    }

}
