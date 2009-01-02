package voldemort.store;

import voldemort.VoldemortException;

/**
 * Thrown to indicate the failure of some store operation (put, get, delete)
 * 
 * @author jay
 * 
 */
public class StoreOperationFailureException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public StoreOperationFailureException(String s, Throwable t) {
        super(s, t);
    }

    public StoreOperationFailureException(String s) {
        super(s);
    }

    public StoreOperationFailureException(Throwable t) {
        super(t);
    }

    public short getId() {
        return 3;
    }

}
