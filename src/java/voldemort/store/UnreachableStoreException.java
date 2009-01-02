package voldemort.store;

import voldemort.VoldemortException;

/**
 * Indicates that the given store cannot be reached (say, due to network
 * failure).
 * 
 * @author jay
 * 
 */
public class UnreachableStoreException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public UnreachableStoreException(String s) {
        super(s);
    }

    public UnreachableStoreException(String s, Throwable t) {
        super(s, t);
    }

    public short getId() {
        return 7;
    }

}
