package voldemort.store.views;

import voldemort.VoldemortApplicationException;

/**
 * Error indicating a write operation on a read-only view or vice-versa
 * 
 * 
 */
public class UnsupportedViewOperationException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1;

    public UnsupportedViewOperationException(String s) {
        super(s);
    }

}
