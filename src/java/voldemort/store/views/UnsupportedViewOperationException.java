package voldemort.store.views;

import voldemort.VoldemortException;

/**
 * Error indicating a write operation on a read-only view or vice-versa
 * 
 * @author jay
 * 
 */
public class UnsupportedViewOperationException extends VoldemortException {

    private static final long serialVersionUID = 1;

    public UnsupportedViewOperationException() {
        super();
    }

    public UnsupportedViewOperationException(String s) {
        super(s);
    }

}
