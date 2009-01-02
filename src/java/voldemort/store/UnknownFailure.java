package voldemort.store;

import voldemort.VoldemortException;

/**
 * When we don't know what the hell happened, call it one of these.
 * 
 * @author jay
 * 
 */
public class UnknownFailure extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public UnknownFailure() {
        super();
    }

    public UnknownFailure(String s, Throwable t) {
        super(s, t);
    }

    public UnknownFailure(String s) {
        super(s);
    }

    public UnknownFailure(Throwable t) {
        super(t);
    }

    public short getId() {
        return 6;
    }

}
