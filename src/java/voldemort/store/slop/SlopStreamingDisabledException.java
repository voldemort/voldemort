package voldemort.store.slop;

import voldemort.store.UnreachableStoreException;

public class SlopStreamingDisabledException extends UnreachableStoreException {

    private static final long serialVersionUID = 1L;

    public SlopStreamingDisabledException(String s) {
        super(s);
    }

    public SlopStreamingDisabledException(String s, Throwable t) {
        super(s, t);
    }

}
