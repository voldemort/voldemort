package voldemort.store;

import voldemort.VoldemortApplicationException;


public class StoreNotFoundException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    public StoreNotFoundException(String s) {
        super(s);
    }

    public StoreNotFoundException(String s, Throwable t) {
        super(s, t);
    }

}
