package voldemort.store.readonly;

import voldemort.VoldemortException;

/**
 * This Exception is thrown when a new RO store is attempted to be pushed to a
 * voldemort cluster without being onboarded formally. Whereas onboarding would
 * have created the store definition and allocated sufficient disk quota for the
 * store before BnP starts.
 * 
 * @author bsakthee
 * 
 */

public class UnauthorizedStoreException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public UnauthorizedStoreException(String message) {
        super(message);
    }

    public UnauthorizedStoreException(String message, Throwable t) {
        super(message, t);
    }

    public static UnauthorizedStoreException wrap(String s, Throwable t) {
        if(t instanceof UnauthorizedStoreException) {
            return new UnauthorizedStoreException(s, t.getCause());
        }
        return new UnauthorizedStoreException(s, t);
    }
}
