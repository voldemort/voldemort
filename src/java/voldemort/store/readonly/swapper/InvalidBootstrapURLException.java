package voldemort.store.readonly.swapper;

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

public class InvalidBootstrapURLException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public InvalidBootstrapURLException(String message) {
        super(message);
    }

    public InvalidBootstrapURLException(String message, Throwable t) {
        super(message, t);
    }

    public static InvalidBootstrapURLException wrap(String s, Throwable t) {
        if(t instanceof InvalidBootstrapURLException) {
            return new InvalidBootstrapURLException(s, t.getCause());
        }
        return new InvalidBootstrapURLException(s, t);
    }
}
