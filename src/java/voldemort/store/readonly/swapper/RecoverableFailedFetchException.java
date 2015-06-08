package voldemort.store.readonly.swapper;

import voldemort.VoldemortException;

/**
 * A class of exceptions representing fetch failures which need to be reported but which
 * should not prevent a subsequent swap from happening.
 */
public class RecoverableFailedFetchException extends VoldemortException {
    public RecoverableFailedFetchException(String details, Exception e) {
        super(details, e);
    }
}
