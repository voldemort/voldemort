package voldemort.store.readonly.swapper;

import voldemort.VoldemortException;

/**
 * A class of exceptions representing fetch failures which should prevent a
 * subsequent swap from happening.
 */
public class UnrecoverableFailedFetchException extends VoldemortException {
    public UnrecoverableFailedFetchException(String details) {
        super(details);
    }
}
