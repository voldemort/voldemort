package voldemort.store;

import voldemort.VoldemortException;

/**
 * Exception thrown if the user attempts to retrieve a capability that the store
 * does not provide.
 * 
 * @see voldemort.store.StoreCapabilityType
 * 
 * 
 */
public class NoSuchCapabilityException extends VoldemortException {

    private static final long serialVersionUID = 1;

    public NoSuchCapabilityException(StoreCapabilityType capability, String storeName) {
        super("Capability " + capability + " not found on store '" + storeName + "'");
    }
}
