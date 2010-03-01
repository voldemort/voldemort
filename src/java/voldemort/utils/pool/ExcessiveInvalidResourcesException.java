package voldemort.utils.pool;

/**
 * Indicates that we have exceeded the maximum number of successive invalid
 * resources that can be created in a single checkout.
 * 
 * 
 */
public class ExcessiveInvalidResourcesException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    public ExcessiveInvalidResourcesException(int count) {
        super(count + " successive attempts to create a valid resource failed; "
              + "this exceeds the maximum resource creation limit for a single checkout.");
    }

}
