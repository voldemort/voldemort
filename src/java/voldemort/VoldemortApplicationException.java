package voldemort;

/**
 * Voldemort application level exceptions.
 * <p>
 * These exceptions are thrown by Voldemort servers to voldemort client for some
 * special handling.
 * 
 * 
 */
public class VoldemortApplicationException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public VoldemortApplicationException(String s, Throwable t) {
        super(s, t);
    }

    public VoldemortApplicationException(String s) {
        super(s);
    }

    public VoldemortApplicationException(Throwable t) {
        super(t);
    }

}
