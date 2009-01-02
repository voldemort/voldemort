package voldemort.serialization;

import voldemort.VoldemortException;

/**
 * Thrown to indicate failures in serialization
 * 
 * This is a user-initiated failure.
 * 
 * @author jay
 * 
 */
public class SerializationException extends VoldemortException {

    private static final long serialVersionUID = 1;

    protected SerializationException() {
        super();
    }

    public SerializationException(String s, Throwable t) {
        super(s, t);
    }

    public SerializationException(String s) {
        super(s);
    }

    public SerializationException(Throwable t) {
        super(t);
    }

}
