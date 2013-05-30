package voldemort.versioning;

import voldemort.VoldemortException;

public class InvalidClockEntryException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public InvalidClockEntryException() {
        super();
    }

    public InvalidClockEntryException(String s, Throwable t) {
        super(s, t);
    }

    public InvalidClockEntryException(String s) {
        super(s);
    }

    public InvalidClockEntryException(Throwable t) {
        super(t);
    }
}
