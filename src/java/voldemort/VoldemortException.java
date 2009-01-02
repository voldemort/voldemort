package voldemort;

/**
 * Base exception
 *
 * @author jay
 *
 */
public class VoldemortException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public VoldemortException() {
		super();
	}
	
	public short getId() {
	    return 1;
	}

	public VoldemortException(String s, Throwable t) {
		super(s, t);
	}

	public VoldemortException(String s) {
		super(s);
	}

	public VoldemortException(Throwable t) {
		super(t);
	}

}
