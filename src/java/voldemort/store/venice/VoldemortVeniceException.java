package voldemort.store.venice;

import voldemort.VoldemortException;

/**
 * An exception thrown for Venice related issuse in Voldemort
 */
public class VoldemortVeniceException extends VoldemortException {

  public VoldemortVeniceException() {
    super();
  }

  public VoldemortVeniceException(String s, Throwable t) {
    super(s, t);
  }

  public VoldemortVeniceException(String s) {
    super(s);
  }

  public VoldemortVeniceException(Throwable t) {
    super(t);
  }

}
