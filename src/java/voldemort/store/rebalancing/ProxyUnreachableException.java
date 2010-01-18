package voldemort.store.rebalancing;

import voldemort.VoldemortException;

public class ProxyUnreachableException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public ProxyUnreachableException(String s, Throwable t) {
        super(s, t);
    }

    public ProxyUnreachableException(String s) {
        super(s);
    }

    public ProxyUnreachableException(Throwable t) {
        super(t);
    }

}
