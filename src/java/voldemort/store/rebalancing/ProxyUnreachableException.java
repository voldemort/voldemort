package voldemort.store.rebalancing;

import voldemort.store.UnreachableStoreException;

public class ProxyUnreachableException extends UnreachableStoreException {

    private static final long serialVersionUID = 1L;

    public ProxyUnreachableException(String s, Throwable t) {
        super(s, t);
    }

    public ProxyUnreachableException(String s) {
        super(s);
    }

    public short getId() {
        return 15;
    }
}
