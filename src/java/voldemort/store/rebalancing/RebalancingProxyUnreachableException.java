package voldemort.store.rebalancing;

import voldemort.VoldemortApplicationException;

public class RebalancingProxyUnreachableException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    public RebalancingProxyUnreachableException(String s, Throwable t) {
        super(s, t);
    }

    public RebalancingProxyUnreachableException(String s) {
        super(s);
    }

    public RebalancingProxyUnreachableException(Throwable t) {
        super(t);
    }

    @Override
    public short getId() {
        return 15;
    }
}
