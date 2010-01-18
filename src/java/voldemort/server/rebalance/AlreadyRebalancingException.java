package voldemort.server.rebalance;

import voldemort.VoldemortApplicationException;

public class AlreadyRebalancingException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    public AlreadyRebalancingException(String s) {
        super(s);
    }

}
