package voldemort.server.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import voldemort.VoldemortApplicationException;

public class VoldemortRebalancingException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    private Collection<Exception> causes = new ArrayList<Exception>();

    public VoldemortRebalancingException(String s) {
        super(s);
    }

    public VoldemortRebalancingException(String s, List<Exception> failures) {
        super(s);
        causes.addAll(failures);
    }

    public Collection<Exception> getCauses() {
        return causes;
    }

}
