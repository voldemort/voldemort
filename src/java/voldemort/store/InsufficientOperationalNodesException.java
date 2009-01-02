package voldemort.store;

import java.util.Collection;
import java.util.Collections;

/**
 * Thrown if an operation fails due to too few reachable nodes.
 * 
 * @author jay
 * 
 */
public class InsufficientOperationalNodesException extends StoreOperationFailureException {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Exception> causes;

    public InsufficientOperationalNodesException(String s, Exception e) {
        super(s, e);
        causes = Collections.singleton(e);
    }

    public InsufficientOperationalNodesException(String s) {
        super(s);
        causes = Collections.emptyList();
    }

    public InsufficientOperationalNodesException(Exception e) {
        super(e);
        causes = Collections.singleton(e);
    }

    public InsufficientOperationalNodesException(Collection<? extends Exception> failures) {
        this("Insufficient operational nodes to immediately satisfy request.", failures);
    }

    public InsufficientOperationalNodesException(String message,
                                                 Collection<? extends Exception> failures) {
        super(message, failures.size() > 0 ? failures.iterator().next() : null);
        this.causes = failures;
    }

    public Collection<? extends Exception> getCauses() {
        return this.causes;
    }

    public short getId() {
        return 2;
    }
}
