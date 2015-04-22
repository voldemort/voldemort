package voldemort.store.readonly.swapper;

import voldemort.utils.Props;

import java.util.Set;

/**
 * Component to make sure we can do some operations synchronously across many processes.
 */
public abstract class FailedFetchLock {
    protected final Props props;
    public FailedFetchLock(Props props) {
        this.props = props;
    }
    public abstract void acquireLock() throws Exception;
    public abstract void releaseLock();
    public abstract Set<Integer> getDisabledNodes();
    public abstract void addDisabledNode(int nodeId, String details) throws Exception;
}