package voldemort.store.readonly.swapper;

import voldemort.server.VoldemortConfig;
import voldemort.utils.Props;

import java.io.Closeable;
import java.util.Set;

/**
 * Component to make sure we can do some operations synchronously across many processes.
 */
public abstract class FailedFetchLock implements Closeable {
    protected final VoldemortConfig config;
    protected final Props props;
    protected final String lockPath;
    protected final String clusterId;
    public FailedFetchLock(VoldemortConfig config, Props props) {
        this.config = config;
        this.props = props;
        this.lockPath = config.getHighAvailabilityPushLockPath();
        this.clusterId = config.getHighAvailabilityPushClusterId();
    }
    public abstract void acquireLock() throws Exception;
    public abstract void releaseLock() throws Exception;
    public abstract Set<Integer> getDisabledNodes() throws Exception;
    public abstract void addDisabledNode(int nodeId,
                                         String storeName,
                                         long storeVersion) throws Exception;
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " for cluster: " + clusterId;
    }
}