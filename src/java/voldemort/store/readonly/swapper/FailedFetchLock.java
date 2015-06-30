package voldemort.store.readonly.swapper;

import voldemort.server.VoldemortConfig;
import voldemort.utils.Props;

import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * Component to make sure we can do some operations synchronously across many processes.
 */
public abstract class FailedFetchLock implements Closeable {
    protected final Props props;
    protected final String lockPath;
    protected final String clusterId;
    public FailedFetchLock(Props props) {
        this.props = props;
        this.lockPath = props.getString(VoldemortConfig.PUSH_HA_LOCK_PATH);
        this.clusterId = props.getString(VoldemortConfig.PUSH_HA_CLUSTER_ID);
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