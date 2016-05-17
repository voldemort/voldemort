package voldemort.store.readonly.swapper;

import voldemort.server.VoldemortConfig;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;

import java.io.Closeable;
import java.util.Map;
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
    public abstract void removeObsoleteStateForStore(int nodeId,
                                                     String storeName,
                                                     Map<Long, Boolean> versionToEnabledMap) throws Exception;
    public abstract void removeObsoleteStateForNode(int nodeId) throws Exception;

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " for cluster: " + clusterId;
    }

    public static FailedFetchLock getLock(VoldemortConfig config, Props remoteJobProps) throws ClassNotFoundException {
        Class<? extends FailedFetchLock> failedFetchLockClass =
                (Class<? extends FailedFetchLock>) Class.forName(config.getHighAvailabilityPushLockImplementation());

        if (remoteJobProps == null) {
            remoteJobProps = new Props();
        }

        // Pass both server properties and the remote job's properties to the FailedFetchLock constructor
        Object[] failedFetchLockParams = new Object[]{config, remoteJobProps};

        return ReflectUtils.callConstructor(failedFetchLockClass, failedFetchLockParams);
    }
}