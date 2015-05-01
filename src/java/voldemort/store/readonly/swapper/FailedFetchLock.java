package voldemort.store.readonly.swapper;

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
    protected final String clusterId;
    public FailedFetchLock(Props props, String clusterUrl) {
        this.props = props;
        this.clusterId = sanitizeClusterId(clusterUrl);
    }
    public abstract void acquireLock() throws Exception;
    public abstract void releaseLock() throws Exception;
    public abstract Set<Integer> getDisabledNodes() throws Exception;
    public abstract void addDisabledNode(int nodeId,
                                         String details,
                                         String storeName,
                                         long storeVersion) throws Exception;
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " for cluster: " + clusterId;
    }

    private String sanitizeClusterId(String clusterUrl) {
        try {
            return new URI(clusterUrl).getHost();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("The clusterUrl parameter must be a valid URI!", e);
        }
    }
}