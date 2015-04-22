package voldemort.store.readonly.swapper;

import voldemort.utils.Props;

import java.util.Set;

/**
 * An implementation of the {@link FailedFetchLock} that uses HDFS as a global lock.
 */
public class HdfsFailedFetchLock extends FailedFetchLock {
    public final static String PUSH_HA_LOCK_HDFS_TIMEOUT = "push.ha.lock.hdfs.timeout";
    public final static String PUSH_HA_LOCK_HDFS_RETRIES = "push.ha.lock.hdfs.retries";
    public final static String PUSH_HA_LOCK_HDFS_PATH = "push.ha.lock.hdfs.path";

    // Default value: 10000 ms * 360 retries = 1 hour
    private final Integer timeOut = props.getInt(PUSH_HA_LOCK_HDFS_TIMEOUT, 10000);
    private final Integer retries = props.getInt(PUSH_HA_LOCK_HDFS_RETRIES, 360);
    private final String path = props.getString(PUSH_HA_LOCK_HDFS_PATH);

    public HdfsFailedFetchLock(Props props, String clusterId, String processId) {
        super(props, clusterId, processId);
    }

    @Override
    public void acquireLock() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void releaseLock() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<Integer> getDisabledNodes() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addDisabledNode(int nodeId, String details) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
