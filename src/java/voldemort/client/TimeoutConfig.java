package voldemort.client;

import java.util.HashMap;

/**
 * Encapsulates the timeouts for various voldemort operations
 * 
 */
public class TimeoutConfig {

    private HashMap<VoldemortOperation, Long> timeoutMap;

    private boolean partialGetAllAllowed;

    public TimeoutConfig(long globalTimeout, boolean allowPartialGetAlls) {
        this(globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             allowPartialGetAlls);
    }

    public TimeoutConfig(long getTimeout,
                         long putTimeout,
                         long deleteTimeout,
                         long getAllTimeout,
                         long getVersionsTimeout,
                         boolean allowPartialGetAlls) {
        timeoutMap = new HashMap<VoldemortOperation, Long>();
        timeoutMap.put(VoldemortOperation.GET, getTimeout);
        timeoutMap.put(VoldemortOperation.PUT, putTimeout);
        timeoutMap.put(VoldemortOperation.DELETE, deleteTimeout);
        timeoutMap.put(VoldemortOperation.GETALL, getAllTimeout);
        timeoutMap.put(VoldemortOperation.GETVERSIONS, getVersionsTimeout);
        setPartialGetAllAllowed(allowPartialGetAlls);
    }

    public long getOperationTimeout(VoldemortOperation operation) {
        assert timeoutMap.containsKey(operation);
        return timeoutMap.get(operation);
    }

    public void setOperationTimeout(VoldemortOperation operation, long timeoutMs) {
        timeoutMap.put(operation, timeoutMs);
    }

    public boolean isPartialGetAllAllowed() {
        return partialGetAllAllowed;
    }

    public void setPartialGetAllAllowed(boolean allowPartialGetAlls) {
        this.partialGetAllAllowed = allowPartialGetAlls;
    }

}
