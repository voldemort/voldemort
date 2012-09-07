package voldemort.client;

import voldemort.utils.OpTimeMap;

/**
 * Encapsulates the timeouts, in ms, for various Voldemort operations
 * 
 */
public class TimeoutConfig {

    private OpTimeMap timeoutMap;

    private boolean partialGetAllAllowed;

    public TimeoutConfig(long globalTimeout, boolean allowPartialGetAlls) {
        timeoutMap = new OpTimeMap(globalTimeout);
        setPartialGetAllAllowed(allowPartialGetAlls);
    }

    public TimeoutConfig(long getTimeout,
                         long putTimeout,
                         long deleteTimeout,
                         long getAllTimeout,
                         long getVersionsTimeout,
                         boolean allowPartialGetAlls) {
        timeoutMap = new OpTimeMap(getTimeout,
                                   putTimeout,
                                   deleteTimeout,
                                   getAllTimeout,
                                   getVersionsTimeout);
        setPartialGetAllAllowed(allowPartialGetAlls);
    }

    public long getOperationTimeout(Byte opCode) {
        return timeoutMap.getOpTime(opCode);
    }

    public void setOperationTimeout(Byte opCode, long timeoutMs) {
        timeoutMap.setOpTime(opCode, timeoutMs);
    }

    public boolean isPartialGetAllAllowed() {
        return partialGetAllAllowed;
    }

    public void setPartialGetAllAllowed(boolean allowPartialGetAlls) {
        this.partialGetAllAllowed = allowPartialGetAlls;
    }

}
