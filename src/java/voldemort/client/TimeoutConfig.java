package voldemort.client;

import java.util.HashMap;

import voldemort.common.VoldemortOpCode;

/**
 * Encapsulates the timeouts for various voldemort operations
 * 
 */
public class TimeoutConfig {

    private HashMap<Byte, Long> timeoutMap;

    private boolean partialGetAllAllowed;

    public TimeoutConfig(long globalTimeout, boolean allowPartialGetAlls) {
        this(globalTimeout,
             globalTimeout,
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
                         long hasKeysTimeout,
                         boolean allowPartialGetAlls) {
        timeoutMap = new HashMap<Byte, Long>();
        timeoutMap.put(VoldemortOpCode.GET_OP_CODE, getTimeout);
        timeoutMap.put(VoldemortOpCode.PUT_OP_CODE, putTimeout);
        timeoutMap.put(VoldemortOpCode.DELETE_OP_CODE, deleteTimeout);
        timeoutMap.put(VoldemortOpCode.GET_ALL_OP_CODE, getAllTimeout);
        timeoutMap.put(VoldemortOpCode.GET_VERSION_OP_CODE, getVersionsTimeout);
        timeoutMap.put(VoldemortOpCode.HAS_KEYS_OP_CODE, hasKeysTimeout);
        setPartialGetAllAllowed(allowPartialGetAlls);
    }

    public long getOperationTimeout(Byte opCode) {
        assert timeoutMap.containsKey(opCode);
        return timeoutMap.get(opCode);
    }

    public void setOperationTimeout(Byte opCode, long timeoutMs) {
        timeoutMap.put(opCode, timeoutMs);
    }

    public boolean isPartialGetAllAllowed() {
        return partialGetAllAllowed;
    }

    public void setPartialGetAllAllowed(boolean allowPartialGetAlls) {
        this.partialGetAllAllowed = allowPartialGetAlls;
    }

}
