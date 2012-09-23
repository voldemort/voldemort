package voldemort.client;

import java.util.HashMap;

import voldemort.common.VoldemortOpCode;

/**
 * Encapsulates the timeouts for various voldemort operations
 * 
 */
public class TimeoutConfig {

    private HashMap<Byte, Long> timeoutMap;

    private boolean partialGetAllsAllowed;

    private boolean partialHasKeysAllowed;

    public TimeoutConfig(long globalTimeout,
                         boolean partialGetAllsAllowed,
                         boolean partialHasKeysAllowed) {
        this(globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             partialGetAllsAllowed,
             partialHasKeysAllowed);
    }

    public TimeoutConfig(long getTimeout,
                         long putTimeout,
                         long deleteTimeout,
                         long getAllTimeout,
                         long getVersionsTimeout,
                         long hasKeysTimeout,
                         boolean partialGetAllsAllowed,
                         boolean partialHasKeysAllowed) {
        timeoutMap = new HashMap<Byte, Long>();
        timeoutMap.put(VoldemortOpCode.GET_OP_CODE, getTimeout);
        timeoutMap.put(VoldemortOpCode.PUT_OP_CODE, putTimeout);
        timeoutMap.put(VoldemortOpCode.DELETE_OP_CODE, deleteTimeout);
        timeoutMap.put(VoldemortOpCode.GET_ALL_OP_CODE, getAllTimeout);
        timeoutMap.put(VoldemortOpCode.GET_VERSION_OP_CODE, getVersionsTimeout);
        timeoutMap.put(VoldemortOpCode.HAS_KEYS_OP_CODE, hasKeysTimeout);
        setPartialGetAllsAllowed(partialGetAllsAllowed);
        setPartialHasKeysAllowed(partialHasKeysAllowed);
    }

    public long getOperationTimeout(Byte opCode) {
        assert timeoutMap.containsKey(opCode);
        return timeoutMap.get(opCode);
    }

    public void setOperationTimeout(Byte opCode, long timeoutMs) {
        timeoutMap.put(opCode, timeoutMs);
    }

    public boolean isPartialGetAllsAllowed() {
        return partialGetAllsAllowed;
    }

    public void setPartialGetAllsAllowed(boolean partialGetAllsAllowed) {
        this.partialGetAllsAllowed = partialGetAllsAllowed;
    }

    public boolean isPartialHasKeysAllowed() {
        return partialHasKeysAllowed;
    }

    public void setPartialHasKeysAllowed(boolean partialHasKeysAllowed) {
        this.partialHasKeysAllowed = partialHasKeysAllowed;
    }

}
