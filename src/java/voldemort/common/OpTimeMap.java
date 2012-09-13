package voldemort.common;

import java.util.HashMap;


/**
 * Encapsulates time to Voldemort operation mapping
 * 
 */
public class OpTimeMap {

    private HashMap<Byte, Long> timeMap;

    public OpTimeMap(long time) {
        this(time, time, time, time, time);
    }

    public OpTimeMap(long getTime,
                     long putTime,
                     long deleteTime,
                     long getAllTime,
                     long getVersionsTime) {
        timeMap = new HashMap<Byte, Long>();
        timeMap.put(VoldemortOpCode.GET_OP_CODE, getTime);
        timeMap.put(VoldemortOpCode.PUT_OP_CODE, putTime);
        timeMap.put(VoldemortOpCode.DELETE_OP_CODE, deleteTime);
        timeMap.put(VoldemortOpCode.GET_ALL_OP_CODE, getAllTime);
        timeMap.put(VoldemortOpCode.GET_VERSION_OP_CODE, getVersionsTime);
    }

    public long getOpTime(Byte opCode) {
        assert timeMap.containsKey(opCode);
        return timeMap.get(opCode);
    }

    public void setOpTime(Byte opCode, long time) {
        timeMap.put(opCode, time);
    }
}
