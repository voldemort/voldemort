/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

    public OpTimeMap(OpTimeMap source) {
        timeMap = new HashMap<Byte, Long>(source.timeMap);
    }

    public long getOpTime(Byte opCode) {
        assert timeMap.containsKey(opCode);
        return timeMap.get(opCode);
    }

    public void setOpTime(Byte opCode, long time) {
        timeMap.put(opCode, time);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '(' + timeMap + ')';
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(obj == null) {
            return false;
        }
        if(getClass() != obj.getClass()) {
            return false;
        }
        OpTimeMap other = (OpTimeMap) obj;
        return timeMap.equals(other.timeMap);
    }

    @Override
    public int hashCode() {
        return 31 + timeMap.hashCode();
    }
}
