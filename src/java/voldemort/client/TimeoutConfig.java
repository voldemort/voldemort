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

package voldemort.client;

import voldemort.common.OpTimeMap;

/**
 * Encapsulates the timeouts, in ms, for various Voldemort operations
 * 
 */
public class TimeoutConfig {

    public static int DEFAULT_GLOBAL_TIMEOUT_MS = 5000;
    public static boolean DEFAULT_ALLOW_PARTIAL_GETALLS = false;
    private OpTimeMap timeoutMap;

    private boolean partialGetAllAllowed;

    public TimeoutConfig() {
        this(DEFAULT_GLOBAL_TIMEOUT_MS, DEFAULT_ALLOW_PARTIAL_GETALLS);
    }

    public TimeoutConfig(long globalTimeoutMs) {
        this(globalTimeoutMs, DEFAULT_ALLOW_PARTIAL_GETALLS);
    }

    public TimeoutConfig(long globalTimeoutMs, boolean allowPartialGetAlls) {
        timeoutMap = new OpTimeMap(globalTimeoutMs);
        setPartialGetAllAllowed(allowPartialGetAlls);
    }

    public TimeoutConfig(TimeoutConfig source) {
        timeoutMap = new OpTimeMap(source.timeoutMap);
        setPartialGetAllAllowed(source.isPartialGetAllAllowed());
    }

    public TimeoutConfig(long getTimeoutMs,
                         long putTimeoutMs,
                         long deleteTimeoutMs,
                         long getAllTimeoutMs,
                         long getVersionsTimeoutMs,
                         boolean allowPartialGetAlls) {
        timeoutMap = new OpTimeMap(getTimeoutMs,
                                   putTimeoutMs,
                                   deleteTimeoutMs,
                                   getAllTimeoutMs,
                                   getVersionsTimeoutMs);
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [timeoutMap=" + timeoutMap
               + ", partialGetAllAllowed=" + partialGetAllAllowed + "]";
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
        TimeoutConfig other = (TimeoutConfig) obj;
        if(partialGetAllAllowed != other.partialGetAllAllowed) {
            return false;
        }
        if(!timeoutMap.equals(other.timeoutMap)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (partialGetAllAllowed ? 1231 : 1237);
        result = prime * result + timeoutMap.hashCode();
        return result;
    }
}
