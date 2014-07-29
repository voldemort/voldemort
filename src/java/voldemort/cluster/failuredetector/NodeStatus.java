/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.cluster.failuredetector;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the status of a node--either available or unavailable as well as the
 * last date the status was checked.
 * 
 * Operations on this class are not atomic, but that is okay.
 * 
 * 
 */
class NodeStatus {

    private long lastChecked;

    private boolean isAvailable;

    private long startMillis;

    private long success;

    private long failure;

    private long total;

    private final AtomicInteger numConsecutiveCatastrophicErrors;

    public NodeStatus() {
        numConsecutiveCatastrophicErrors = new AtomicInteger(0);
    }

    public long getLastChecked() {
        return lastChecked;
    }

    public void setLastChecked(long lastChecked) {
        this.lastChecked = lastChecked;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    public void setAvailable(boolean isAvailable) {
        this.isAvailable = isAvailable;
    }

    public long getStartMillis() {
        return startMillis;
    }

    public void setStartMillis(long startMillis) {
        this.startMillis = startMillis;
    }

    public long getSuccess() {
        return success;
    }

    public long getFailure() {
        return failure;
    }

    public long getTotal() {
        return total;
    }

    public int getNumConsecutiveCatastrophicErrors() {
        return numConsecutiveCatastrophicErrors.get();
    }

    public void resetNumConsecutiveCatastrophicErrors() {
        this.numConsecutiveCatastrophicErrors.set(0);
    }

    public int incrementConsecutiveCatastrophicErrors() {
        return this.numConsecutiveCatastrophicErrors.incrementAndGet();
    }

    public void resetCounters(long currentTime) {
        setStartMillis(currentTime);
        // Not resetting the catastrophic errors, as they will be reset
        // whenever a success is received.
        success = 0;
        failure = 0;
        total = 0;
    }

    public void recordOperation(boolean isSuccess) {
        if(isSuccess) {
            success++;
        } else {
            failure++;
        }
        total++;
    }

}
