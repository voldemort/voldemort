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

package voldemort.cluster;

import java.io.Serializable;
import java.util.Date;

import voldemort.utils.SystemTime;
import voldemort.utils.Time;

import com.google.common.base.Objects;

/**
 * Holds the status of a node--either available or unavailable as well as the
 * last date the status was checked.
 * 
 * Operations on this class are not atomic, but that is okay.
 * 
 * @author jay
 * 
 */
public class NodeStatus implements Serializable {

    private static final long serialVersionUID = 1;

    private final Time time;
    private volatile long lastChecked;
    private volatile boolean isAvailable;

    public NodeStatus() {
        this(SystemTime.INSTANCE, System.currentTimeMillis(), true);
    }

    public NodeStatus(Time time) {
        this(time, time.getMilliseconds(), true);
    }

    public NodeStatus(Time time, long lastChecked, boolean isAvailable) {
        this.time = Objects.nonNull(time);
        this.lastChecked = lastChecked;
        this.isAvailable = isAvailable;
    }

    public long getLastCheckedMs() {
        return lastChecked;
    }

    public long getMsSinceLastCheck() {
        return time.getMilliseconds() - lastChecked;
    }

    public boolean isUnavailable(long msExpiration) {
        boolean isExpired = time.getMilliseconds() > lastChecked + msExpiration;
        return !isAvailable && !isExpired;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    public boolean isUnavailable() {
        return !isAvailable;
    }

    public void setUnavailable() {
        this.isAvailable = false;
        this.lastChecked = time.getMilliseconds();
    }

    public void setAvailable() {
        this.isAvailable = true;
        this.lastChecked = time.getMilliseconds();
    }

    @Override
    public String toString() {
        return "Status(" + (isAvailable ? "available" : "down") + ", " + new Date(lastChecked);
    }

}
