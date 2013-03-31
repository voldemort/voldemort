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

package voldemort.versioning;

import java.io.Serializable;

import voldemort.annotations.concurrency.NotThreadsafe;

/**
 * An entry element for a vector clock versioning scheme This assigns the
 * version from a specific machine, the VectorClock keeps track of the complete
 * system version, which will consist of many individual Version objects.
 * 
 * 
 */
@NotThreadsafe
public final class ClockEntry implements Cloneable, Serializable {

    private static final long serialVersionUID = 1;

    private short nodeId;
    private long version;

    /**
     * Default constructor
     */
    public ClockEntry() {
        this.nodeId = -1;
        this.version = -1;
    }

    /**
     * Create a new Version from constituate parts
     * 
     * @param nodeId The node id
     * @param version The current version
     */
    public ClockEntry(short nodeId, long version) {
        if(nodeId < 0)
            throw new IllegalArgumentException("Node id " + nodeId + " is not in the range (0, "
                                               + Short.MAX_VALUE + ").");
        if(version < 1)
            throw new IllegalArgumentException("Version " + version + " is not in the range (1, "
                                               + Short.MAX_VALUE + ").");
        this.nodeId = nodeId;
        this.version = version;
    }

    @Override
    public ClockEntry clone() {
        try {
            return (ClockEntry) super.clone();
        } catch(CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public short getNodeId() {
        return nodeId;
    }

    public long getVersion() {
        return version;
    }

    public ClockEntry incremented() {
        return new ClockEntry(nodeId, version + 1);
    }

    @Override
    public int hashCode() {
        return nodeId + (((int) version) << 16);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;

        if(o == null)
            return false;

        if(o.getClass().equals(ClockEntry.class)) {
            ClockEntry v = (ClockEntry) o;
            return v.getNodeId() == getNodeId() && v.getVersion() == getVersion();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return nodeId + ":" + version;
    }

    public void setNodeId(short nodeId) {
        this.nodeId = nodeId;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}
