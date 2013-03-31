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
import java.util.ArrayList;
import java.util.List;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.utils.ByteUtils;

import com.google.common.collect.Lists;

/**
 * A vector of the number of writes mastered by each node. The vector is stored
 * sparely, since, in general, writes will be mastered by only one node. This
 * means implicitly all the versions are at zero, but we only actually store
 * those greater than zero.
 * 
 * 
 */
@NotThreadsafe
public class VectorClock implements Version, Serializable {

    private static final long serialVersionUID = 1;

    private static final int MAX_NUMBER_OF_VERSIONS = Short.MAX_VALUE;

    /* A sorted list of live versions ordered from least to greatest */
    private final List<ClockEntry> versions;

    /*
     * The time of the last update on the server on which the update was
     * performed
     */
    private volatile long timestamp;

    /**
     * Construct an empty VectorClock
     */
    public VectorClock() {
        this(new ArrayList<ClockEntry>(0), System.currentTimeMillis());
    }

    public VectorClock(long timestamp) {
        this(new ArrayList<ClockEntry>(0), timestamp);
    }

    /**
     * Create a VectorClock with the given version and timestamp
     * 
     * @param versions The version to prepopulate
     * @param timestamp The timestamp to prepopulate
     */
    public VectorClock(List<ClockEntry> versions, long timestamp) {
        this.versions = versions;
        this.timestamp = timestamp;
    }

    /**
     * Takes the bytes of a VectorClock and creates a java object from them. For
     * efficiency reasons the extra bytes can be attached to the end of the byte
     * array that are not related to the VectorClock
     * 
     * @param bytes The serialized bytes of the VectorClock
     */
    public VectorClock(byte[] bytes) {
        this(bytes, 0);
    }

    /**
     * Read the vector clock from the given bytes starting from a particular
     * offset
     * 
     * @param bytes The bytes to read from
     * @param offset The offset to start reading from
     */
    public VectorClock(byte[] bytes, int offset) {
        if(bytes == null || bytes.length <= offset)
            throw new IllegalArgumentException("Invalid byte array for serialization--no bytes to read.");
        int numEntries = ByteUtils.readShort(bytes, offset);
        int versionSize = bytes[offset + 2];
        int entrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
        int minimumBytes = offset + ByteUtils.SIZE_OF_SHORT + 1 + numEntries * entrySize
                           + ByteUtils.SIZE_OF_LONG;
        if(bytes.length < minimumBytes)
            throw new IllegalArgumentException("Too few bytes: expected at least " + minimumBytes
                                               + " but found only " + bytes.length + ".");

        this.versions = new ArrayList<ClockEntry>(numEntries);
        int index = 3 + offset;
        for(int i = 0; i < numEntries; i++) {
            short nodeId = ByteUtils.readShort(bytes, index);
            long version = ByteUtils.readBytes(bytes, index + ByteUtils.SIZE_OF_SHORT, versionSize);
            this.versions.add(new ClockEntry(nodeId, version));
            index += entrySize;
        }
        this.timestamp = ByteUtils.readLong(bytes, index);
    }

    public byte[] toBytes() {
        byte[] serialized = new byte[sizeInBytes()];
        toBytes(serialized, 0);
        return serialized;
    }

    public int toBytes(byte[] buf, int offset) {
        // write the number of versions
        ByteUtils.writeShort(buf, (short) versions.size(), offset);
        offset += ByteUtils.SIZE_OF_SHORT;
        // write the size of each version in bytes
        byte versionSize = ByteUtils.numberOfBytesRequired(getMaxVersion());
        buf[offset] = versionSize;
        offset++;

        int clockEntrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
        for(ClockEntry v: versions) {
            ByteUtils.writeShort(buf, v.getNodeId(), offset);
            ByteUtils.writeBytes(buf, v.getVersion(), offset + ByteUtils.SIZE_OF_SHORT, versionSize);
            offset += clockEntrySize;
        }
        ByteUtils.writeLong(buf, this.timestamp, offset);
        return sizeInBytes();
    }

    public int sizeInBytes() {
        byte versionSize = ByteUtils.numberOfBytesRequired(getMaxVersion());
        return ByteUtils.SIZE_OF_SHORT + 1 + this.versions.size()
               * (ByteUtils.SIZE_OF_SHORT + versionSize) + ByteUtils.SIZE_OF_LONG;
    }

    /**
     * Increment the version info associated with the given node
     * 
     * @param node The node
     */
    public void incrementVersion(int node, long time) {
        if(node < 0 || node > Short.MAX_VALUE)
            throw new IllegalArgumentException(node
                                               + " is outside the acceptable range of node ids.");

        this.timestamp = time;

        // stop on the index greater or equal to the node
        boolean found = false;
        int index = 0;
        for(; index < versions.size(); index++) {
            if(versions.get(index).getNodeId() == node) {
                found = true;
                break;
            } else if(versions.get(index).getNodeId() > node) {
                found = false;
                break;
            }
        }

        if(found) {
            versions.set(index, versions.get(index).incremented());
        } else if(index < versions.size() - 1) {
            versions.add(index, new ClockEntry((short) node, 1));
        } else {
            // we don't already have a version for this, so add it
            if(versions.size() > MAX_NUMBER_OF_VERSIONS)
                throw new IllegalStateException("Vector clock is full!");
            versions.add(index, new ClockEntry((short) node, 1));
        }

    }

    /**
     * Get new vector clock based on this clock but incremented on index nodeId
     * 
     * @param nodeId The id of the node to increment
     * @return A vector clock equal on each element execept that indexed by
     *         nodeId
     */
    public VectorClock incremented(int nodeId, long time) {
        VectorClock copyClock = this.clone();
        copyClock.incrementVersion(nodeId, time);
        return copyClock;
    }

    @Override
    public VectorClock clone() {
        return new VectorClock(Lists.newArrayList(versions), this.timestamp);
    }

    @Override
    public boolean equals(Object object) {
        if(this == object)
            return true;
        if(object == null)
            return false;
        if(!object.getClass().equals(VectorClock.class))
            return false;
        VectorClock clock = (VectorClock) object;
        return versions.equals(clock.versions);
    }

    @Override
    public int hashCode() {
        return versions.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("version(");
        if(this.versions.size() > 0) {
            for(int i = 0; i < this.versions.size() - 1; i++) {
                builder.append(this.versions.get(i));
                builder.append(", ");
            }
            builder.append(this.versions.get(this.versions.size() - 1));
        }
        builder.append(")");
        builder.append(" ts:" + timestamp);
        return builder.toString();
    }

    public long getMaxVersion() {
        long max = -1;
        for(ClockEntry entry: versions)
            max = Math.max(entry.getVersion(), max);
        return max;
    }

    public VectorClock merge(VectorClock clock) {
        VectorClock newClock = new VectorClock();
        int i = 0;
        int j = 0;
        while(i < this.versions.size() && j < clock.versions.size()) {
            ClockEntry v1 = this.versions.get(i);
            ClockEntry v2 = clock.versions.get(j);
            if(v1.getNodeId() == v2.getNodeId()) {
                newClock.versions.add(new ClockEntry(v1.getNodeId(), Math.max(v1.getVersion(),
                                                                              v2.getVersion())));
                i++;
                j++;
            } else if(v1.getNodeId() < v2.getNodeId()) {
                newClock.versions.add(v1.clone());
                i++;
            } else {
                newClock.versions.add(v2.clone());
                j++;
            }
        }

        // Okay now there may be leftovers on one or the other list remaining
        for(int k = i; k < this.versions.size(); k++)
            newClock.versions.add(this.versions.get(k).clone());
        for(int k = j; k < clock.versions.size(); k++)
            newClock.versions.add(clock.versions.get(k).clone());

        return newClock;
    }

    public Occurred compare(Version v) {
        if(!(v instanceof VectorClock))
            throw new IllegalArgumentException("Cannot compare Versions of different types.");

        return compare(this, (VectorClock) v);
    }

    /**
     * Is this Reflexive, AntiSymetic, and Transitive? Compare two VectorClocks,
     * the outcomes will be one of the following: -- Clock 1 is BEFORE clock 2
     * if there exists an i such that c1(i) <= c(2) and there does not exist a j
     * such that c1(j) > c2(j). -- Clock 1 is CONCURRENT to clock 2 if there
     * exists an i, j such that c1(i) < c2(i) and c1(j) > c2(j) -- Clock 1 is
     * AFTER clock 2 otherwise
     * 
     * @param v1 The first VectorClock
     * @param v2 The second VectorClock
     */
    public static Occurred compare(VectorClock v1, VectorClock v2) {
        if(v1 == null || v2 == null)
            throw new IllegalArgumentException("Can't compare null vector clocks!");
        // We do two checks: v1 <= v2 and v2 <= v1 if both are true then
        boolean v1Bigger = false;
        boolean v2Bigger = false;
        int p1 = 0;
        int p2 = 0;

        while(p1 < v1.versions.size() && p2 < v2.versions.size()) {
            ClockEntry ver1 = v1.versions.get(p1);
            ClockEntry ver2 = v2.versions.get(p2);
            if(ver1.getNodeId() == ver2.getNodeId()) {
                if(ver1.getVersion() > ver2.getVersion())
                    v1Bigger = true;
                else if(ver2.getVersion() > ver1.getVersion())
                    v2Bigger = true;
                p1++;
                p2++;
            } else if(ver1.getNodeId() > ver2.getNodeId()) {
                // since ver1 is bigger that means it is missing a version that
                // ver2 has
                v2Bigger = true;
                p2++;
            } else {
                // this means ver2 is bigger which means it is missing a version
                // ver1 has
                v1Bigger = true;
                p1++;
            }
        }

        /* Okay, now check for left overs */
        if(p1 < v1.versions.size())
            v1Bigger = true;
        else if(p2 < v2.versions.size())
            v2Bigger = true;

        /* This is the case where they are equal, return BEFORE arbitrarily */
        if(!v1Bigger && !v2Bigger)
            return Occurred.BEFORE;
        /* This is the case where v1 is a successor clock to v2 */
        else if(v1Bigger && !v2Bigger)
            return Occurred.AFTER;
        /* This is the case where v2 is a successor clock to v1 */
        else if(!v1Bigger && v2Bigger)
            return Occurred.BEFORE;
        /* This is the case where both clocks are parallel to one another */
        else
            return Occurred.CONCURRENTLY;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public List<ClockEntry> getEntries() {
        return this.versions;
    }

}
