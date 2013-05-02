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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

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

    /* A map of versions keyed by nodeId */
    private final Map<Short, Long> versionMap;

    /*
     * The time of the last update on the server on which the update was
     * performed
     */
    private volatile long timestamp;

    /**
     * Construct an empty VectorClock
     */
    public VectorClock() {
        this(System.currentTimeMillis());
    }

    public VectorClock(long timestamp) {
        this.versionMap = new HashMap<Short, Long>(0);
        this.timestamp = timestamp;
    }

    /**
     * This function is not safe because it may break the pre-condition that
     * clock entries should be sorted by nodeId
     * 
     */
    @Deprecated
    public VectorClock(List<ClockEntry> versions, long timestamp) {
        this.versionMap = new HashMap<Short, Long>(0);
        this.timestamp = timestamp;
        for(ClockEntry clockEntry: versions) {
            this.versionMap.put(clockEntry.getNodeId(), clockEntry.getVersion());
        }
    }

    public VectorClock(Map<Short, Long> versionMap, long timestamp) {
        this.versionMap = Utils.notNull(versionMap);
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

        this.versionMap = new HashMap<Short, Long>(numEntries);
        int index = 3 + offset;
        for(int i = 0; i < numEntries; i++) {
            short nodeId = ByteUtils.readShort(bytes, index);
            long version = ByteUtils.readBytes(bytes, index + ByteUtils.SIZE_OF_SHORT, versionSize);
            this.versionMap.put(nodeId, version);
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
        ByteUtils.writeShort(buf, (short) versionMap.size(), offset);
        offset += ByteUtils.SIZE_OF_SHORT;
        // write the size of each version in bytes
        byte versionSize = ByteUtils.numberOfBytesRequired(getMaxVersion());
        buf[offset] = versionSize;
        offset++;

        int clockEntrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
        for(Map.Entry<Short, Long> entry: versionMap.entrySet()) {
            ByteUtils.writeShort(buf, entry.getKey(), offset);
            ByteUtils.writeBytes(buf,
                                 entry.getValue(),
                                 offset + ByteUtils.SIZE_OF_SHORT,
                                 versionSize);
            offset += clockEntrySize;
        }
        ByteUtils.writeLong(buf, this.timestamp, offset);
        return sizeInBytes();
    }

    public int sizeInBytes() {
        byte versionSize = ByteUtils.numberOfBytesRequired(getMaxVersion());
        return ByteUtils.SIZE_OF_SHORT + 1 + this.versionMap.size()
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

        Long version = versionMap.get((short) node);
        if(version == null) {
            version = 1L;
        } else {
            version = version + 1L;
        }

        versionMap.put((short) node, version);
        if(versionMap.size() >= MAX_NUMBER_OF_VERSIONS) {
            throw new IllegalStateException("Vector clock is full!");
        }

    }

    /**
     * List Get new vector clock based on this clock but incremented on index
     * nodeId
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
        return new VectorClock(Maps.newHashMap(versionMap), this.timestamp);
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
        return versionMap.equals(clock.versionMap);
    }

    @Override
    public int hashCode() {
        return versionMap.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("version(");
        int versionsLeft = versionMap.size();
        for(Map.Entry<Short, Long> entry: versionMap.entrySet()) {
            versionsLeft--;
            Short node = entry.getKey();
            Long version = entry.getValue();
            builder.append(node + ":" + version);
            if(versionsLeft > 0) {
                builder.append(", ");
            }
        }
        builder.append(")");
        builder.append(" ts:" + timestamp);
        return builder.toString();
    }

    public long getMaxVersion() {
        long max = -1;
        for(Long version: versionMap.values())
            max = Math.max(version, max);
        return max;
    }

    public VectorClock merge(VectorClock clock) {
        VectorClock newClock = new VectorClock();
        for(Map.Entry<Short, Long> entry: this.versionMap.entrySet()) {
            newClock.versionMap.put(entry.getKey(), entry.getValue());
        }
        for(Map.Entry<Short, Long> entry: clock.versionMap.entrySet()) {
            Long version = newClock.versionMap.get(entry.getKey());
            if(version == null) {
                newClock.versionMap.put(entry.getKey(), entry.getValue());
            } else {
                newClock.versionMap.put(entry.getKey(), Math.max(version, entry.getValue()));
            }
        }

        return newClock;
    }

    @Override
    public Occurred compare(Version v) {
        if(!(v instanceof VectorClock))
            throw new IllegalArgumentException("Cannot compare Versions of different types.");

        return compare(this, (VectorClock) v);
    }

    /**
     * Compare two VectorClocks, the outcomes will be one of the following: --
     * v1 <= v2 : BEFORE (implies v2 AFTER or BEFORE v1)<br>
     * v1 > v2 => AFTER (implies v2 BEFORE v1)<br>
     * v1 <> v2 => CONCURRENTLY (implies v2 CONCURRENTLY to v1)
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

        Map<Short, Long> v1MapCopy = Maps.newHashMap(v1.versionMap);
        for(Map.Entry<Short, Long> v2Entry: v2.versionMap.entrySet()) {
            Long v1Version = v1MapCopy.remove(v2Entry.getKey());
            Long v2Version = v2Entry.getValue();
            if(v1Version == null) {
                v2Bigger = true;
            } else if(v1Version < v2Version) {
                v2Bigger = true;
            } else if(v1Version > v2Version) {
                v1Bigger = true;
            }
            // two clockEntry (of same nodeId) with the same version
            // will not make either version bigger
        }
        // if there are clockEntry not touched in v1MapCopy then v1 has bigger
        // clocks
        if(v1MapCopy.size() > 0) {
            v1Bigger = true;
        }

        /*
         * This is the case where they are equal. Consciously return BEFORE, so
         * that the we would throw back an ObsoleteVersionException for online
         * writes with the same clock.
         */
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

    @Deprecated
    public List<ClockEntry> getEntries() {
        List<ClockEntry> clocks = new ArrayList<ClockEntry>(versionMap.size());
        for(Map.Entry<Short, Long> entry: versionMap.entrySet()) {
            clocks.add(new ClockEntry(entry.getKey(), entry.getValue()));
        }
        return Collections.unmodifiableList(clocks);
    }
}
