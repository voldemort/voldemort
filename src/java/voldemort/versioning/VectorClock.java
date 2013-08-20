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
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

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
    private final TreeMap<Short, Long> versionMap;

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

    public TreeMap<Short, Long> getVersionMap() {
        return versionMap;
    }

    public VectorClock(long timestamp) {
        this.versionMap = new TreeMap<Short, Long>();
        this.timestamp = timestamp;
    }

    /**
     * This function is not safe because it may break the pre-condition that
     * clock entries should be sorted by nodeId
     * 
     */
    @Deprecated
    public VectorClock(List<ClockEntry> versions, long timestamp) {
        this.versionMap = new TreeMap<Short, Long>();
        this.timestamp = timestamp;
        for(ClockEntry clockEntry: versions) {
            this.versionMap.put(clockEntry.getNodeId(), clockEntry.getVersion());
        }
    }

    /**
     * Only used for cloning
     * 
     * @param versionMap
     * @param timestamp
     */
    private VectorClock(TreeMap<Short, Long> versionMap, long timestamp) {
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

        this.versionMap = new TreeMap<Short, Long>();
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
        SortedSet<Short> nodeIds = versionMap.navigableKeySet();
        for(Short nodeId: nodeIds) {
            Long version = versionMap.get(nodeId);
            ByteUtils.writeShort(buf, nodeId, offset);
            ByteUtils.writeBytes(buf, version, offset + ByteUtils.SIZE_OF_SHORT, versionSize);
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
        return new VectorClock(Maps.newTreeMap(versionMap), this.timestamp);
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

        return VectorClockUtils.compare(this, (VectorClock) v);
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

    /**
     * Function to copy values from another VectorClock. This is used for
     * in-place updates during a Voldemort put operation.
     * 
     * @param vc The VectorClock object from which the inner values are to be
     *        copied.
     */
    public void copyFromVectorClock(VectorClock vc) {
        this.versionMap.clear();
        this.timestamp = vc.getTimestamp();
        for(ClockEntry clockEntry: vc.getEntries()) {
            this.versionMap.put(clockEntry.getNodeId(), clockEntry.getVersion());
        }
    }
}
