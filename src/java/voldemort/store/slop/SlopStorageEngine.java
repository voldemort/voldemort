/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.store.slop;

import com.google.common.collect.Maps;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.SlopSerializer;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage engine which tracks statistics of hints that were attempted, but not
 * successfully pushed last time a {@link SlopPusherJob} ran and hints
 * that have been added after the last run
 */
public class SlopStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {
    
    private final StorageEngine<ByteArray, byte[], byte[]> slopEngine;
    private final AtomicLong addedSinceResetTotal;
    private volatile long outstandingTotal;
    private final Map<Integer, AtomicLong> outstandingByNode;
    private final Map<Integer, AtomicLong> addedSinceResetByNode;
    private final int numNodes;
    private final SlopSerializer slopSerializer;

    public SlopStorageEngine(StorageEngine<ByteArray, byte[], byte[]> slopEngine,
                             int numNodes) {
        this.slopEngine = slopEngine;
        this.addedSinceResetTotal = new AtomicLong(0);
        this.outstandingTotal = 0;
        this.numNodes = numNodes;
        outstandingByNode = Maps.newHashMapWithExpectedSize(numNodes);
        addedSinceResetByNode = Maps.newHashMapWithExpectedSize(numNodes);

        for(int i=0; i < numNodes; i++) {
            outstandingByNode.put(i, new AtomicLong(0));
            addedSinceResetByNode.put(i, new AtomicLong(0));
        }
        slopSerializer = new SlopSerializer();
    }

    @JmxGetter(name="addedSinceResetTotal", description="slops added since reset")
    public Long getAddedSinceResetTotal() {
        return addedSinceResetTotal.get();
    }

    @JmxGetter(name="addedSinceResetByNode", description="slops added since reset by node")
    public Map<Integer, AtomicLong> getAddedSinceResetByNode() {
        return addedSinceResetByNode;
    }

    public void resetStats(long outstanding, Map<Integer, Long> outstandingByNode) {
        addedSinceResetTotal.set(0);
        this.outstandingTotal = outstanding;

        for(int i = 0; i < numNodes; i++)
            addedSinceResetByNode.get(i).set(0);

        for(Map.Entry<Integer, Long> entry: outstandingByNode.entrySet())
            this.outstandingByNode.get(entry.getKey()).set(entry.getValue());

    }

    @JmxGetter(name="outstandingTotal", description="slops outstanding since last push")
    public long getOutstandingTotal() {
        return outstandingTotal;
    }

    @JmxGetter(name="outstandingByNode", description="slops outstanding by node since last push")
    public Map<Integer, AtomicLong> getOutstandingByNode() {
        return outstandingByNode;
    }

    public StorageEngine<ByteArray, Slop, byte[]> asSlopStore() {
        return SerializingStorageEngine.wrap(this,
                                             new ByteArraySerializer(),
                                             slopSerializer,
                                             new IdentitySerializer());
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return slopEngine.entries();
    }

    public ClosableIterator<ByteArray> keys() {
        return slopEngine.keys();
    }

    public void truncate() {
        slopEngine.truncate();
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        return slopEngine.get(key, transforms);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys, Map<ByteArray, byte[]> transforms) throws VoldemortException {
        return slopEngine.getAll(keys, transforms);
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms) throws VoldemortException {
        Slop slop = slopSerializer.toObject(value.getValue());
        slopEngine.put(key, value, transforms);
        addedSinceResetTotal.incrementAndGet();
        addedSinceResetByNode.get(slop.getNodeId()).incrementAndGet();
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return slopEngine.delete(key, version);
    }

    public String getName() {
        return slopEngine.getName();
    }

    public void close() throws VoldemortException {
        slopEngine.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        return slopEngine.getCapability(capability);
    }

    public List<Version> getVersions(ByteArray key) {
        return slopEngine.getVersions(key);
    }
}
