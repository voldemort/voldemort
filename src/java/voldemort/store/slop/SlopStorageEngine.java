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

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.cluster.Cluster;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks statistics of hints that were attempted, but not successfully
 * pushed last time a {@link voldemort.server.scheduler.SlopPusherJob} ran;
 * also tracks hints that have been added after the last run
 *
 */
public class SlopStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private final static Logger logger = Logger.getLogger(SlopStorageEngine.class);
    
    private final StorageEngine<ByteArray, byte[], byte[]> slopEngine;
    private final AtomicLong addedSinceResetTotal;

    // TODO: add a stats/counter object to simplify the logic
    private volatile long outstandingTotal;
    private final ConcurrentMap<Integer, Long> outstandingByNode;
    private final ConcurrentMap<Integer, AtomicLong> addedSinceResetByNode;

    private final Cluster cluster;
    private final SlopSerializer slopSerializer;

    public SlopStorageEngine(StorageEngine<ByteArray, byte[], byte[]> slopEngine,
                             Cluster cluster) {
        this.slopEngine = slopEngine;
        this.addedSinceResetTotal = new AtomicLong(0);
        this.outstandingTotal = 0;
        this.cluster = cluster;
        int numNodes = cluster.getNumberOfNodes();
        outstandingByNode = new ConcurrentHashMap<Integer, Long>(numNodes);
        addedSinceResetByNode = new ConcurrentHashMap<Integer, AtomicLong>(numNodes);
        for(int i=0; i < numNodes; i++) {
            outstandingByNode.put(i, 0L);
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

    public void resetStats(long newTotal, Map<Integer, Long> newByNode) {
        addedSinceResetTotal.set(0);
        outstandingTotal = newTotal;
        for(int i = 0; i < cluster.getNumberOfNodes(); i++)
            addedSinceResetByNode.get(i).set(0);
        outstandingByNode.putAll(newByNode);
    }

    @JmxGetter(name="outstandingTotal", description="slops outstanding since last push")
    public long getOutstandingTotal() {
        return outstandingTotal;
    }

    @JmxGetter(name="outstandingByNode", description="slops outstanding by node since last push")
    public Map<Integer, Long> getOutstandingByNode() {
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
        AtomicLong addedSinceReset = addedSinceResetByNode.get(slop.getNodeId());
        if(addedSinceReset == null) {
            addedSinceReset = new AtomicLong(0);
            addedSinceResetByNode.putIfAbsent(slop.getNodeId(), addedSinceReset);
        }
        addedSinceReset.incrementAndGet();
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
