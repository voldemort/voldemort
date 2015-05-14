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

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.cluster.Cluster;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.SlopSerializer;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.store.stats.SlopStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Tracks statistics of hints that were attempted, but not successfully pushed
 * last time a pusher job ran; also tracks hints that have been added after the
 * last run
 * 
 */
public class SlopStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    public static final String SLOP_STORE_NAME = "slop";

    private final StorageEngine<ByteArray, byte[], byte[]> slopEngine;
    private final SlopSerializer slopSerializer;
    private final SlopStats slopStats;

    public SlopStorageEngine(StorageEngine<ByteArray, byte[], byte[]> slopEngine, Cluster cluster) {
        super(slopEngine.getName());
        this.slopEngine = slopEngine;
        this.slopSerializer = new SlopSerializer();
        this.slopStats = new SlopStats(cluster);
    }

    @JmxGetter(name = "outstandingTotal", description = "slops outstanding since last push")
    public long getOutstandingTotal() {
        return slopStats.getTotalCount(SlopStats.Tracked.OUTSTANDING);
    }

    @JmxGetter(name = "outstandingByNode", description = "slops outstanding by node since last push")
    public Map<Integer, Long> getOutstandingByNode() {
        return slopStats.asMap(SlopStats.Tracked.OUTSTANDING);
    }

    @JmxGetter(name = "outstandingByZone", description = "slops outstanding by zone since last push")
    public Map<Integer, Long> getOutstandingByZone() {
        return slopStats.byZone(SlopStats.Tracked.OUTSTANDING);
    }

    public void resetStats(Map<Integer, Long> newValues) {
        slopStats.setAll(SlopStats.Tracked.OUTSTANDING, newValues);
    }

    public StorageEngine<ByteArray, Slop, byte[]> asSlopStore() {
        return SerializingStorageEngine.wrap(this,
                                             new ByteArraySerializer(),
                                             slopSerializer,
                                             new IdentitySerializer());
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return slopEngine.entries();
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return slopEngine.keys();
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        return slopEngine.entries(partition);
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        return slopEngine.keys(partition);
    }

    @Override
    public void truncate() {
        slopEngine.truncate();
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        return slopEngine.get(key, transforms);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        return slopEngine.getAll(keys, transforms);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        slopEngine.put(key, value, transforms);
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return slopEngine.delete(key, version);
    }

    @Override
    public void close() throws VoldemortException {
        slopEngine.close();
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        return slopEngine.getCapability(capability);
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return slopEngine.getVersions(key);
    }

    @Override
    public boolean isPartitionAware() {
        return slopEngine.isPartitionAware();
    }

    @Override
    public boolean isPartitionScanSupported() {
        return slopEngine.isPartitionScanSupported();
    }

    @Override
    public boolean beginBatchModifications() {
        return slopEngine.beginBatchModifications();
    }

    @Override
    public boolean endBatchModifications() {
        return slopEngine.endBatchModifications();
    }
}
