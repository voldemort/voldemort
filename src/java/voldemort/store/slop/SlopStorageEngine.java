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
 * Storage engine specifically designed for storing hints, which will
 * keep a statistic of hints that were attempted, but not succesfully push
 * last time a {@link voldemort.server.scheduler.SlopPusherJob} ran and hints
 * that have been added after the last run
 */
public class SlopStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {
    private final StorageEngine<ByteArray, byte[], byte[]> slopEngine;
    private final AtomicLong addedSinceReset;
    private volatile long outstanding;

    public SlopStorageEngine(StorageEngine<ByteArray, byte[], byte[]> slopEngine) {
        this.slopEngine = slopEngine;
        this.addedSinceReset = new AtomicLong(0);
        this.outstanding = 0;
    }

    @JmxGetter(name="addedSinceReset", description="slops added since reset")
    public Long getAddedSinceReset() {
        return addedSinceReset.get();
    }

    public void resetStats(long outstanding) {
        addedSinceReset.set(0);
        this.outstanding = outstanding;
    }

    @JmxGetter(name="outstanding", description="slops outstanding since last push")
    public long getOutstanding() {
        return outstanding;
    }

    public StorageEngine<ByteArray, Slop, byte[]> asSlopStore() {
        return SerializingStorageEngine.wrap(this,
                                             new ByteArraySerializer(),
                                             new SlopSerializer(),
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
        slopEngine.put(key, value, transforms);
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
